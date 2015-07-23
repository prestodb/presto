/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.type;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.math.BigInteger;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.OperatorType.SUBTRACT;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.math.BigInteger.TEN;

public final class DecimalOperators
{
    public static final DecimalAddOperator ADD_OPERATOR = new DecimalAddOperator();
    public static final DecimalSubtractOperator SUBTRACT_OPERATOR = new DecimalSubtractOperator();

    private static final BigInteger MAX_DECIMAL_UNSCALED_VALUE = new BigInteger("99999999999999999999999999999999999999");
    private static final BigInteger MIN_DECIMAL_UNSCALED_VALUE = MAX_DECIMAL_UNSCALED_VALUE.negate();

    private DecimalOperators()
    {
    }

    private abstract static class DecimalBinaryOperator
            extends ParametricOperator
    {
        private OperatorType operatorType;

        protected DecimalBinaryOperator(OperatorType operatorType)
        {
            super(operatorType, ImmutableList.of(
                    comparableWithVariadicBound("A", DECIMAL),
                    comparableWithVariadicBound("B", DECIMAL)), DECIMAL, ImmutableList.of("A", "B"));
            this.operatorType = operatorType;
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(arity == 2, "Expected arity to be 2");
            DecimalType aType = (DecimalType) types.get("A");
            DecimalType bType = (DecimalType) types.get("B");

            int aScale = aType.getScale();
            int bScale = bType.getScale();
            int aPrecision = aType.getPrecision();
            int bPrecision = bType.getPrecision();
            int resultPrecision = min(
                    DecimalType.MAX_PRECISION,
                    1 + max(aScale, bScale) + max(aPrecision - aScale, bPrecision - bScale));
            int resultScale = max(aScale, bScale);

            BigInteger aRescale = TEN.pow(resultScale - aScale);
            BigInteger bRescale = TEN.pow(resultScale - bScale);

            DecimalType resultType = DecimalType.createDecimalType(resultPrecision, resultScale);

            MethodHandle baseMethodHandle = getBaseMethodHandle(aType, bType, resultType);
            MethodHandle methodHandle = getMethodHandle(aRescale, bRescale, baseMethodHandle, rescaleParamsAreLongs(baseMethodHandle));

            return operatorInfo(operatorType, resultType.getTypeSignature(), ImmutableList.of(aType.getTypeSignature(), bType.getTypeSignature()), methodHandle, false, ImmutableList.of(false, false));
        }

        private boolean rescaleParamsAreLongs(MethodHandle baseMethodHandle)
        {
            return baseMethodHandle.type().parameterType(baseMethodHandle.type().parameterCount() - 1).isAssignableFrom(long.class);
        }

        private MethodHandle getMethodHandle(BigInteger aRescale, BigInteger bRescale, MethodHandle baseMethodHandle, boolean rescaleParamsAreLongs)
        {
            MethodHandle methodHandle;
            if (rescaleParamsAreLongs) {
                methodHandle = MethodHandles.insertArguments(baseMethodHandle, 2, aRescale.longValue(), bRescale.longValue());
            }
            else {
                methodHandle = MethodHandles.insertArguments(baseMethodHandle, 2, aRescale, bRescale);
            }
            return methodHandle;
        }

        protected abstract MethodHandle getBaseMethodHandle(DecimalType aType, DecimalType bType, DecimalType resultType);
    }

    public static class DecimalAddOperator
            extends DecimalBinaryOperator
    {
        private static final MethodHandle SHORT_SHORT_SHORT_ADD_METHOD_HANDLE =
                methodHandle(DecimalAddOperator.class, "addShortShortShort", long.class, long.class, long.class, long.class);
        private static final MethodHandle SHORT_SHORT_LONG_ADD_METHOD_HANDLE =
                methodHandle(DecimalAddOperator.class, "addShortShortLong", long.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_LONG_LONG_ADD_METHOD_HANDLE =
                methodHandle(DecimalAddOperator.class, "addLongLongLong", Slice.class, Slice.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_SHORT_LONG_ADD_METHOD_HANDLE =
                methodHandle(DecimalAddOperator.class, "addLongShortLong", Slice.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle SHORT_LONG_LONG_ADD_METHOD_HANDLE =
                methodHandle(DecimalAddOperator.class, "addShortLongLong", long.class, Slice.class, BigInteger.class, BigInteger.class);

        protected DecimalAddOperator()
        {
            super(ADD);
        }

        @Override
        protected MethodHandle getBaseMethodHandle(DecimalType aType, DecimalType bType, DecimalType resultType)
        {
            MethodHandle baseMethodHandle;
            if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof ShortDecimalType) {
                baseMethodHandle = SHORT_SHORT_SHORT_ADD_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof LongDecimalType) {
                baseMethodHandle = SHORT_SHORT_LONG_ADD_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof LongDecimalType) {
                baseMethodHandle = SHORT_LONG_LONG_ADD_METHOD_HANDLE;
            }
            else if (aType instanceof LongDecimalType && bType instanceof ShortDecimalType) {
                baseMethodHandle = LONG_SHORT_LONG_ADD_METHOD_HANDLE;
            }
            else {
                baseMethodHandle = LONG_LONG_LONG_ADD_METHOD_HANDLE;
            }
            return baseMethodHandle;
        }

        public static long addShortShortShort(long a, long b, long aRescale, long bRescale)
        {
            return a * aRescale + b * bRescale;
        }

        public static Slice addShortShortLong(long a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalAddLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice addLongLongLong(Slice a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalAddLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice addShortLongLong(long a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalAddLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice addLongShortLong(Slice a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalAddLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        private static Slice internalAddLongLongLong(BigInteger aBigInteger, BigInteger bBigInteger, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aRescaled = aBigInteger.multiply(aRescale);
            BigInteger bRescaled = bBigInteger.multiply(bRescale);
            BigInteger result = aRescaled.add(bRescaled);
            checkOverflow(result);
            return LongDecimalType.unscaledValueToSlice(result);
        }
    }

    public static class DecimalSubtractOperator
            extends DecimalBinaryOperator
    {
        private static final MethodHandle SHORT_SHORT_SHORT_SUBTRACT_METHOD_HANDLE =
                methodHandle(DecimalSubtractOperator.class, "subtractShortShortShort", long.class, long.class, long.class, long.class);
        private static final MethodHandle SHORT_SHORT_LONG_SUBTRACT_METHOD_HANDLE =
                methodHandle(DecimalSubtractOperator.class, "subtractShortShortLong", long.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_LONG_LONG_SUBTRACT_METHOD_HANDLE =
                methodHandle(DecimalSubtractOperator.class, "subtractLongLongLong", Slice.class, Slice.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle LONG_SHORT_LONG_SUBTRACT_METHOD_HANDLE =
                methodHandle(DecimalSubtractOperator.class, "subtractLongShortLong", Slice.class, long.class, BigInteger.class, BigInteger.class);
        private static final MethodHandle SHORT_LONG_LONG_SUBTRACT_METHOD_HANDLE =
                methodHandle(DecimalSubtractOperator.class, "subtractShortLongLong", long.class, Slice.class, BigInteger.class, BigInteger.class);

        protected DecimalSubtractOperator()
        {
            super(SUBTRACT);
        }

        @Override
        protected MethodHandle getBaseMethodHandle(DecimalType aType, DecimalType bType, DecimalType resultType)
        {
            if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof ShortDecimalType) {
                return SHORT_SHORT_SHORT_SUBTRACT_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof ShortDecimalType && resultType instanceof LongDecimalType) {
                return SHORT_SHORT_LONG_SUBTRACT_METHOD_HANDLE;
            }
            else if (aType instanceof ShortDecimalType && bType instanceof LongDecimalType) {
                return SHORT_LONG_LONG_SUBTRACT_METHOD_HANDLE;
            }
            else if (aType instanceof LongDecimalType && bType instanceof ShortDecimalType) {
                return LONG_SHORT_LONG_SUBTRACT_METHOD_HANDLE;
            }
            else {
                return LONG_LONG_LONG_SUBTRACT_METHOD_HANDLE;
            }
        }

        public static long subtractShortShortShort(long a, long b, long aRescale, long bRescale)
        {
            return a * aRescale - b * bRescale;
        }

        public static Slice subtractShortShortLong(long a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalSubtractLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice subtractLongLongLong(Slice a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalSubtractLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice subtractShortLongLong(long a, Slice b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = BigInteger.valueOf(a);
            BigInteger bBigInteger = LongDecimalType.unscaledValueToBigInteger(b);
            return internalSubtractLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        public static Slice subtractLongShortLong(Slice a, long b, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aBigInteger = LongDecimalType.unscaledValueToBigInteger(a);
            BigInteger bBigInteger = BigInteger.valueOf(b);
            return internalSubtractLongLongLong(aBigInteger, bBigInteger, aRescale, bRescale);
        }

        private static Slice internalSubtractLongLongLong(BigInteger aBigInteger, BigInteger bBigInteger, BigInteger aRescale, BigInteger bRescale)
        {
            BigInteger aRescaled = aBigInteger.multiply(aRescale);
            BigInteger bRescaled = bBigInteger.multiply(bRescale);
            BigInteger result = aRescaled.subtract(bRescaled);
            checkOverflow(result);
            return LongDecimalType.unscaledValueToSlice(result);
        }
    }

    private static void checkOverflow(BigInteger value)
    {
        if (value.compareTo(MAX_DECIMAL_UNSCALED_VALUE) > 0 || value.compareTo(MIN_DECIMAL_UNSCALED_VALUE) < 0) {
            // todo determine correct ErrorCode.
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "DECIMAL result exceeds 38 digits");
        }
    }
}
