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
    public static final BigInteger MAX_DECIMAL_UNSCALED_VALUE = new BigInteger("99999999999999999999999999999999999999");
    public static final BigInteger MIN_DECIMAL_UNSCALED_VALUE = new BigInteger("-99999999999999999999999999999999999999");

    private DecimalOperators()
    {
    }

    public static class DecimalAddOperator
            extends ParametricOperator
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
            super(ADD, ImmutableList.of(
                    comparableWithVariadicBound("A", DECIMAL),
                    comparableWithVariadicBound("B", DECIMAL)), DECIMAL, ImmutableList.of("A", "B"));
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

            boolean rescaleParamsAreLongs = baseMethodHandle == SHORT_SHORT_SHORT_ADD_METHOD_HANDLE;
            MethodHandle methodHandle = getMethodHandle(aRescale, bRescale, baseMethodHandle, rescaleParamsAreLongs);

            return operatorInfo(ADD, resultType.getTypeSignature(), ImmutableList.of(aType.getTypeSignature(), bType.getTypeSignature()), methodHandle, false, ImmutableList.of(false, false));
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

        private static void checkOverflow(BigInteger value)
        {
            if (value.compareTo(MAX_DECIMAL_UNSCALED_VALUE) > 0 || value.compareTo(MIN_DECIMAL_UNSCALED_VALUE) < 0) {
                // todo determine correct ErrorCode.
                throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "DECIMAL result exceeds 38 digits");
            }
        }
    }
}
