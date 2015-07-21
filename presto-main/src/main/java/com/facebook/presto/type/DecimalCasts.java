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
import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DecimalCasts
{
    public static final DecimalToBigintCast DECIMAL_TO_BIGINT_CAST = new DecimalToBigintCast();
    public static final BigintToDecimalCast BIGINT_TO_DECIMAL_CAST = new BigintToDecimalCast();
    public static final DecimalToVarcharCast DECIMAL_TO_VARCHAR_CAST = new DecimalToVarcharCast();
    public static final DecimalToDoubleCast DECIMAL_TO_DOUBLE_CASTS = new DecimalToDoubleCast();

    private static final long[] TEN_TO_NTH = new long[DecimalType.MAX_SHORT_PRECISION + 1];

    static {
        for (int i = 0; i < TEN_TO_NTH.length; ++i) {
            TEN_TO_NTH[i] = round(pow(10, i));
        }
    }

    private DecimalCasts() {}

    @ScalarOperator(CAST)
    @SqlType(BOOLEAN)
    public static boolean castDecimalToBoolean(@SqlType(DECIMAL) long decimal)
    {
        return decimal != 0;
    }

    public static class DecimalToBigintCast
            extends BaseFromDecimalCast
    {
        public DecimalToBigintCast()
        {
            super(BIGINT);
        }

        public static long castFromShortDecimal(long decimal, long precision, long scale, long tenToScale)
        {
            if (decimal >= 0) {
                return (decimal + tenToScale / 2) / tenToScale;
            }
            else {
                return -((-decimal + tenToScale / 2) / tenToScale);
            }
        }
    }

    public static class BigintToDecimalCast
            extends BaseToDecimalCast
    {
        public BigintToDecimalCast()
        {
            super(BIGINT);
        }

        public static long castToShortDecimal(long bigint, long precision, long scale, long tenToScale)
        {
            try {
                long decimal = multiplyExact(bigint, tenToScale);
                if (Math.abs(decimal) >= TEN_TO_NTH[(int) precision]) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot cast BIGINT " + bigint + " to decimal(" + precision + ", " + scale + ")");
                }
                return decimal;
            }
            catch (ArithmeticException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Cannot cast BIGINT " + bigint + " to decimal(" + precision + ", " + scale + ")");
            }
        }
    }

    public static class DecimalToDoubleCast
            extends BaseFromDecimalCast
    {
        public DecimalToDoubleCast()
        {
            super(DOUBLE);
        }

        public static double castFromShortDecimal(long decimal, long precision, long scale, long tenToScale)
        {
            return ((double) decimal) / tenToScale;
        }
    }

    public static class DecimalToVarcharCast
            extends BaseFromDecimalCast
    {
        public DecimalToVarcharCast()
        {
            super(VARCHAR);
        }

        public static Slice castFromShortDecimal(long decimal, long precision, long scale, long tenToScale)
        {
            return Slices.copiedBuffer(ShortDecimalType.toString(decimal, (int) precision, (int) scale), UTF_8);
        }
    }

    private abstract static class BaseFromDecimalCast
            extends ParametricOperator
    {
        private final String resultType;

        protected BaseFromDecimalCast(String resultType)
        {
            super(CAST, ImmutableList.of(comparableWithVariadicBound("D", DECIMAL)), resultType, ImmutableList.of("D"));
            this.resultType = resultType;
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(arity == 1, "Expected arity to be 1");

            DecimalType aType = (DecimalType) types.get("D");
            checkArgument(aType.getPrecision() <= DecimalType.MAX_SHORT_PRECISION, "Only short decimals are supported");
            long tenToScale = round(pow(10, aType.getScale()));

            MethodHandle methodHandle = Reflection.methodHandle(getClass(), "castFromShortDecimal", long.class, long.class, long.class, long.class);
            methodHandle = MethodHandles.insertArguments(methodHandle, 1, aType.getPrecision(), aType.getScale(), tenToScale);

            return operatorInfo(CAST, parseTypeSignature(resultType), ImmutableList.of(aType.getTypeSignature()), methodHandle, false, ImmutableList.of(false));
        }
    }

    private abstract static class BaseToDecimalCast
            extends ParametricOperator
    {
        private final String argumentType;

        protected BaseToDecimalCast(String argumentType)
        {
            super(CAST, ImmutableList.of(comparableWithVariadicBound("D", DECIMAL)), "D", ImmutableList.of(argumentType));
            this.argumentType = argumentType;
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(arity == 1, "Expected arity to be 1");

            DecimalType aType = (DecimalType) types.get("D");
            checkArgument(aType.getPrecision() <= DecimalType.MAX_SHORT_PRECISION, "Only short decimals are supported");
            long tenToScale = round(pow(10, aType.getScale()));

            Type argumentConcreteType = typeManager.getType(parseTypeSignature(argumentType));
            MethodHandle methodHandle = Reflection.methodHandle(getClass(), "castToShortDecimal", argumentConcreteType.getJavaType(), long.class, long.class, long.class);
            methodHandle = MethodHandles.insertArguments(methodHandle, 1, aType.getPrecision(), aType.getScale(), tenToScale);

            return operatorInfo(CAST, aType.getTypeSignature(), ImmutableList.of(argumentConcreteType.getTypeSignature()), methodHandle, false, ImmutableList.of(false));
        }
    }
}
