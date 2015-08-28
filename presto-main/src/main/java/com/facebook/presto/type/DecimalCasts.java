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

import com.facebook.presto.metadata.ParametricFunction;
import com.facebook.presto.metadata.ParametricFunctionBuilder.SpecializeContext;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;

import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.util.DecimalUtils.overflows;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.math.BigInteger.ZERO;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DecimalCasts
{
    public static final ParametricFunction DECIMAL_TO_BOOLEAN_CAST = castFunctionFromDecimalTo(BOOLEAN, "shortDecimalToBoolean", "longDecimalToBoolean");
    public static final ParametricFunction BOOLEAN_TO_DECIMAL_CAST = castFunctionToDecimalFrom(BOOLEAN, "booleanToShortDecimal", "booleanToLongDecimal");
    public static final ParametricFunction DECIMAL_TO_BIGINT_CAST = castFunctionFromDecimalTo(BIGINT, "shortDecimalToBigint", "longDecimalToBigint");
    public static final ParametricFunction BIGINT_TO_DECIMAL_CAST = castFunctionToDecimalFrom(BIGINT, "bigintToShortDecimal", "bigintToLongDecimal");
    public static final ParametricFunction DECIMAL_TO_DOUBLE_CAST = castFunctionFromDecimalTo(DOUBLE, "shortDecimalToDouble", "longDecimalToDouble");
    public static final ParametricFunction DOUBLE_TO_DECIMAL_CAST = castFunctionToDecimalFrom(DOUBLE, "doubleToShortDecimal", "doubleToLongDecimal");
    public static final ParametricFunction DECIMAL_TO_VARCHAR_CAST = castFunctionFromDecimalTo(VARCHAR, "shortDecimalToVarchar", "longDecimalToVarchar");
    public static final ParametricFunction VARCHAR_TO_DECIMAL_CAST = castFunctionToDecimalFrom(VARCHAR, "varcharToShortDecimal", "varcharToLongDecimal");

    private static ParametricFunction castFunctionFromDecimalTo(String to, String... methodNames)
    {
        Signature signature = Signature.builder()
                .operatorType(CAST)
                .argumentTypes("decimal(precision, scale)")
                .returnType(to)
                .build();
        return ParametricFunction.builder(DecimalCasts.class)
                .signature(signature)
                .methods(methodNames)
                .extraParameters(DecimalCasts::castExtraParametersFromLiterals)
                .build();
    }

    private static ParametricFunction castFunctionToDecimalFrom(String from, String... methodNames)
    {
        Signature signature = Signature.builder()
                .operatorType(CAST)
                .typeParameters(comparableWithVariadicBound("X", DECIMAL))
                .argumentTypes(from)
                .returnType("X")
                .build();
        return ParametricFunction.builder(DecimalCasts.class)
                .signature(signature)
                .methods(methodNames)
                .extraParameters(DecimalCasts::castExtraParametersFromTypeParameters)
                .build();
    }

    private DecimalCasts() {}

    private static List<Object> castExtraParametersFromLiterals(SpecializeContext context)
    {
        long precision = context.getLiteral("precision");
        long scale = context.getLiteral("scale");
        long tenToScale = ShortDecimalType.tenToNth((int) scale);
        return ImmutableList.of(precision, scale, tenToScale);
    }

    private static List<Object> castExtraParametersFromTypeParameters(SpecializeContext context)
    {
        DecimalType resultType = (DecimalType) context.getType("X");
        long tenToScale = ShortDecimalType.tenToNth(resultType.getScale());
        return ImmutableList.of(resultType.getPrecision(), resultType.getScale(), tenToScale);
    }

    public static boolean shortDecimalToBoolean(long decimal, long precision, long scale, long tenToScale)
    {
        return decimal != 0;
    }

    public static boolean longDecimalToBoolean(Slice decimal, long precision, long scale, long tenToScale)
    {
        BigInteger decimalBigInteger = LongDecimalType.unscaledValueToBigInteger(decimal);
        return !decimalBigInteger.equals(ZERO);
    }

    public static long booleanToShortDecimal(boolean booleanValue, long precision, long scale, long tenToScale)
    {
        return booleanValue ? tenToScale : 0;
    }

    public static Slice booleanToLongDecimal(boolean booleanValue, long precision, long scale, long tenToScale)
    {
        return LongDecimalType.unscaledValueToSlice(BigInteger.valueOf(booleanValue ? tenToScale : 0L));
    }

    public static long shortDecimalToBigint(long decimal, long precision, long scale, long tenToScale)
    {
        if (decimal >= 0) {
            return (decimal + tenToScale / 2) / tenToScale;
        }
        else {
            return -((-decimal + tenToScale / 2) / tenToScale);
        }
    }

    public static long longDecimalToBigint(Slice decimal, long precision, long scale, long tenToScale)
    {
        BigInteger decimalBigInteger = LongDecimalType.unscaledValueToBigInteger(decimal);
        BigDecimal bigDecimal = new BigDecimal(decimalBigInteger, (int) scale);
        bigDecimal = bigDecimal.setScale(0, RoundingMode.HALF_UP);
        try {
            return bigDecimal.longValueExact();
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to BIGINT", bigDecimal));
        }
    }

    public static long bigintToShortDecimal(long bigint, long precision, long scale, long tenToScale)
    {
        try {
            long decimal = multiplyExact(bigint, tenToScale);
            if (overflows(decimal, (int) precision)) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", bigint, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", bigint, precision, scale));
        }
    }

    public static Slice bigintToLongDecimal(long bigint, long precision, long scale, long tenToScale)
    {
        BigInteger decimalBigInteger = BigInteger.valueOf(bigint);
        decimalBigInteger = decimalBigInteger.multiply(BigInteger.valueOf(tenToScale));
        if (overflows(decimalBigInteger, (int) precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", bigint, precision, scale));
        }
        return LongDecimalType.unscaledValueToSlice(decimalBigInteger);
    }

    public static double shortDecimalToDouble(long decimal, long precision, long scale, long tenToScale)
    {
        return ((double) decimal) / tenToScale;
    }

    public static double longDecimalToDouble(Slice decimal, long precision, long scale, long tenToScale)
    {
        BigInteger decimalBigInteger = LongDecimalType.unscaledValueToBigInteger(decimal);
        BigDecimal bigDecimal = new BigDecimal(decimalBigInteger, (int) scale);
        return bigDecimal.doubleValue();
    }

    public static long doubleToShortDecimal(double doubleValue, long precision, long scale, long tenToScale)
    {
        BigDecimal decimal = new BigDecimal(doubleValue);
        decimal = decimal.setScale((int) scale, ROUND_HALF_UP);
        if (decimal.precision() > precision) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", doubleValue, precision, scale));
        }
        return decimal.unscaledValue().longValue();
    }

    public static Slice doubleToLongDecimal(double doubleValue, long precision, long scale, long tenToScale)
    {
        BigDecimal decimal = new BigDecimal(doubleValue);
        decimal = decimal.setScale((int) scale, ROUND_HALF_UP);
        if (decimal.precision() > precision) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", doubleValue, precision, scale));
        }
        BigInteger decimalBigInteger = decimal.unscaledValue();
        return LongDecimalType.unscaledValueToSlice(decimalBigInteger);
    }

    public static Slice shortDecimalToVarchar(long decimal, long precision, long scale, long tenToScale)
    {
        return Slices.copiedBuffer(ShortDecimalType.toString(decimal, (int) precision, (int) scale), UTF_8);
    }

    public static Slice longDecimalToVarchar(Slice decimal, long precision, long scale, long tenToScale)
    {
        return Slices.copiedBuffer(LongDecimalType.toString(decimal, (int) precision, (int) scale), UTF_8);
    }

    public static long varcharToShortDecimal(Slice varchar, long precision, long scale, long tenToScale)
    {
        try {
            String varcharStr = varchar.toString(UTF_8);
            BigDecimal decimal = new BigDecimal(varcharStr);
            decimal = decimal.setScale((int) scale, ROUND_HALF_UP);
            if (decimal.precision() > precision) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s)", varcharStr, precision, scale));
            }
            return decimal.unscaledValue().longValue();
        }
        catch (NumberFormatException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s)", varchar.toString(UTF_8), precision, scale));
        }
    }

    public static Slice varcharToLongDecimal(Slice varchar, long precision, long scale, long tenToScale)
    {
        String varcharStr = varchar.toString(UTF_8);
        BigDecimal decimal = new BigDecimal(varcharStr);
        decimal = decimal.setScale((int) scale, ROUND_HALF_UP);
        if (decimal.precision() > precision) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s)", varcharStr, precision, scale));
        }
        return LongDecimalType.unscaledValueToSlice(decimal.unscaledValue());
    }
}
