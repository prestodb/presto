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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static com.facebook.presto.spi.type.Decimals.overflows;
import static com.facebook.presto.spi.type.StandardTypes.BIGINT;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.StandardTypes.DOUBLE;
import static com.facebook.presto.spi.type.StandardTypes.INTEGER;
import static com.facebook.presto.spi.type.StandardTypes.VARCHAR;
import static com.facebook.presto.util.Types.checkType;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.math.BigDecimal.ROUND_HALF_UP;
import static java.math.BigInteger.ZERO;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DecimalCasts
{
    public static final SqlScalarFunction DECIMAL_TO_BOOLEAN_CAST = castFunctionFromDecimalTo(BOOLEAN, "shortDecimalToBoolean", "longDecimalToBoolean");
    public static final SqlScalarFunction BOOLEAN_TO_DECIMAL_CAST = castFunctionToDecimalFrom(BOOLEAN, "booleanToShortDecimal", "booleanToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_BIGINT_CAST = castFunctionFromDecimalTo(BIGINT, "shortDecimalToBigint", "longDecimalToBigint");
    public static final SqlScalarFunction INTEGER_TO_DECIMAL_CAST = castFunctionToDecimalFrom(INTEGER, "integerToShortDecimal", "integerToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_INTEGER_CAST = castFunctionFromDecimalTo(INTEGER, "shortDecimalToInteger", "longDecimalToInteger");
    public static final SqlScalarFunction BIGINT_TO_DECIMAL_CAST = castFunctionToDecimalFrom(BIGINT, "bigintToShortDecimal", "bigintToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_DOUBLE_CAST = castFunctionFromDecimalTo(DOUBLE, "shortDecimalToDouble", "longDecimalToDouble");
    public static final SqlScalarFunction DOUBLE_TO_DECIMAL_CAST = castFunctionToDecimalFrom(DOUBLE, "doubleToShortDecimal", "doubleToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_VARCHAR_CAST = castFunctionFromDecimalTo(VARCHAR, "shortDecimalToVarchar", "longDecimalToVarchar");
    public static final SqlScalarFunction VARCHAR_TO_DECIMAL_CAST = castFunctionToDecimalFrom(VARCHAR, "varcharToShortDecimal", "varcharToLongDecimal");

    private static SqlScalarFunction castFunctionFromDecimalTo(String to, String... methodNames)
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(CAST)
                .argumentTypes("decimal(precision,scale)")
                .literalParameters("precision", "scale")
                .returnType(to)
                .build();
        return SqlScalarFunction.builder(DecimalCasts.class)
                .signature(signature)
                .implementation(b -> b
                        .methods(methodNames)
                        .withExtraParameters((context) -> {
                            long precision = context.getLiteral("precision");
                            long scale = context.getLiteral("scale");
                            long tenToScale = longTenToNth((int) scale);
                            return ImmutableList.of(precision, scale, tenToScale);
                        })
                )
                .build();
    }

    private static SqlScalarFunction castFunctionToDecimalFrom(String from, String... methodNames)
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(CAST)
                .typeVariableConstraints(withVariadicBound("X", DECIMAL))
                .argumentTypes(from)
                .returnType("X")
                .build();
        return SqlScalarFunction.builder(DecimalCasts.class)
                .signature(signature)
                .implementation(b -> b
                        .methods(methodNames)
                        .withExtraParameters((context) -> {
                            DecimalType resultType = checkType(context.getReturnType(), DecimalType.class, "resultType");
                            long tenToScale = longTenToNth(resultType.getScale());
                            return ImmutableList.of(resultType.getPrecision(), resultType.getScale(), tenToScale);
                        })
                )
                .build();
    }

    private DecimalCasts() {}

    @UsedByGeneratedCode
    public static boolean shortDecimalToBoolean(long decimal, long precision, long scale, long tenToScale)
    {
        return decimal != 0;
    }

    @UsedByGeneratedCode
    public static boolean longDecimalToBoolean(Slice decimal, long precision, long scale, long tenToScale)
    {
        return !decodeUnscaledValue(decimal).equals(ZERO);
    }

    @UsedByGeneratedCode
    public static long booleanToShortDecimal(boolean value, long precision, long scale, long tenToScale)
    {
        return value ? tenToScale : 0;
    }

    @UsedByGeneratedCode
    public static Slice booleanToLongDecimal(boolean value, long precision, long scale, long tenToScale)
    {
        return encodeUnscaledValue(BigInteger.valueOf(value ? tenToScale : 0L));
    }

    @UsedByGeneratedCode
    public static long shortDecimalToBigint(long decimal, long precision, long scale, long tenToScale)
    {
        // this rounds the decimal value to the nearest integral value
        if (decimal >= 0) {
            return (decimal + tenToScale / 2) / tenToScale;
        }
        else {
            return -((-decimal + tenToScale / 2) / tenToScale);
        }
    }

    @UsedByGeneratedCode
    public static long longDecimalToBigint(Slice decimal, long precision, long scale, long tenToScale)
    {
        BigDecimal bigDecimal = new BigDecimal(decodeUnscaledValue(decimal), (int) scale);
        bigDecimal = bigDecimal.setScale(0, RoundingMode.HALF_UP);
        try {
            return bigDecimal.longValueExact();
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to BIGINT", bigDecimal));
        }
    }

    @UsedByGeneratedCode
    public static long bigintToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        try {
            long decimal = multiplyExact(value, tenToScale);
            if (overflows(decimal, (int) precision)) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Slice bigintToLongDecimal(long value, long precision, long scale, long tenToScale)
    {
        BigInteger decimalBigInteger = BigInteger.valueOf(value).multiply(BigInteger.valueOf(tenToScale));
        if (overflows(decimalBigInteger, (int) precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
        return encodeUnscaledValue(decimalBigInteger);
    }

    @UsedByGeneratedCode
    public static long shortDecimalToInteger(long decimal, long precision, long scale, long tenToScale)
    {
        // this rounds the decimal value to the nearest integral value
        long longResult = (decimal + tenToScale / 2) / tenToScale;
        if (decimal < 0) {
            longResult = -((-decimal + tenToScale / 2) / tenToScale);
        }

        try {
            return Math.toIntExact(longResult);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to INTEGER", longResult));
        }
    }

    @UsedByGeneratedCode
    public static long longDecimalToInteger(Slice decimal, long precision, long scale, long tenToScale)
    {
        BigDecimal bigDecimal = new BigDecimal(decodeUnscaledValue(decimal), (int) scale);
        bigDecimal = bigDecimal.setScale(0, RoundingMode.HALF_UP);
        try {
            return bigDecimal.intValueExact();
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to INTEGER", bigDecimal));
        }
    }

    @UsedByGeneratedCode
    public static long integerToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        try {
            long decimal = multiplyExact(value, tenToScale);
            if (overflows(decimal, (int) precision)) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast INTEGER '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast INTEGER '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Slice integerToLongDecimal(long value, long precision, long scale, long tenToScale)
    {
        BigInteger decimalBigInteger = BigInteger.valueOf(value).multiply(BigInteger.valueOf(tenToScale));
        if (overflows(decimalBigInteger, (int) precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
        return encodeUnscaledValue(decimalBigInteger);
    }

    @UsedByGeneratedCode
    public static double shortDecimalToDouble(long decimal, long precision, long scale, long tenToScale)
    {
        return ((double) decimal) / tenToScale;
    }

    @UsedByGeneratedCode
    public static double longDecimalToDouble(Slice decimal, long precision, long scale, long tenToScale)
    {
        BigInteger decimalBigInteger = decodeUnscaledValue(decimal);
        BigDecimal bigDecimal = new BigDecimal(decimalBigInteger, (int) scale);
        return bigDecimal.doubleValue();
    }

    @UsedByGeneratedCode
    public static long doubleToShortDecimal(double value, long precision, long scale, long tenToScale)
    {
        BigDecimal decimal = new BigDecimal(value);
        decimal = decimal.setScale((int) scale, ROUND_HALF_UP);
        if (overflows(decimal, precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
        return decimal.unscaledValue().longValue();
    }

    @UsedByGeneratedCode
    public static Slice doubleToLongDecimal(double value, long precision, long scale, long tenToScale)
    {
        BigDecimal decimal = new BigDecimal(value);
        decimal = decimal.setScale((int) scale, ROUND_HALF_UP);
        if (overflows(decimal, precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
        BigInteger decimalBigInteger = decimal.unscaledValue();
        return encodeUnscaledValue(decimalBigInteger);
    }

    @UsedByGeneratedCode
    public static Slice shortDecimalToVarchar(long decimal, long precision, long scale, long tenToScale)
    {
        return Slices.copiedBuffer(Decimals.toString(decimal, (int) scale), UTF_8);
    }

    @UsedByGeneratedCode
    public static Slice longDecimalToVarchar(Slice decimal, long precision, long scale, long tenToScale)
    {
        return Slices.copiedBuffer(Decimals.toString(decimal, (int) scale), UTF_8);
    }

    @UsedByGeneratedCode
    public static long varcharToShortDecimal(Slice value, long precision, long scale, long tenToScale)
    {
        try {
            String stringValue = value.toString(UTF_8);
            BigDecimal decimal = new BigDecimal(stringValue).setScale((int) scale, ROUND_HALF_UP);
            if (overflows(decimal, precision)) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s)", stringValue, precision, scale));
            }
            return decimal.unscaledValue().longValue();
        }
        catch (NumberFormatException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s)", value.toString(UTF_8), precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Slice varcharToLongDecimal(Slice value, long precision, long scale, long tenToScale)
    {
        String stringValue = value.toString(UTF_8);
        BigDecimal decimal = new BigDecimal(stringValue).setScale((int) scale, ROUND_HALF_UP);
        if (overflows(decimal, precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s)", stringValue, precision, scale));
        }
        return encodeUnscaledValue(decimal.unscaledValue());
    }
}
