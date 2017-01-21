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
import com.facebook.presto.metadata.SqlScalarFunctionBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.operator.scalar.JsonOperators.JSON_FACTORY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Decimals.bigIntegerTenToNth;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static com.facebook.presto.spi.type.Decimals.overflows;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.JsonType.JSON;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.JsonUtil.createJsonGenerator;
import static com.facebook.presto.util.JsonUtil.createJsonParser;
import static com.facebook.presto.util.Types.checkType;
import static java.lang.Double.isFinite;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.multiplyExact;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.math.BigInteger.ZERO;
import static java.math.RoundingMode.HALF_UP;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class DecimalCasts
{
    public static final SqlScalarFunction DECIMAL_TO_BOOLEAN_CAST = castFunctionFromDecimalTo(BOOLEAN.getTypeSignature(), "shortDecimalToBoolean", "longDecimalToBoolean");
    public static final SqlScalarFunction BOOLEAN_TO_DECIMAL_CAST = castFunctionToDecimalFrom(BOOLEAN.getTypeSignature(), "booleanToShortDecimal", "booleanToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_BIGINT_CAST = castFunctionFromDecimalTo(BIGINT.getTypeSignature(), "shortDecimalToBigint", "longDecimalToBigint");
    public static final SqlScalarFunction BIGINT_TO_DECIMAL_CAST = castFunctionToDecimalFrom(BIGINT.getTypeSignature(), "bigintToShortDecimal", "bigintToLongDecimal");
    public static final SqlScalarFunction INTEGER_TO_DECIMAL_CAST = castFunctionToDecimalFrom(INTEGER.getTypeSignature(), "integerToShortDecimal", "integerToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_INTEGER_CAST = castFunctionFromDecimalTo(INTEGER.getTypeSignature(), "shortDecimalToInteger", "longDecimalToInteger");
    public static final SqlScalarFunction SMALLINT_TO_DECIMAL_CAST = castFunctionToDecimalFrom(SMALLINT.getTypeSignature(), "smallintToShortDecimal", "smallintToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_SMALLINT_CAST = castFunctionFromDecimalTo(SMALLINT.getTypeSignature(), "shortDecimalToSmallint", "longDecimalToSmallint");
    public static final SqlScalarFunction TINYINT_TO_DECIMAL_CAST = castFunctionToDecimalFrom(TINYINT.getTypeSignature(), "tinyintToShortDecimal", "tinyintToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_TINYINT_CAST = castFunctionFromDecimalTo(TINYINT.getTypeSignature(), "shortDecimalToTinyint", "longDecimalToTinyint");
    public static final SqlScalarFunction DECIMAL_TO_DOUBLE_CAST = castFunctionFromDecimalTo(DOUBLE.getTypeSignature(), "shortDecimalToDouble", "longDecimalToDouble");
    public static final SqlScalarFunction DOUBLE_TO_DECIMAL_CAST = castFunctionToDecimalFrom(DOUBLE.getTypeSignature(), "doubleToShortDecimal", "doubleToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_REAL_CAST = castFunctionFromDecimalTo(REAL.getTypeSignature(), "shortDecimalToReal", "longDecimalToReal");
    public static final SqlScalarFunction REAL_TO_DECIMAL_CAST = castFunctionToDecimalFrom(REAL.getTypeSignature(), "realToShortDecimal", "realToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_VARCHAR_CAST = castFunctionFromDecimalTo(parseTypeSignature("varchar(x)", ImmutableSet.of("x")), "shortDecimalToVarchar", "longDecimalToVarchar");
    public static final SqlScalarFunction VARCHAR_TO_DECIMAL_CAST = castFunctionToDecimalFrom(parseTypeSignature("varchar(x)", ImmutableSet.of("x")), "varcharToShortDecimal", "varcharToLongDecimal");
    public static final SqlScalarFunction DECIMAL_TO_JSON_CAST = castFunctionFromDecimalTo(JSON.getTypeSignature(), "shortDecimalToJson", "longDecimalToJson");
    public static final SqlScalarFunction JSON_TO_DECIMAL_CAST = castFunctionToDecimalFromBuilder(JSON.getTypeSignature(), "jsonToShortDecimal", "jsonToLongDecimal").nullableResult(true).build();

    private static SqlScalarFunction castFunctionFromDecimalTo(TypeSignature to, String... methodNames)
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(CAST)
                .argumentTypes(parseTypeSignature("decimal(precision,scale)", ImmutableSet.of("precision", "scale")))
                .returnType(to)
                .build();
        return SqlScalarFunction.builder(DecimalCasts.class)
                .signature(signature)
                .implementation(b -> b
                        .methods(methodNames)
                        .withExtraParameters((context) -> {
                            long precision = context.getLiteral("precision");
                            long scale = context.getLiteral("scale");
                            Number tenToScale;
                            if (isShortDecimal(context.getParameterTypes().get(0))) {
                                tenToScale = longTenToNth(intScale(scale));
                            }
                            else {
                                tenToScale = bigIntegerTenToNth(intScale(scale));
                            }
                            return ImmutableList.of(precision, scale, tenToScale);
                        })
                )
                .build();
    }

    private static SqlScalarFunction castFunctionToDecimalFrom(TypeSignature from, String... methodNames)
    {
        return castFunctionToDecimalFromBuilder(from, methodNames).build();
    }

    private static SqlScalarFunctionBuilder castFunctionToDecimalFromBuilder(TypeSignature from, String... methodNames)
    {
        Signature signature = Signature.builder()
                .kind(SCALAR)
                .operatorType(CAST)
                .argumentTypes(from)
                .returnType(parseTypeSignature("decimal(precision,scale)", ImmutableSet.of("precision", "scale")))
                .build();
        return SqlScalarFunction.builder(DecimalCasts.class)
                .signature(signature)
                .implementation(b -> b
                        .methods(methodNames)
                        .withExtraParameters((context) -> {
                            DecimalType resultType = checkType(context.getReturnType(), DecimalType.class, "resultType");
                            Number tenToScale;
                            if (isShortDecimal(resultType)) {
                                tenToScale = longTenToNth(resultType.getScale());
                            }
                            else {
                                tenToScale = bigIntegerTenToNth(resultType.getScale());
                            }
                            return ImmutableList.of(resultType.getPrecision(), resultType.getScale(), tenToScale);
                        })
                );
    }

    private DecimalCasts() {}

    @UsedByGeneratedCode
    public static boolean shortDecimalToBoolean(long decimal, long precision, long scale, long tenToScale)
    {
        return decimal != 0;
    }

    @UsedByGeneratedCode
    public static boolean longDecimalToBoolean(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        return !decodeUnscaledValue(decimal).equals(ZERO);
    }

    @UsedByGeneratedCode
    public static long booleanToShortDecimal(boolean value, long precision, long scale, long tenToScale)
    {
        return value ? tenToScale : 0;
    }

    @UsedByGeneratedCode
    public static Slice booleanToLongDecimal(boolean value, long precision, long scale, BigInteger tenToScale)
    {
        return encodeUnscaledValue(value ? tenToScale : ZERO);
    }

    @UsedByGeneratedCode
    public static long shortDecimalToBigint(long decimal, long precision, long scale, long tenToScale)
    {
        // this rounds the decimal value to the nearest integral value
        if (decimal >= 0) {
            return (decimal + tenToScale / 2) / tenToScale;
        }
        return -((-decimal + tenToScale / 2) / tenToScale);
    }

    @UsedByGeneratedCode
    public static long longDecimalToBigint(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        BigDecimal bigDecimal = new BigDecimal(decodeUnscaledValue(decimal), intScale(scale));
        bigDecimal = bigDecimal.setScale(0, HALF_UP);
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
            if (overflows(decimal, intScale(precision))) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast BIGINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Slice bigintToLongDecimal(long value, long precision, long scale, BigInteger tenToScale)
    {
        BigInteger decimalBigInteger = BigInteger.valueOf(value).multiply(tenToScale);
        if (overflows(decimalBigInteger, intScale(precision))) {
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
            return toIntExact(longResult);
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to INTEGER", longResult));
        }
    }

    @UsedByGeneratedCode
    public static long longDecimalToInteger(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        BigDecimal bigDecimal = new BigDecimal(decodeUnscaledValue(decimal), intScale(scale));
        bigDecimal = bigDecimal.setScale(0, HALF_UP);
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
            if (overflows(decimal, intScale(precision))) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast INTEGER '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast INTEGER '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Slice integerToLongDecimal(long value, long precision, long scale, BigInteger tenToScale)
    {
        BigInteger decimalBigInteger = BigInteger.valueOf(value).multiply(tenToScale);
        if (overflows(decimalBigInteger, intScale(precision))) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast INTEGER '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
        return encodeUnscaledValue(decimalBigInteger);
    }

    @UsedByGeneratedCode
    public static long shortDecimalToSmallint(long decimal, long precision, long scale, long tenToScale)
    {
        // this rounds the decimal value to the nearest integral value
        long longResult = (decimal + tenToScale / 2) / tenToScale;
        if (decimal < 0) {
            longResult = -((-decimal + tenToScale / 2) / tenToScale);
        }

        try {
            return Shorts.checkedCast(longResult);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to SMALLINT", longResult));
        }
    }

    @UsedByGeneratedCode
    public static long longDecimalToSmallint(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        BigDecimal bigDecimal = new BigDecimal(decodeUnscaledValue(decimal), intScale(scale));
        bigDecimal = bigDecimal.setScale(0, HALF_UP);
        try {
            return bigDecimal.shortValueExact();
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to SMALLINT", bigDecimal));
        }
    }

    @UsedByGeneratedCode
    public static long smallintToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        try {
            long decimal = multiplyExact(value, tenToScale);
            if (overflows(decimal, intScale(precision))) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast SMALLINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast SMALLINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Slice smallintToLongDecimal(long value, long precision, long scale, BigInteger tenToScale)
    {
        BigInteger decimalBigInteger = BigInteger.valueOf(value).multiply(tenToScale);
        if (overflows(decimalBigInteger, intScale(precision))) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast SMALLINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
        return encodeUnscaledValue(decimalBigInteger);
    }

    @UsedByGeneratedCode
    public static long shortDecimalToTinyint(long decimal, long precision, long scale, long tenToScale)
    {
        // this rounds the decimal value to the nearest integral value
        long longResult = (decimal + tenToScale / 2) / tenToScale;
        if (decimal < 0) {
            longResult = -((-decimal + tenToScale / 2) / tenToScale);
        }

        try {
            return SignedBytes.checkedCast(longResult);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to TINYINT", longResult));
        }
    }

    @UsedByGeneratedCode
    public static long longDecimalToTinyint(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        BigDecimal bigDecimal = new BigDecimal(decodeUnscaledValue(decimal), intScale(scale));
        bigDecimal = bigDecimal.setScale(0, HALF_UP);
        try {
            return bigDecimal.byteValueExact();
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to TINYINT", bigDecimal));
        }
    }

    @UsedByGeneratedCode
    public static long tinyintToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        try {
            long decimal = multiplyExact(value, tenToScale);
            if (overflows(decimal, intScale(precision))) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast TINYINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast TINYINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Slice tinyintToLongDecimal(long value, long precision, long scale, BigInteger tenToScale)
    {
        BigInteger decimalBigInteger = BigInteger.valueOf(value).multiply(tenToScale);
        if (overflows(decimalBigInteger, intScale(precision))) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast TINYINT '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
        return encodeUnscaledValue(decimalBigInteger);
    }

    @UsedByGeneratedCode
    public static double shortDecimalToDouble(long decimal, long precision, long scale, long tenToScale)
    {
        return ((double) decimal) / tenToScale;
    }

    @UsedByGeneratedCode
    public static double longDecimalToDouble(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        BigInteger decimalBigInteger = decodeUnscaledValue(decimal);
        BigDecimal bigDecimal = new BigDecimal(decimalBigInteger, intScale(scale));
        return bigDecimal.doubleValue();
    }

    @UsedByGeneratedCode
    public static long shortDecimalToReal(long decimal, long precision, long scale, long tenToScale)
    {
        return floatToRawIntBits(((float) decimal) / tenToScale);
    }

    @UsedByGeneratedCode
    public static long longDecimalToReal(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        BigInteger decimalBigInteger = decodeUnscaledValue(decimal);
        BigDecimal bigDecimal = new BigDecimal(decimalBigInteger, intScale(scale));
        return floatToRawIntBits(bigDecimal.floatValue());
    }

    @UsedByGeneratedCode
    public static long doubleToShortDecimal(double value, long precision, long scale, long tenToScale)
    {
        BigDecimal decimal = doubleToBigDecimal(value, precision, scale);
        decimal = decimal.setScale(intScale(scale), HALF_UP);
        if (overflows(decimal, precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
        return decimal.unscaledValue().longValue();
    }

    @UsedByGeneratedCode
    public static Slice doubleToLongDecimal(double value, long precision, long scale, BigInteger tenToScale)
    {
        BigDecimal decimal = doubleToBigDecimal(value, precision, scale);
        decimal = decimal.setScale(intScale(scale), HALF_UP);
        if (overflows(decimal, precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
        BigInteger decimalBigInteger = decimal.unscaledValue();
        return encodeUnscaledValue(decimalBigInteger);
    }

    @UsedByGeneratedCode
    public static long realToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        BigDecimal decimal = doubleToBigDecimal(intBitsToFloat(intScale(value)), precision, scale);
        decimal = decimal.setScale(intScale(scale), HALF_UP);
        if (overflows(decimal, precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", intBitsToFloat(intScale(value)), precision, scale));
        }
        return decimal.unscaledValue().longValue();
    }

    @UsedByGeneratedCode
    public static Slice realToLongDecimal(long value, long precision, long scale, BigInteger tenToScale)
    {
        BigDecimal decimal = doubleToBigDecimal(intBitsToFloat(intScale(value)), precision, scale);
        decimal = decimal.setScale(intScale(scale), HALF_UP);
        if (overflows(decimal, precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", intBitsToFloat(intScale(value)), precision, scale));
        }
        BigInteger decimalBigInteger = decimal.unscaledValue();
        return encodeUnscaledValue(decimalBigInteger);
    }

    private static BigDecimal doubleToBigDecimal(double value, long precision, long scale)
    {
        if (!isFinite(value)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast floating point %s to DECIMAL(%s, %s)", value, precision, scale));
        }
        return new BigDecimal(value);
    }

    @UsedByGeneratedCode
    public static Slice shortDecimalToVarchar(long decimal, long precision, long scale, long tenToScale)
    {
        return Slices.copiedBuffer(Decimals.toString(decimal, intScale(scale)), UTF_8);
    }

    @UsedByGeneratedCode
    public static Slice longDecimalToVarchar(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        return Slices.copiedBuffer(Decimals.toString(decimal, intScale(scale)), UTF_8);
    }

    @UsedByGeneratedCode
    public static long varcharToShortDecimal(Slice value, long precision, long scale, long tenToScale)
    {
        try {
            String stringValue = value.toString(UTF_8);
            BigDecimal decimal = new BigDecimal(stringValue).setScale(intScale(scale), HALF_UP);
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
    public static Slice varcharToLongDecimal(Slice value, long precision, long scale, BigInteger tenToScale)
    {
        String stringValue = value.toString(UTF_8);
        BigDecimal decimal = new BigDecimal(stringValue).setScale(intScale(scale), HALF_UP);
        if (overflows(decimal, precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s)", stringValue, precision, scale));
        }
        return encodeUnscaledValue(decimal.unscaledValue());
    }

    @UsedByGeneratedCode
    public static Slice shortDecimalToJson(long decimal, long precision, long scale, long tenToScale)
            throws IOException
    {
        return decimalToJson(BigDecimal.valueOf(decimal, intScale(scale)));
    }

    @UsedByGeneratedCode
    public static Slice longDecimalToJson(Slice decimal, long precision, long scale, BigInteger tenToScale)
            throws IOException
    {
        return decimalToJson(new BigDecimal(decodeUnscaledValue(decimal), intScale(scale)));
    }

    private static Slice decimalToJson(BigDecimal bigDecimal)
    {
        try {
            SliceOutput dynamicSliceOutput = new DynamicSliceOutput(32);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, dynamicSliceOutput)) {
                jsonGenerator.writeNumber(bigDecimal);
            }
            return dynamicSliceOutput.slice();
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%f' to %s", bigDecimal, StandardTypes.JSON));
        }
    }

    @UsedByGeneratedCode
    public static Slice jsonToLongDecimal(Slice json, long precision, long scale, BigInteger tenToScale)
            throws IOException
    {
        BigDecimal bigDecimal = jsonToDecimal(json, precision, scale);
        if (bigDecimal == null) {
            return null;
        }
        return encodeUnscaledValue(bigDecimal.unscaledValue());
    }

    @UsedByGeneratedCode
    public static Long jsonToShortDecimal(Slice json, long precision, long scale, long tenToScale)
            throws IOException
    {
        BigDecimal bigDecimal = jsonToDecimal(json, precision, scale);
        return bigDecimal != null ? bigDecimal.unscaledValue().longValue() : null;
    }

    @Nullable
    private static BigDecimal jsonToDecimal(Slice json, long precision, long scale)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            BigDecimal result;
            switch (parser.getCurrentToken()) {
                case VALUE_NULL:
                    result = null;
                    break;
                case VALUE_STRING:
                    result = new BigDecimal(parser.getText());
                    result = result.setScale(intScale(scale), HALF_UP);
                    break;
                case VALUE_NUMBER_FLOAT:
                case VALUE_NUMBER_INT:
                    result = parser.getDecimalValue();
                    result = result.setScale(intScale(scale), HALF_UP);
                    break;
                case VALUE_TRUE:
                    result = BigDecimal.ONE.setScale(intScale(scale), HALF_UP);
                    break;
                case VALUE_FALSE:
                    result = BigDecimal.ZERO.setScale(intScale(scale), HALF_UP);
                    break;
                default:
                    throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to DECIMAL(%s,%s)", json.toStringUtf8(), precision, scale));
            }
            checkCondition(
                    parser.nextToken() == null &&
                            (result == null || result.precision() <= precision),
                    INVALID_CAST_ARGUMENT, "Cannot cast input json to DECIMAL(%s,%s)", precision, scale); // check no trailing token

            return result;
        }
        catch (IOException | NumberFormatException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to DECIMAL(%s,%s)", json.toStringUtf8(), precision, scale));
        }
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static int intScale(long scale)
    {
        return (int) scale;
    }
}
