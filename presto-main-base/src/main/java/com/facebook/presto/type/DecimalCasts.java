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
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UnscaledDecimal128Arithmetic;
import com.facebook.presto.metadata.SignatureBuilder;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.util.JsonCastException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Shorts;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.Decimals.bigIntegerTenToNth;
import static com.facebook.presto.common.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.common.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.common.type.Decimals.longTenToNth;
import static com.facebook.presto.common.type.Decimals.overflows;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.compareAbsolute;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.multiply;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.overflows;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.rescale;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLong;
import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLongUnsafe;
import static com.facebook.presto.operator.scalar.JsonOperators.JSON_FACTORY;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.JsonUtil.createJsonGenerator;
import static com.facebook.presto.util.JsonUtil.createJsonParser;
import static com.facebook.presto.util.JsonUtil.currentTokenAsLongDecimal;
import static com.facebook.presto.util.JsonUtil.currentTokenAsShortDecimal;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Float.parseFloat;
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
    public static final SqlScalarFunction JSON_TO_DECIMAL_CAST = castFunctionToDecimalFromBuilder(JSON.getTypeSignature(), true, "jsonToShortDecimal", "jsonToLongDecimal");

    /**
     * Powers of 10 which can be represented exactly in double.
     */
    private static final double[] DOUBLE_10_POW = {
            1.0e0, 1.0e1, 1.0e2, 1.0e3, 1.0e4, 1.0e5,
            1.0e6, 1.0e7, 1.0e8, 1.0e9, 1.0e10, 1.0e11,
            1.0e12, 1.0e13, 1.0e14, 1.0e15, 1.0e16, 1.0e17,
            1.0e18, 1.0e19, 1.0e20, 1.0e21, 1.0e22
    };

    /**
     * Powers of 10 which can be represented exactly in float.
     */
    private static final float[] FLOAT_10_POW = {
            1.0e0f, 1.0e1f, 1.0e2f, 1.0e3f, 1.0e4f, 1.0e5f,
            1.0e6f, 1.0e7f, 1.0e8f, 1.0e9f, 1.0e10f
    };

    private static final Slice MAX_EXACT_DOUBLE = unscaledDecimal((1L << 52) - 1);
    private static final Slice MAX_EXACT_FLOAT = unscaledDecimal((1L << 22) - 1);

    private static SqlScalarFunction castFunctionFromDecimalTo(TypeSignature to, String... methodNames)
    {
        Signature signature = SignatureBuilder.builder()
                .kind(SCALAR)
                .operatorType(CAST)
                .argumentTypes(parseTypeSignature("decimal(precision,scale)", ImmutableSet.of("precision", "scale")))
                .returnType(to)
                .build();
        return SqlScalarFunction.builder(DecimalCasts.class, CAST)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .implementation(methodsGroup -> methodsGroup
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
                                })))
                .build();
    }

    private static SqlScalarFunction castFunctionToDecimalFrom(TypeSignature from, String... methodNames)
    {
        return castFunctionToDecimalFromBuilder(from, false, methodNames);
    }

    private static SqlScalarFunction castFunctionToDecimalFromBuilder(TypeSignature from, boolean nullableResult, String... methodNames)
    {
        Signature signature = SignatureBuilder.builder()
                .kind(SCALAR)
                .operatorType(CAST)
                .argumentTypes(from)
                .returnType(parseTypeSignature("decimal(precision,scale)", ImmutableSet.of("precision", "scale")))
                .build();
        return SqlScalarFunction.builder(DecimalCasts.class, CAST)
                .signature(signature)
                .deterministic(true)
                .choice(choice -> choice
                        .nullableResult(nullableResult)
                        .implementation(methodsGroup -> methodsGroup
                                .methods(methodNames)
                                .withExtraParameters((context) -> {
                                    DecimalType resultType = (DecimalType) context.getReturnType();
                                    Number tenToScale;
                                    if (isShortDecimal(resultType)) {
                                        tenToScale = longTenToNth(resultType.getScale());
                                    }
                                    else {
                                        tenToScale = bigIntegerTenToNth(resultType.getScale());
                                    }
                                    return ImmutableList.of(resultType.getPrecision(), resultType.getScale(), tenToScale);
                                }))).build();
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
        return unscaledDecimal(value ? tenToScale : ZERO);
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
        try {
            return unscaledDecimalToUnscaledLong(rescale(decimal, intScale(-scale)));
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to BIGINT", Decimals.toString(decimal, intScale(scale))));
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
        try {
            Slice decimal = multiply(unscaledDecimal(value), unscaledDecimal(tenToScale));
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
        try {
            return toIntExact(unscaledDecimalToUnscaledLong(rescale(decimal, intScale(-scale))));
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to INTEGER", Decimals.toString(decimal, intScale(scale))));
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
        try {
            Slice decimal = multiply(unscaledDecimal(value), unscaledDecimal(tenToScale));
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
        try {
            return Shorts.checkedCast(unscaledDecimalToUnscaledLong(rescale(decimal, intScale(-scale))));
        }
        catch (ArithmeticException | IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to SMALLINT", Decimals.toString(decimal, intScale(scale))));
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
        try {
            Slice decimal = multiply(unscaledDecimal(value), unscaledDecimal(tenToScale));
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
        try {
            return SignedBytes.checkedCast(unscaledDecimalToUnscaledLong(rescale(decimal, intScale(-scale))));
        }
        catch (ArithmeticException | IllegalArgumentException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to TINYINT", Decimals.toString(decimal, intScale(scale))));
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
        try {
            Slice decimal = multiply(unscaledDecimal(value), unscaledDecimal(tenToScale));
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
    public static double shortDecimalToDouble(long decimal, long precision, long scale, long tenToScale)
    {
        return ((double) decimal) / tenToScale;
    }

    @UsedByGeneratedCode
    public static double longDecimalToDouble(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        // If both decimal and scale can be represented exactly in double then compute rescaled and rounded result directly in double.
        if (scale < DOUBLE_10_POW.length && compareAbsolute(decimal, MAX_EXACT_DOUBLE) <= 0) {
            return (double) unscaledDecimalToUnscaledLongUnsafe(decimal) / DOUBLE_10_POW[intScale(scale)];
        }

        // TODO: optimize and convert directly to double in similar fashion as in double to decimal casts
        return parseDouble(Decimals.toString(decimal, intScale(scale)));
    }

    @UsedByGeneratedCode
    public static long shortDecimalToReal(long decimal, long precision, long scale, long tenToScale)
    {
        return floatToRawIntBits(((float) decimal) / tenToScale);
    }

    @UsedByGeneratedCode
    public static long longDecimalToReal(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        // If both decimal and scale can be represented exactly in float then compute rescaled and rounded result directly in float.
        if (scale < FLOAT_10_POW.length && compareAbsolute(decimal, MAX_EXACT_FLOAT) <= 0) {
            return floatToRawIntBits((float) unscaledDecimalToUnscaledLongUnsafe(decimal) / FLOAT_10_POW[intScale(scale)]);
        }

        // TODO: optimize and convert directly to float in similar fashion as in double to decimal casts
        return floatToRawIntBits(parseFloat(Decimals.toString(decimal, intScale(scale))));
    }

    @UsedByGeneratedCode
    public static long doubleToShortDecimal(double value, long precision, long scale, long tenToScale)
    {
        // TODO: implement specialized version for short decimals
        Slice decimal = internalDoubleToLongDecimal(value, precision, scale);

        long low = UnscaledDecimal128Arithmetic.getLong(decimal, 0);
        long high = UnscaledDecimal128Arithmetic.getLong(decimal, 1);

        checkState(high == 0 && low >= 0, "Unexpected long decimal");

        if (UnscaledDecimal128Arithmetic.isNegative(decimal)) {
            return -low;
        }
        else {
            return low;
        }
    }

    @UsedByGeneratedCode
    public static Slice doubleToLongDecimal(double value, long precision, long scale, BigInteger tenToScale)
    {
        return internalDoubleToLongDecimal(value, precision, scale);
    }

    private static Slice internalDoubleToLongDecimal(double value, long precision, long scale)
    {
        if (Double.isInfinite(value) || Double.isNaN(value)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }

        try {
            // todo consider changing this implementation to more performant one which does not use intermediate String objects
            BigDecimal bigDecimal = BigDecimal.valueOf(value).setScale(intScale(scale), HALF_UP);
            Slice decimal = Decimals.encodeScaledValue(bigDecimal);
            if (overflows(decimal, intScale(precision))) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DOUBLE '%s' to DECIMAL(%s, %s)", value, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static long realToShortDecimal(long value, long precision, long scale, long tenToScale)
    {
        // TODO: implement specialized version for short decimals
        Slice decimal = realToLongDecimal(value, precision, scale);

        long low = UnscaledDecimal128Arithmetic.getLong(decimal, 0);
        long high = UnscaledDecimal128Arithmetic.getLong(decimal, 1);

        checkState(high == 0 && low >= 0, "Unexpected long decimal");

        if (UnscaledDecimal128Arithmetic.isNegative(decimal)) {
            return -low;
        }
        else {
            return low;
        }
    }

    @UsedByGeneratedCode
    public static Slice realToLongDecimal(long value, long precision, long scale, BigInteger tenToScale)
    {
        return realToLongDecimal(value, precision, scale);
    }

    private static Slice realToLongDecimal(long value, long precision, long scale)
    {
        float floatValue = intBitsToFloat(intScale(value));
        if (Float.isInfinite(floatValue) || Float.isNaN(floatValue)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", floatValue, precision, scale));
        }

        try {
            // todo consider changing this implementation to more performant one which does not use intermediate String objects
            BigDecimal bigDecimal = new BigDecimal(String.valueOf(floatValue)).setScale(intScale(scale), HALF_UP);
            Slice decimal = Decimals.encodeScaledValue(bigDecimal);
            if (overflows(decimal, intScale(precision))) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", floatValue, precision, scale));
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast REAL '%s' to DECIMAL(%s, %s)", floatValue, precision, scale));
        }
    }

    @UsedByGeneratedCode
    public static Slice shortDecimalToVarchar(long decimal, long precision, long scale, long tenToScale)
    {
        return utf8Slice(Decimals.toString(decimal, intScale(scale)));
    }

    @UsedByGeneratedCode
    public static Slice longDecimalToVarchar(Slice decimal, long precision, long scale, BigInteger tenToScale)
    {
        return utf8Slice(Decimals.toString(decimal, intScale(scale)));
    }

    @UsedByGeneratedCode
    public static long varcharToShortDecimal(Slice value, long precision, long scale, long tenToScale)
    {
        BigDecimal result;
        String stringValue = value.toString(UTF_8);
        try {
            result = new BigDecimal(stringValue).setScale(intScale(scale), HALF_UP);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value is not a number.", stringValue, precision, scale));
        }

        if (overflows(result, precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value too large.", stringValue, precision, scale));
        }

        return result.unscaledValue().longValue();
    }

    @UsedByGeneratedCode
    public static Slice varcharToLongDecimal(Slice value, long precision, long scale, BigInteger tenToScale)
    {
        BigDecimal result;
        String stringValue = value.toString(UTF_8);
        try {
            result = new BigDecimal(stringValue).setScale(intScale(scale), HALF_UP);
        }
        catch (NumberFormatException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value is not a number.", stringValue, precision, scale));
        }

        if (overflows(result, precision)) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast VARCHAR '%s' to DECIMAL(%s, %s). Value too large.", stringValue, precision, scale));
        }

        return encodeUnscaledValue(result.unscaledValue());
    }

    @UsedByGeneratedCode
    public static Slice shortDecimalToJson(long decimal, long precision, long scale, long tenToScale)
    {
        return decimalToJson(BigDecimal.valueOf(decimal, intScale(scale)));
    }

    @UsedByGeneratedCode
    public static Slice longDecimalToJson(Slice decimal, long precision, long scale, BigInteger tenToScale)
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
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Slice result = currentTokenAsLongDecimal(parser, intPrecision(precision), intScale(scale));
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to DECIMAL(%s,%s)", precision, scale); // check no trailing token
            return result;
        }
        catch (IOException | NumberFormatException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to DECIMAL(%s,%s)", json.toStringUtf8(), precision, scale), e);
        }
    }

    @UsedByGeneratedCode
    public static Long jsonToShortDecimal(Slice json, long precision, long scale, long tenToScale)
    {
        try (JsonParser parser = createJsonParser(JSON_FACTORY, json)) {
            parser.nextToken();
            Long result = currentTokenAsShortDecimal(parser, intPrecision(precision), intScale(scale));
            checkCondition(parser.nextToken() == null, INVALID_CAST_ARGUMENT, "Cannot cast input json to DECIMAL(%s,%s)", precision, scale); // check no trailing token
            return result;
        }
        catch (IOException | NumberFormatException | JsonCastException e) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast '%s' to DECIMAL(%s,%s)", json.toStringUtf8(), precision, scale), e);
        }
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static int intPrecision(long precision)
    {
        return (int) precision;
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static int intScale(long scale)
    {
        return (int) scale;
    }
}
