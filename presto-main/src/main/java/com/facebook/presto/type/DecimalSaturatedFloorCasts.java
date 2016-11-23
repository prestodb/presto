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
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.Decimals.bigIntegerTenToNth;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.Float.intBitsToFloat;
import static java.math.BigInteger.ONE;
import static java.math.RoundingMode.FLOOR;

public final class DecimalSaturatedFloorCasts
{
    private DecimalSaturatedFloorCasts() {}

    public static final SqlScalarFunction DECIMAL_TO_DECIMAL_SATURATED_FLOOR_CAST = SqlScalarFunction.builder(DecimalSaturatedFloorCasts.class)
            .signature(Signature.builder()
                    .kind(SCALAR)
                    .operatorType(SATURATED_FLOOR_CAST)
                    .argumentTypes(parseTypeSignature("decimal(source_precision,source_scale)", ImmutableSet.of("source_precision", "source_scale")))
                    .returnType(parseTypeSignature("decimal(result_precision,result_scale)", ImmutableSet.of("result_precision", "result_scale")))
                    .build()
            )
            .implementation(b -> b
                    .methods("shortDecimalToShortDecimal", "shortDecimalToLongDecimal", "longDecimalToShortDecimal", "longDecimalToLongDecimal")
                    .withExtraParameters((context) -> {
                        int sourcePrecision = Ints.checkedCast(context.getLiteral("source_precision"));
                        int sourceScale = Ints.checkedCast(context.getLiteral("source_scale"));
                        int resultPrecision = Ints.checkedCast(context.getLiteral("result_precision"));
                        int resultScale = Ints.checkedCast(context.getLiteral("result_scale"));
                        return ImmutableList.of(sourcePrecision, sourceScale, resultPrecision, resultScale);
                    })
            ).build();

    @UsedByGeneratedCode
    public static long shortDecimalToShortDecimal(long value, int sourcePrecision, int sourceScale, int resultPrecision, int resultScale)
    {
        return bigintToBigintFloorSaturatedCast(BigInteger.valueOf(value), sourceScale, resultPrecision, resultScale).longValueExact();
    }

    @UsedByGeneratedCode
    public static Slice shortDecimalToLongDecimal(long value, int sourcePrecision, int sourceScale, int resultPrecision, int resultScale)
    {
        return encodeUnscaledValue(bigintToBigintFloorSaturatedCast(BigInteger.valueOf(value), sourceScale, resultPrecision, resultScale));
    }

    @UsedByGeneratedCode
    public static long longDecimalToShortDecimal(Slice value, int sourcePrecision, int sourceScale, int resultPrecision, int resultScale)
    {
        return bigintToBigintFloorSaturatedCast(decodeUnscaledValue(value), sourceScale, resultPrecision, resultScale).longValueExact();
    }

    @UsedByGeneratedCode
    public static Slice longDecimalToLongDecimal(Slice value, int sourcePrecision, int sourceScale, int resultPrecision, int resultScale)
    {
        return encodeUnscaledValue(bigintToBigintFloorSaturatedCast(decodeUnscaledValue(value), sourceScale, resultPrecision, resultScale));
    }

    private static BigInteger bigintToBigintFloorSaturatedCast(BigInteger value, int sourceScale, int resultPrecision, int resultScale)
    {
        return bigDecimalToBigintFloorSaturatedCast(new BigDecimal(value, sourceScale), resultPrecision, resultScale);
    }

    private static BigInteger bigDecimalToBigintFloorSaturatedCast(BigDecimal bigDecimal, int resultPrecision, int resultScale)
    {
        BigDecimal rescaledValue = bigDecimal.setScale(resultScale, FLOOR);
        BigInteger unscaledValue = rescaledValue.unscaledValue();
        BigInteger maxUnscaledValue = bigIntegerTenToNth(resultPrecision).subtract(ONE);
        if (unscaledValue.compareTo(maxUnscaledValue) > 0) {
            return maxUnscaledValue;
        }
        BigInteger minUnscaledValue = maxUnscaledValue.negate();
        if (unscaledValue.compareTo(minUnscaledValue) < 0) {
            return minUnscaledValue;
        }
        return unscaledValue;
    }

    public static final SqlScalarFunction DOUBLE_TO_DECIMAL_SATURATED_FLOOR_CAST = SqlScalarFunction.builder(DecimalSaturatedFloorCasts.class)
            .signature(Signature.builder()
                    .kind(SCALAR)
                    .operatorType(SATURATED_FLOOR_CAST)
                    .argumentTypes(DOUBLE.getTypeSignature())
                    .returnType(parseTypeSignature("decimal(result_precision,result_scale)", ImmutableSet.of("result_precision", "result_scale")))
                    .build()
            )
            .implementation(b -> b
                    .methods("doubleToShortDecimal", "doubleToLongDecimal")
                    .withExtraParameters((context) -> {
                        int resultPrecision = Ints.checkedCast(context.getLiteral("result_precision"));
                        int resultScale = Ints.checkedCast(context.getLiteral("result_scale"));
                        return ImmutableList.of(resultPrecision, resultScale);
                    })
            ).build();

    @UsedByGeneratedCode
    public static long doubleToShortDecimal(double value, int resultPrecision, int resultScale)
    {
        return bigDecimalToBigintFloorSaturatedCast(new BigDecimal(value), resultPrecision, resultScale).longValueExact();
    }

    @UsedByGeneratedCode
    public static Slice doubleToLongDecimal(double value, int resultPrecision, int resultScale)
    {
        return encodeUnscaledValue(bigDecimalToBigintFloorSaturatedCast(new BigDecimal(value), resultPrecision, resultScale));
    }

    public static final SqlScalarFunction REAL_TO_DECIMAL_SATURATED_FLOOR_CAST = SqlScalarFunction.builder(DecimalSaturatedFloorCasts.class)
            .signature(Signature.builder()
                    .kind(SCALAR)
                    .operatorType(SATURATED_FLOOR_CAST)
                    .argumentTypes(REAL.getTypeSignature())
                    .returnType(parseTypeSignature("decimal(result_precision,result_scale)", ImmutableSet.of("result_precision", "result_scale")))
                    .build()
            )
            .implementation(b -> b
                    .methods("realToShortDecimal", "realToLongDecimal")
                    .withExtraParameters((context) -> {
                        int resultPrecision = Ints.checkedCast(context.getLiteral("result_precision"));
                        int resultScale = Ints.checkedCast(context.getLiteral("result_scale"));
                        return ImmutableList.of(resultPrecision, resultScale);
                    })
            ).build();

    @UsedByGeneratedCode
    public static long realToShortDecimal(long value, int resultPrecision, int resultScale)
    {
        return bigDecimalToBigintFloorSaturatedCast(new BigDecimal(intBitsToFloat((int) value)), resultPrecision, resultScale).longValueExact();
    }

    @UsedByGeneratedCode
    public static Slice realToLongDecimal(long value, int resultPrecision, int resultScale)
    {
        return encodeUnscaledValue(bigDecimalToBigintFloorSaturatedCast(new BigDecimal(intBitsToFloat((int) value)), resultPrecision, resultScale));
    }

    public static final SqlScalarFunction DECIMAL_TO_BIGINT_SATURATED_FLOOR_CAST = decimalToGenericIntegerTypeSaturatedFloorCast(BIGINT, Long.MIN_VALUE, Long.MAX_VALUE);
    public static final SqlScalarFunction DECIMAL_TO_INTEGER_SATURATED_FLOOR_CAST = decimalToGenericIntegerTypeSaturatedFloorCast(INTEGER, Integer.MIN_VALUE, Integer.MAX_VALUE);
    public static final SqlScalarFunction DECIMAL_TO_SMALLINT_SATURATED_FLOOR_CAST = decimalToGenericIntegerTypeSaturatedFloorCast(SMALLINT, Short.MIN_VALUE, Short.MAX_VALUE);
    public static final SqlScalarFunction DECIMAL_TO_TINYINT_SATURATED_FLOOR_CAST = decimalToGenericIntegerTypeSaturatedFloorCast(TINYINT, Byte.MIN_VALUE, Byte.MAX_VALUE);

    private static SqlScalarFunction decimalToGenericIntegerTypeSaturatedFloorCast(Type type, long minValue, long maxValue)
    {
        return SqlScalarFunction.builder(DecimalSaturatedFloorCasts.class)
                .signature(Signature.builder()
                        .kind(SCALAR)
                        .operatorType(SATURATED_FLOOR_CAST)
                        .argumentTypes(parseTypeSignature("decimal(source_precision,source_scale)", ImmutableSet.of("source_precision", "source_scale")))
                        .returnType(type.getTypeSignature())
                        .build()
                )
                .implementation(b -> b
                        .methods("shortDecimalToGenericIntegerType", "longDecimalToGenericIntegerType")
                        .withExtraParameters((context) -> {
                            int sourceScale = Ints.checkedCast(context.getLiteral("source_scale"));
                            return ImmutableList.of(sourceScale, minValue, maxValue);
                        })
                ).build();
    }

    @UsedByGeneratedCode
    public static long shortDecimalToGenericIntegerType(long value, int sourceScale, long minValue, long maxValue)
    {
        return bigIntegerDecimalToGenericIntegerType(BigInteger.valueOf(value), sourceScale, minValue, maxValue);
    }

    @UsedByGeneratedCode
    public static long longDecimalToGenericIntegerType(Slice value, int sourceScale, long minValue, long maxValue)
    {
        return bigIntegerDecimalToGenericIntegerType(decodeUnscaledValue(value), sourceScale, minValue, maxValue);
    }

    private static long bigIntegerDecimalToGenericIntegerType(BigInteger bigInteger, int sourceScale, long minValue, long maxValue)
    {
        BigDecimal bigDecimal = new BigDecimal(bigInteger, sourceScale);
        BigInteger unscaledValue = bigDecimal.setScale(0, FLOOR).unscaledValue();
        if (unscaledValue.compareTo(BigInteger.valueOf(maxValue)) > 0) {
            return maxValue;
        }
        if (unscaledValue.compareTo(BigInteger.valueOf(minValue)) < 0) {
            return minValue;
        }
        return unscaledValue.longValueExact();
    }

    public static final SqlScalarFunction BIGINT_TO_DECIMAL_SATURATED_FLOOR_CAST = genericIntegerTypeToDecimalSaturatedFloorCast(BIGINT);
    public static final SqlScalarFunction INTEGER_TO_DECIMAL_SATURATED_FLOOR_CAST = genericIntegerTypeToDecimalSaturatedFloorCast(INTEGER);
    public static final SqlScalarFunction SMALLINT_TO_DECIMAL_SATURATED_FLOOR_CAST = genericIntegerTypeToDecimalSaturatedFloorCast(SMALLINT);
    public static final SqlScalarFunction TINYINT_TO_DECIMAL_SATURATED_FLOOR_CAST = genericIntegerTypeToDecimalSaturatedFloorCast(TINYINT);

    private static SqlScalarFunction genericIntegerTypeToDecimalSaturatedFloorCast(Type integerType)
    {
        return SqlScalarFunction.builder(DecimalSaturatedFloorCasts.class)
                .signature(Signature.builder()
                        .kind(SCALAR)
                        .operatorType(SATURATED_FLOOR_CAST)
                        .argumentTypes(integerType.getTypeSignature())
                        .returnType(parseTypeSignature("decimal(result_precision,result_scale)", ImmutableSet.of("result_precision", "result_scale")))
                        .build()
                )
                .implementation(b -> b
                        .methods("genericIntegerTypeToShortDecimal", "genericIntegerTypeToLongDecimal")
                        .withExtraParameters((context) -> {
                            int resultPrecision = Ints.checkedCast(context.getLiteral("result_precision"));
                            int resultScale = Ints.checkedCast(context.getLiteral("result_scale"));
                            return ImmutableList.of(resultPrecision, resultScale);
                        })
                ).build();
    }

    @UsedByGeneratedCode
    public static long genericIntegerTypeToShortDecimal(long value, int resultPrecision, int resultScale)
    {
        return bigDecimalToBigintFloorSaturatedCast(BigDecimal.valueOf(value), resultPrecision, resultScale).longValueExact();
    }

    @UsedByGeneratedCode
    public static Slice genericIntegerTypeToLongDecimal(long value, int resultPrecision, int resultScale)
    {
        return encodeUnscaledValue(bigDecimalToBigintFloorSaturatedCast(BigDecimal.valueOf(value), resultPrecision, resultScale));
    }
}
