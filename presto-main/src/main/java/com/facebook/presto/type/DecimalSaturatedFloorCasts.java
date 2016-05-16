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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.OperatorType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.spi.type.Decimals.bigIntegerTenToNth;
import static com.facebook.presto.spi.type.Decimals.decodeUnscaledValue;
import static com.facebook.presto.spi.type.Decimals.encodeUnscaledValue;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
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
        BigDecimal bigDecimal = new BigDecimal(value, sourceScale);
        bigDecimal = bigDecimal.setScale(resultScale, FLOOR);
        BigInteger unscaledValue = bigDecimal.unscaledValue();
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
}
