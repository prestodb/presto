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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.List;

import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.type.LongDecimalType.unscaledValueToBigInteger;
import static com.facebook.presto.spi.type.LongDecimalType.unscaledValueToSlice;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.util.DecimalUtils.overflows;
import static java.lang.String.format;

public final class DecimalToDecimalCasts
{
    public static final Signature SIGNATURE = Signature.builder()
            .operatorType(CAST)
            .typeParameters(comparableWithVariadicBound("X", DECIMAL), comparableWithVariadicBound("Y", DECIMAL))
            .argumentTypes("X")
            .returnType("Y")
            .build();

    // TODO: filtering mechanism could be used to return NoOp method when only precision is increased
    public static final ParametricFunction DECIMAL_TO_DECIMAL_CAST = ParametricFunction.builder(DecimalToDecimalCasts.class)
            .signature(SIGNATURE)
            .methods("shortToShortCast")
            .extraParameters(DecimalToDecimalCasts::shortToShortExtraParameters)
            .methods("shortToLongCast", "longToShortCast", "longToLongCast")
            .extraParameters(DecimalToDecimalCasts::precisionAndScaleExtraParameters)
            .build();

    private DecimalToDecimalCasts() {}

    private static List<Object> shortToShortExtraParameters(SpecializeContext context)
    {
        DecimalType argumentType = (DecimalType) context.getType("X");
        DecimalType resultType = (DecimalType) context.getType("Y");
        long rescale = ShortDecimalType.tenToNth(Math.abs(resultType.getScale() - argumentType.getScale()));
        return ImmutableList.of(
                argumentType.getPrecision(), argumentType.getScale(),
                resultType.getPrecision(), resultType.getScale(),
                rescale, rescale / 2);
    }

    public static long shortToShortCast(long value,
            long sourcePrecision, long sourceScale, long resultPrecision, long resultScale,
            long rescale, long halfRescale)
    {
        long returnValue;
        if (resultScale >= sourceScale) {
            returnValue = value * rescale;
        }
        else {
            returnValue = value / rescale;
            if (value >= 0) {
                if (value % rescale >= halfRescale) {
                    returnValue++;
                }
            }
            else {
                if (value % rescale <= -halfRescale) {
                    returnValue--;
                }
            }
        }
        if (overflows(returnValue, (int) resultPrecision)) {
            throwCastException(value, sourcePrecision, sourceScale, resultPrecision, resultScale);
        }
        return returnValue;
    }

    private static List<Object> precisionAndScaleExtraParameters(SpecializeContext context)
    {
        DecimalType argumentType = (DecimalType) context.getType("X");
        DecimalType resultType = (DecimalType) context.getType("Y");
        return ImmutableList.of(
                argumentType.getPrecision(), argumentType.getScale(),
                resultType.getPrecision(), resultType.getScale());
    }

    public static Slice shortToLongCast(long value,
            long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        return unscaledValueToSlice(bigintToBigintCast(BigInteger.valueOf(value), sourcePrecision, sourceScale, resultPrecision, resultScale));
    }

    public static Slice longToLongCast(Slice value,
            long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        return unscaledValueToSlice(bigintToBigintCast(unscaledValueToBigInteger(value), sourcePrecision, sourceScale, resultPrecision, resultScale));
    }

    public static long longToShortCast(Slice value,
            long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        return bigintToBigintCast(unscaledValueToBigInteger(value), sourcePrecision, sourceScale, resultPrecision, resultScale).longValue();
    }

    private static BigInteger bigintToBigintCast(BigInteger value,
            long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        BigDecimal bigDecimal = new BigDecimal(value, (int) sourceScale);
        bigDecimal = bigDecimal.setScale((int) resultScale, RoundingMode.HALF_UP);
        if (bigDecimal.precision() > resultPrecision) {
            throwCastException(value, sourcePrecision, sourceScale, resultPrecision, resultScale);
        }
        return bigDecimal.unscaledValue();
    }

    private static void throwCastException(long value, long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DECIMAL '%s' to DECIMAL(%d, %d)",
                ShortDecimalType.toString(value, (int) sourcePrecision, (int) sourceScale),
                resultPrecision, resultScale));
    }

    private static void throwCastException(BigInteger value, long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        throw new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DECIMAL '%s' to DECIMAL(%d, %d)",
                LongDecimalType.toString(value, (int) sourcePrecision, (int) sourceScale),
                resultPrecision, resultScale));
    }
}
