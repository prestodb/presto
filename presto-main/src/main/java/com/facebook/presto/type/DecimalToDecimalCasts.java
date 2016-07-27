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

import java.math.BigInteger;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.withVariadicBound;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static com.facebook.presto.spi.type.Decimals.overflows;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.overflows;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLongUnsafe;
import static java.lang.String.format;

public final class DecimalToDecimalCasts
{
    public static final Signature SIGNATURE = Signature.builder()
            .kind(SCALAR)
            .operatorType(CAST)
            .typeVariableConstraints(withVariadicBound("F", DECIMAL), withVariadicBound("T", DECIMAL))
            .argumentTypes(parseTypeSignature("F"))
            .returnType(parseTypeSignature("T"))
            .build();

    // TODO: filtering mechanism could be used to return NoOp method when only precision is increased
    public static final SqlScalarFunction DECIMAL_TO_DECIMAL_CAST = SqlScalarFunction.builder(DecimalToDecimalCasts.class)
            .signature(SIGNATURE)
            .implementation(b -> b
                    .methods("shortToShortCast")
                    .withExtraParameters((context) -> {
                        DecimalType argumentType = (DecimalType) context.getType("F");
                        DecimalType resultType = (DecimalType) context.getType("T");
                        long rescale = longTenToNth(Math.abs(resultType.getScale() - argumentType.getScale()));
                        return ImmutableList.of(
                                argumentType.getPrecision(), argumentType.getScale(),
                                resultType.getPrecision(), resultType.getScale(),
                                rescale, rescale / 2);
                    })
            )
            .implementation(b -> b
                    .methods("shortToLongCast", "longToShortCast", "longToLongCast")
                    .withExtraParameters((context) -> {
                        DecimalType argumentType = (DecimalType) context.getType("F");
                        DecimalType resultType = (DecimalType) context.getType("T");
                        return ImmutableList.of(
                                argumentType.getPrecision(), argumentType.getScale(),
                                resultType.getPrecision(), resultType.getScale());
                    })
            )
            .build();

    private DecimalToDecimalCasts() {}

    @UsedByGeneratedCode
    public static long shortToShortCast(
            long value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale,
            long scalingFactor,
            long halfOfScalingFactor)
    {
        long returnValue;
        if (resultScale >= sourceScale) {
            returnValue = value * scalingFactor;
        }
        else {
            returnValue = value / scalingFactor;
            if (value >= 0) {
                if (value % scalingFactor >= halfOfScalingFactor) {
                    returnValue++;
                }
            }
            else {
                if (value % scalingFactor <= -halfOfScalingFactor) {
                    returnValue--;
                }
            }
        }
        if (overflows(returnValue, (int) resultPrecision)) {
            throw throwCastException(value, sourcePrecision, sourceScale, resultPrecision, resultScale);
        }
        return returnValue;
    }

    @UsedByGeneratedCode
    public static Slice shortToLongCast(
            long value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale)
    {
        return longToLongCast(unscaledDecimal(value), sourcePrecision, sourceScale, resultPrecision, resultScale);
    }

    @UsedByGeneratedCode
    public static long longToShortCast(
            Slice value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale)
    {
        return unscaledDecimalToUnscaledLongUnsafe(longToLongCast(value, sourcePrecision, sourceScale, resultPrecision, resultScale));
    }

    @UsedByGeneratedCode
    public static Slice longToLongCast(
            Slice value,
            long sourcePrecision,
            long sourceScale,
            long resultPrecision,
            long resultScale)
    {
        try {
            Slice result = rescale(value, (int) (resultScale - sourceScale));
            if (overflows(result, (int) resultPrecision)) {
                throw throwCastException(unscaledDecimalToBigInteger(value), sourcePrecision, sourceScale, resultPrecision, resultScale);
            }
            return result;
        }
        catch (ArithmeticException e) {
            throw throwCastException(unscaledDecimalToBigInteger(value), sourcePrecision, sourceScale, resultPrecision, resultScale);
        }
    }

    private static PrestoException throwCastException(long value, long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        return new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DECIMAL '%s' to DECIMAL(%d, %d)",
                Decimals.toString(value, (int) sourceScale),
                resultPrecision, resultScale));
    }

    private static PrestoException throwCastException(BigInteger value, long sourcePrecision, long sourceScale, long resultPrecision, long resultScale)
    {
        return new PrestoException(INVALID_CAST_ARGUMENT, format("Cannot cast DECIMAL '%s' to DECIMAL(%d, %d)",
                Decimals.toString(value, (int) sourceScale),
                resultPrecision, resultScale));
    }
}
