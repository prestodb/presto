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
package com.facebook.presto.hive.coercions;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slice;

import java.util.function.Function;

import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static com.facebook.presto.spi.type.Decimals.overflows;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.compareAbsolute;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLongUnsafe;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;

public class DecimalCoercers
{
    private DecimalCoercers()
    {
    }

    public static Function<Block, Block> createDecimalToDecimalCoercer(DecimalType fromType, DecimalType toType)
    {
        if (fromType.isShort() && toType.isShort()) {
            return new ShortDecimalToShortDecimalCoercer(fromType, toType);
        }
        else if (fromType.isShort() && !toType.isShort()) {
            return new ShortDecimalToLongDecimalCoercer(fromType, toType);
        }
        else if (!fromType.isShort() && toType.isShort()) {
            return new LongDecimalToShortDecimalCoercer(fromType, toType);
        }
        else {
            return new LongDecimalToLongDecimalCoercer(fromType, toType);
        }
    }

    private static class ShortDecimalToShortDecimalCoercer
            extends AbstractCoercer<DecimalType, DecimalType>
    {
        private final long scalingFactor;

        public ShortDecimalToShortDecimalCoercer(DecimalType fromType, DecimalType toType)
        {
            super(fromType, toType);
            this.scalingFactor = longTenToNth(Math.abs(toType.getScale() - fromType.getScale()));
        }

        @Override
        protected void appendCoercedLong(BlockBuilder blockBuilder, long value)
        {
            long returnValue;
            if (toType.getScale() >= fromType.getScale()) {
                returnValue = value * scalingFactor;
            }
            else {
                returnValue = value / scalingFactor;
                if (value >= 0) {
                    if (value % scalingFactor >= scalingFactor / 2) {
                        returnValue++;
                    }
                }
                else {
                    if (value % scalingFactor <= -(scalingFactor / 2)) {
                        returnValue--;
                    }
                }
            }
            if (overflows(returnValue, toType.getPrecision())) {
                blockBuilder.appendNull();
            }
            else {
                toType.writeLong(blockBuilder, returnValue);
            }
        }
    }

    private static class ShortDecimalToLongDecimalCoercer
            extends AbstractCoercer<DecimalType, DecimalType>
    {
        public ShortDecimalToLongDecimalCoercer(DecimalType fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void appendCoercedLong(BlockBuilder blockBuilder, long value)
        {
            Slice coercedValue = longToLongCast(unscaledDecimal(value), fromType.getScale(), toType.getPrecision(), toType.getScale());
            if (coercedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                toType.writeSlice(blockBuilder, coercedValue);
            }
        }
    }

    private static class LongDecimalToShortDecimalCoercer
            extends AbstractCoercer<DecimalType, DecimalType>
    {
        public LongDecimalToShortDecimalCoercer(DecimalType fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void appendCoercedSlice(BlockBuilder blockBuilder, Slice value)
        {
            Slice coercedValue = longToLongCast(value, fromType.getScale(), toType.getPrecision(), toType.getScale());
            if (coercedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                toType.writeLong(blockBuilder, unscaledDecimalToUnscaledLongUnsafe(coercedValue));
            }
        }
    }

    private static class LongDecimalToLongDecimalCoercer
            extends AbstractCoercer<DecimalType, DecimalType>
    {
        public LongDecimalToLongDecimalCoercer(DecimalType fromType, DecimalType toType)
        {
            super(fromType, toType);
        }

        @Override
        protected void appendCoercedSlice(BlockBuilder blockBuilder, Slice value)
        {
            Slice coercedValue = longToLongCast(value, fromType.getScale(), toType.getPrecision(), toType.getScale());
            if (coercedValue == null) {
                blockBuilder.appendNull();
            }
            else {
                toType.writeSlice(blockBuilder, coercedValue);
            }
        }
    }

    private static Slice longToLongCast(
            Slice value,
            long sourceScale,
            long resultPrecision,
            long resultScale)
    {
        try {
            Slice result = rescale(value, (int) (resultScale - sourceScale));
            if (UnscaledDecimal128Arithmetic.overflows(result, (int) resultPrecision)) {
                return null;
            }
            return result;
        }
        catch (ArithmeticException e) {
            return null;
        }
    }

    public static Function<Block, Block> createDecimalToRealCoercer(DecimalType fromType)
    {
        if (fromType.isShort()) {
            return new ShortDecimalToRealCoercer(fromType);
        }
        else {
            return new LongDecimalToRealCoercer(fromType);
        }
    }

    private static class ShortDecimalToRealCoercer
            extends AbstractCoercer<DecimalType, RealType>
    {
        private final double scalingFactor;

        public ShortDecimalToRealCoercer(DecimalType fromType)
        {
            super(fromType, REAL);
            this.scalingFactor = (double) longTenToNth(Math.abs(fromType.getScale()));
        }

        @Override
        protected void appendCoercedLong(BlockBuilder blockBuilder, long value)
        {
            double doubleValue = (double) value / scalingFactor;
            toType.writeLong(blockBuilder, floatToRawIntBits((float) doubleValue));
        }
    }

    /**
     * Powers of 10 which can be represented exactly in float.
     */
    private static final float[] FLOAT_10_POW = {
            1.0e0f, 1.0e1f, 1.0e2f, 1.0e3f, 1.0e4f, 1.0e5f,
            1.0e6f, 1.0e7f, 1.0e8f, 1.0e9f, 1.0e10f
    };
    private static final Slice MAX_EXACT_FLOAT = unscaledDecimal((1L << 22) - 1);

    private static class LongDecimalToRealCoercer
            extends AbstractCoercer<DecimalType, RealType>
    {
        public LongDecimalToRealCoercer(DecimalType fromType)
        {
            super(fromType, REAL);
        }

        @Override
        protected void appendCoercedSlice(BlockBuilder blockBuilder, Slice value)
        {
            float coercedValue;
            // If both decimal and scale can be represented exactly in float then compute rescaled and rounded result directly in float.
            if (fromType.getScale() < FLOAT_10_POW.length && compareAbsolute(value, MAX_EXACT_FLOAT) <= 0) {
                coercedValue = (float) unscaledDecimalToUnscaledLongUnsafe(value) / FLOAT_10_POW[fromType.getScale()];
            }
            else {
                // TODO: optimize and convert directly to float in similar fashion as in double to decimal casts
                coercedValue = parseFloat(Decimals.toString(value, fromType.getScale()));
            }
            toType.writeLong(blockBuilder, floatToRawIntBits(coercedValue));
        }
    }
}
