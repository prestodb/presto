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
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slice;

import java.util.function.Function;

import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static com.facebook.presto.spi.type.Decimals.overflows;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLongUnsafe;

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
}
