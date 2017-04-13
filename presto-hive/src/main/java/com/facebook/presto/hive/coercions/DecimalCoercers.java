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
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slice;

import java.util.function.Function;

import static com.facebook.presto.spi.type.Decimals.longTenToNth;
import static com.facebook.presto.spi.type.Decimals.overflows;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.compareAbsolute;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToUnscaledLongUnsafe;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.intBitsToFloat;
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

    public static Function<Block, Block> createDecimalToDoubleCoercer(DecimalType fromType)
    {
        if (fromType.isShort()) {
            return new ShortDecimalToDoubleCoercer(fromType);
        }
        else {
            return new LongDecimalToDoubleCoercer(fromType);
        }
    }

    private static class ShortDecimalToDoubleCoercer
            extends AbstractCoercer<DecimalType, DoubleType>
    {
        private final double scalingFactor;

        public ShortDecimalToDoubleCoercer(DecimalType fromType)
        {
            super(fromType, DOUBLE);
            this.scalingFactor = (double) longTenToNth(Math.abs(fromType.getScale()));
        }

        @Override
        protected void appendCoercedLong(BlockBuilder blockBuilder, long value)
        {
            toType.writeDouble(blockBuilder, value / scalingFactor);
        }
    }

    /**
     * Powers of 10 which can be represented exactly in double.
     */
    private static final double[] DOUBLE_10_POW = {
            1.0e0, 1.0e1, 1.0e2, 1.0e3, 1.0e4, 1.0e5,
            1.0e6, 1.0e7, 1.0e8, 1.0e9, 1.0e10, 1.0e11,
            1.0e12, 1.0e13, 1.0e14, 1.0e15, 1.0e16, 1.0e17,
            1.0e18, 1.0e19, 1.0e20, 1.0e21, 1.0e22
    };
    private static final Slice MAX_EXACT_DOUBLE = unscaledDecimal((1L << 52) - 1);

    private static class LongDecimalToDoubleCoercer
            extends AbstractCoercer<DecimalType, DoubleType>
    {
        public LongDecimalToDoubleCoercer(DecimalType fromType)
        {
            super(fromType, DOUBLE);
        }

        @Override
        protected void appendCoercedSlice(BlockBuilder blockBuilder, Slice value)
        {
            double coercedValue;

            // If both decimal and scale can be represented exactly in double then compute rescaled and rounded result directly in double.
            if (fromType.getScale() < DOUBLE_10_POW.length && compareAbsolute(value, MAX_EXACT_DOUBLE) <= 0) {
                coercedValue = unscaledDecimalToUnscaledLongUnsafe(value) / DOUBLE_10_POW[fromType.getScale()];
            }
            else {
                // TODO: optimize and convert directly to double in similar fashion as in double to decimal casts
                coercedValue = parseDouble(Decimals.toString(value, fromType.getScale()));
            }
            toType.writeDouble(blockBuilder, coercedValue);
        }
    }

    public static Function<Block, Block> createRealToDecimalCoercer(DecimalType toType)
    {
        if (toType.isShort()) {
            return new RealToShortDecimalCoercer(toType);
        }
        else {
            return new RealToLongDecimalCoercer(toType);
        }
    }

    private static class RealToShortDecimalCoercer
            extends AbstractCoercer<RealType, DecimalType>
    {
        public RealToShortDecimalCoercer(DecimalType toType)
        {
            super(REAL, toType);
        }

        @Override
        protected void appendCoercedLong(BlockBuilder blockBuilder, long value)
        {
            Long shortDecimal = doubleToShortDecimal(intBitsToFloat((int) value), toType.getPrecision(), toType.getScale());
            if (shortDecimal == null) {
                blockBuilder.appendNull();
            }
            else {
                toType.writeLong(blockBuilder, shortDecimal);
            }
        }
    }

    private static class RealToLongDecimalCoercer
            extends AbstractCoercer<RealType, DecimalType>
    {
        public RealToLongDecimalCoercer(DecimalType toType)
        {
            super(REAL, toType);
        }

        @Override
        protected void appendCoercedLong(BlockBuilder blockBuilder, long value)
        {
            Slice decimal = doubleToLongDecimal(intBitsToFloat((int) value), toType.getPrecision(), toType.getScale());
            if (decimal == null) {
                blockBuilder.appendNull();
            }
            else {
                toType.writeSlice(blockBuilder, decimal);
            }
        }
    }

    public static Function<Block, Block> createDoubleToDecimalCoercer(DecimalType toType)
    {
        if (toType.isShort()) {
            return new DoubleToShortDecimalCoercer(toType);
        }
        else {
            return new DoubleToLongDecimalCoercer(toType);
        }
    }

    private static class DoubleToShortDecimalCoercer
            extends AbstractCoercer<DoubleType, DecimalType>
    {
        public DoubleToShortDecimalCoercer(DecimalType toType)
        {
            super(DOUBLE, toType);
        }

        @Override
        protected void appendCoercedDouble(BlockBuilder blockBuilder, double value)
        {
            Long shortDecimal = doubleToShortDecimal(value, toType.getPrecision(), toType.getScale());
            if (shortDecimal == null) {
                blockBuilder.appendNull();
            }
            else {
                toType.writeLong(blockBuilder, shortDecimal);
            }
        }
    }

    private static class DoubleToLongDecimalCoercer
            extends AbstractCoercer<DoubleType, DecimalType>
    {
        public DoubleToLongDecimalCoercer(DecimalType toType)
        {
            super(DOUBLE, toType);
        }

        @Override
        protected void appendCoercedDouble(BlockBuilder blockBuilder, double value)
        {
            Slice decimal = doubleToLongDecimal(value, toType.getPrecision(), toType.getScale());
            if (decimal == null) {
                blockBuilder.appendNull();
            }
            else {
                toType.writeSlice(blockBuilder, decimal);
            }
        }
    }

    private static Slice doubleToLongDecimal(double value, long precision, long scale)
    {
        try {
            Slice decimal = UnscaledDecimal128Arithmetic.doubleToLongDecimal(value, precision, (int) scale);
            if (UnscaledDecimal128Arithmetic.overflows(decimal, (int) precision)) {
                return null;
            }
            return decimal;
        }
        catch (ArithmeticException e) {
            return null;
        }
    }

    private static Long doubleToShortDecimal(double value, long precision, long scale)
    {
        Slice longDecimal = doubleToLongDecimal(value, precision, scale);
        if (longDecimal == null) {
            return null;
        }

        long low = UnscaledDecimal128Arithmetic.getLong(longDecimal, 0);
        long high = UnscaledDecimal128Arithmetic.getLong(longDecimal, 1);

        checkState(high == 0 && low >= 0, "Unexpected long decimal");
        long shortDecimal = low;
        if (UnscaledDecimal128Arithmetic.isNegative(longDecimal)) {
            shortDecimal = -shortDecimal;
        }
        return shortDecimal;
    }
}
