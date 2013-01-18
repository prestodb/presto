package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.slice.Slice;

import com.facebook.presto.block.BlockCursor;

public class LongVarianceAggregation
    extends DoubleVarianceAggregation
{
    public static final LongVarianceAggregation VARIANCE_INSTANCE = new LongVarianceAggregation(false);
    public static final LongVarianceAggregation VARIANCE_POP_INSTANCE = new LongVarianceAggregation(true);

    LongVarianceAggregation(final boolean population)
    {
        super(population);
    }

    @Override
    public void addInput(final int positionCount, final Block block, final Slice valueSlice, final int valueOffset)
    {
        boolean hasValue = !VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);
        long count = hasValue ? VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0) : 0;
        double mean = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 1);
        double m2 = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

        final BlockCursor cursor = block.cursor();

        while (cursor.advanceNextPosition()) {
            if (cursor.isNull(0)) {
                continue;
            }

            // There is now at least one value present.
            hasValue = true;

            count++;
            final double x = cursor.getLong(0);
            final double delta = x - mean;
            mean += (delta / count);
            m2 += (delta * (x - mean));
        }

        if (hasValue) {
            VARIANCE_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 0);
            VARIANCE_CONTEXT_INFO.setLong(valueSlice, valueOffset, 0, count);
            VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 1, mean);
            VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, m2);
        }
    }

    @Override
    public void addInput(final BlockCursor cursor, final Slice valueSlice, final int valueOffset)
    {
        boolean hasValue = !VARIANCE_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);

        if (cursor.isNull(0)) {
            return;
        }

        long count = hasValue ? VARIANCE_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0) : 0;
        double mean = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 1);
        double m2 = VARIANCE_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

        count++;
        final double x = cursor.getLong(0);
        final double delta = x - mean;
        mean += (delta / count);
        m2 += (delta * (x - mean));

        if (!hasValue) {
            VARIANCE_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 0);
        }

        VARIANCE_CONTEXT_INFO.setLong(valueSlice, valueOffset, 0, count);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 1, mean);
        VARIANCE_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, m2);
    }
}
