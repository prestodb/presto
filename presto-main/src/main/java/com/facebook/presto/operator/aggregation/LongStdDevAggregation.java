package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.slice.Slice;

public class LongStdDevAggregation
    extends LongVarianceAggregation
{
    public static final LongStdDevAggregation STDDEV_INSTANCE = new LongStdDevAggregation(false);
    public static final LongStdDevAggregation STDDEV_POP_INSTANCE = new LongStdDevAggregation(true);

    LongStdDevAggregation(final boolean population)
    {
        super(population);
    }

    @Override
    public void evaluateFinal(final Slice valueSlice, final int valueOffset, final BlockBuilder output)
    {
        final Double result = DoubleStdDevAggregation.buildFinalStdDev(population, valueSlice, valueOffset);

        if (result == null) {
            output.appendNull();
        }
        else {
            output.append(result.doubleValue());
        }
    }
}

