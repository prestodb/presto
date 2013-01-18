package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;

import com.facebook.presto.slice.Slice;

public class DoubleStdDevAggregation
    extends DoubleVarianceAggregation
{
    public static final DoubleStdDevAggregation STDDEV_INSTANCE = new DoubleStdDevAggregation(false);
    public static final DoubleStdDevAggregation STDDEV_POP_INSTANCE = new DoubleStdDevAggregation(true);

    DoubleStdDevAggregation(final boolean population)
    {
        super(population);
    }

    static final Double buildFinalStdDev(final boolean population, final Slice valueSlice, final int valueOffset)
    {
        final Double variance = DoubleVarianceAggregation.buildFinalVariance(population, valueSlice, valueOffset);
        return (variance == null) ? null : Math.sqrt(variance);
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
