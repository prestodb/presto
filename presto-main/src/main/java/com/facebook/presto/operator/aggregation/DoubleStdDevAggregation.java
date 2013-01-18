package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;

import com.facebook.presto.slice.Slice;

public class DoubleStdDevAggregation
        extends DoubleVarianceAggregation
{
    public static final DoubleStdDevAggregation STDDEV_INSTANCE = new DoubleStdDevAggregation(false);
    public static final DoubleStdDevAggregation STDDEV_POP_INSTANCE = new DoubleStdDevAggregation(true);

    DoubleStdDevAggregation(boolean population)
    {
        super(population);
    }

    static final Double buildFinalStdDev(boolean population, Slice valueSlice, int valueOffset)
    {
        Double variance = DoubleVarianceAggregation.buildFinalVariance(population, valueSlice, valueOffset);
        return (variance == null) ? null : Math.sqrt(variance);
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        Double result = DoubleStdDevAggregation.buildFinalStdDev(population, valueSlice, valueOffset);

        if (result == null) {
            output.appendNull();
        }
        else {
            output.append(result.doubleValue());
        }
    }
}
