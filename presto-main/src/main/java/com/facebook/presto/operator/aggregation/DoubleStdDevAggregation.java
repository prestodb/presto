package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;
import io.airlift.slice.Slice;

public class DoubleStdDevAggregation
        extends DoubleVarianceAggregation
{
    public static final DoubleStdDevAggregation STDDEV_INSTANCE = new DoubleStdDevAggregation(false);
    public static final DoubleStdDevAggregation STDDEV_POP_INSTANCE = new DoubleStdDevAggregation(true);

    DoubleStdDevAggregation(boolean population)
    {
        super(population);
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        Double result = AbstractVarianceAggregation.buildFinalStdDev(population, valueSlice, valueOffset);

        if (result == null) {
            output.appendNull();
        }
        else {
            output.append(result.doubleValue());
        }
    }
}
