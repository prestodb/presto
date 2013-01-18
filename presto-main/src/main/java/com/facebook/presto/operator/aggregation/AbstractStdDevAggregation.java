package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.slice.Slice;

public abstract class AbstractStdDevAggregation
    extends AbstractVarianceAggregation
{
    AbstractStdDevAggregation(final boolean population)
    {
        super(population);
    }

    @Override
    protected Double buildFinal(final Slice valueSlice, final int valueOffset)
    {
        final Double variance = super.buildFinal(valueSlice, valueOffset);
        if (variance == null) {
            return null;
        }

        return Math.sqrt(variance);
    }
}
