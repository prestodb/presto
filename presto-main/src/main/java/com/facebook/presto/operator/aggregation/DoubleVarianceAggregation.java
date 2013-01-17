package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;

public class DoubleVarianceAggregation
    extends AbstractVarianceAggregation
{
    public static final DoubleVarianceAggregation VARIANCE_INSTANCE = new DoubleVarianceAggregation(false);
    public static final DoubleVarianceAggregation VARIANCE_POP_INSTANCE = new DoubleVarianceAggregation(true);

    DoubleVarianceAggregation(final boolean population)
    {
        super(population);
    }

    @Override
    protected double getX(final BlockCursor cursor)
    {
        return cursor.getDouble(0);
    }
}
