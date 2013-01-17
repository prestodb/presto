package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;

public class LongVarianceAggregation
    extends AbstractVarianceAggregation
{
    public static final LongVarianceAggregation VARIANCE_INSTANCE = new LongVarianceAggregation(false);
    public static final LongVarianceAggregation VARIANCE_POP_INSTANCE = new LongVarianceAggregation(true);

    LongVarianceAggregation(final boolean population)
    {
        super(population);
    }

    @Override
    protected double getX(final BlockCursor cursor)
    {
        return (double) cursor.getLong(0);
    }
}
