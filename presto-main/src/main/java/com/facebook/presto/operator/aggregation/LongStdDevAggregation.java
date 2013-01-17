package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;

public class LongStdDevAggregation
    extends AbstractStdDevAggregation
{
    public static final LongStdDevAggregation STDDEV_INSTANCE = new LongStdDevAggregation(false);
    public static final LongStdDevAggregation STDDEV_POP_INSTANCE = new LongStdDevAggregation(true);

    LongStdDevAggregation(final boolean population)
    {
        super(population);
    }

    @Override
    protected double getX(final BlockCursor cursor)
    {
        return (double) cursor.getLong(0);
    }
}

