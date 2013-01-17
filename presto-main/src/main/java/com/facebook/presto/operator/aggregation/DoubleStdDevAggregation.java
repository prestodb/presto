package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;

public class DoubleStdDevAggregation
    extends AbstractStdDevAggregation
{
    public static final DoubleStdDevAggregation STDDEV_INSTANCE = new DoubleStdDevAggregation(false);
    public static final DoubleStdDevAggregation STDDEV_POP_INSTANCE = new DoubleStdDevAggregation(true);

    DoubleStdDevAggregation(final boolean population)
    {
        super(population);
    }

    @Override
    protected double getX(final BlockCursor cursor)
    {
        return (double) cursor.getDouble(0);
    }
}
