package com.facebook.presto.noperator.aggregation;

import com.facebook.presto.nblock.BlockCursor;

public class TestCount
    extends TestAggregationFunction
{
    @Override
    public BlockCursor getSequenceCursor(long max)
    {
        return new LongSequenceCursor(max);
    }

    @Override
    public AggregationFunction getFunction()
    {
        return new CountAggregation();
    }

    @Override
    public Number getExpectedValue(long positions)
    {
        return positions;
    }

    @Override
    public Number getActualValue(AggregationFunction function)
    {
        return function.evaluate().getLong(0);
    }
}
