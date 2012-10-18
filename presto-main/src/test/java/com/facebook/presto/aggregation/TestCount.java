package com.facebook.presto.aggregation;

import com.facebook.presto.block.Cursor;

public class TestCount
    extends TestAggregationFunction
{
    @Override
    public Cursor getSequenceCursor(long max)
    {
        return new LongSequenceCursor(10);
    }

    @Override
    public AggregationFunction getFunction()
    {
        return new CountAggregation();
    }

    @Override
    public Number getExpectedValue(long start, long end)
    {
        return end - start + 1;
    }

    @Override
    public Number getActualValue(AggregationFunction function)
    {
        return function.evaluate().getLong(0);
    }
}
