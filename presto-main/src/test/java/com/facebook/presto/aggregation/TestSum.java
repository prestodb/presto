package com.facebook.presto.aggregation;

import com.facebook.presto.block.Cursor;

public class TestSum
    extends TestAggregationFunction
{
    @Override
    public LongSumAggregation getFunction()
    {
        return new LongSumAggregation(0);
    }

    @Override
    public Long getExpectedValue(long start, long end)
    {
        long sum = 0;
        for (long i = start; i <= end; i++) {
            sum += i;
        }
        return sum;
    }

    @Override
    public Long getActualValue(AggregationFunction function)
    {
        return function.evaluate().getLong(0);
    }

    @Override
    public Cursor getSequenceCursor(long max)
    {
        return new LongSequenceCursor(max);
    }
}
