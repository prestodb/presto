package com.facebook.presto.noperator.aggregation;


import com.facebook.presto.nblock.BlockCursor;

public class TestSum
    extends TestAggregationFunction
{
    @Override
    public BlockCursor getSequenceCursor(long max)
    {
        return new LongSequenceCursor(max);
    }

    @Override
    public LongSumAggregation getFunction()
    {
        return new LongSumAggregation(0, 0);
    }

    @Override
    public Long getExpectedValue(long positions)
    {
        long sum = 0;
        for (long i = 0; i < positions; i++) {
            sum += i;
        }
        return sum;
    }

    @Override
    public Long getActualValue(AggregationFunction function)
    {
        return function.evaluate().getLong(0);
    }
}
