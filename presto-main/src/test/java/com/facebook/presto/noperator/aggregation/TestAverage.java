package com.facebook.presto.noperator.aggregation;

import com.facebook.presto.nblock.BlockCursor;

public class TestAverage
    extends TestAggregationFunction
{
    @Override
    public BlockCursor getSequenceCursor(long max)
    {
        return new DoubleSequenceCursor(max);
    }

    @Override
    public AverageAggregation getFunction()
    {
        return new AverageAggregation(0, 0);
    }

    @Override
    public Double getExpectedValue(long positions)
    {
        double sum = 0;
        for (long i = 0; i < positions; i++) {
            sum += i;
        }
        return sum / positions;
    }

    @Override
    public Double getActualValue(AggregationFunction function)
    {
        return function.evaluate().getDouble(0);
    }
}
