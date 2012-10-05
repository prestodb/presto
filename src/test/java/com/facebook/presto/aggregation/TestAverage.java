package com.facebook.presto.aggregation;

import com.facebook.presto.block.Cursor;

public class TestAverage
    extends TestAggregationFunction
{
    @Override
    public AverageAggregation getFunction()
    {
        return new AverageAggregation();
    }

    @Override
    public Double getExpectedValue(long start, long end)
    {
        double sum = 0;
        for (long i = start; i <= end; i++) {
            sum += i;
        }
        return sum / (end - start + 1);
    }

    @Override
    public Double getActualValue(AggregationFunction function)
    {
        return function.evaluate().getDouble(0);
    }

    @Override
    public Cursor getSequenceCursor(long max)
    {
        return new DoubleSequenceCursor(max);
    }
}
