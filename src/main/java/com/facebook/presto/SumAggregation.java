package com.facebook.presto;

public class SumAggregation
    implements AggregationFunction
{
    private long sum;

    @Override
    public void add(ValueBlock values, PositionBlock relevantPositions)
    {
        for (Object value : values.filter(relevantPositions)) {
            sum += (Long) value;
        }
    }

    @Override
    public Object evaluate()
    {
        return sum;
    }
}
