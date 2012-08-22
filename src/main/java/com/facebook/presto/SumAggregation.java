package com.facebook.presto;

public class SumAggregation
        implements AggregationFunction
{
    private long sum;

    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(TupleInfo.Type.FIXED_INT_64);
    }

    @Override
    public void add(ValueBlock values, PositionBlock relevantPositions)
    {
        ValueBlock filtered = values.filter(relevantPositions);

        for (Tuple value : filtered) {
            sum += value.getLong(0);
        }
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(sum)
                .build();
    }
}
