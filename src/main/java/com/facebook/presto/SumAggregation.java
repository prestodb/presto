package com.facebook.presto;

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;

public class SumAggregation
        implements AggregationFunction
{
    private long sum;

    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(SIZE_OF_LONG);
    }

    @Override
    public void add(ValueBlock values, PositionBlock relevantPositions)
    {
        for (Tuple value : values.filter(relevantPositions)) {
            sum += value.getLong(0);
        }
    }

    @Override
    public Tuple evaluate()
    {
        Slice slice = Slices.allocate(SIZE_OF_LONG);
        slice.setLong(0, sum);
        return new Tuple(slice, getTupleInfo());
    }
}
