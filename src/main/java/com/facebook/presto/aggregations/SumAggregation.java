package com.facebook.presto.aggregations;

import com.facebook.presto.PositionBlock;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.google.common.base.Optional;

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
        Optional<ValueBlock> filtered = values.filter(relevantPositions);

        if (filtered.isPresent()) {
            for (Tuple value : filtered.get()) {
                sum += value.getLong(0);
            }
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
