package com.facebook.presto.aggregations;

import com.facebook.presto.PositionBlock;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.google.common.base.Optional;

public class CountAggregation
        implements AggregationFunction
{
    private long count;

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
            count += filtered.get().getCount();
        }
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(count)
                .build();
    }
}
