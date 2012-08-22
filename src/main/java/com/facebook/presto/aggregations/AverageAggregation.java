package com.facebook.presto.aggregations;

import com.facebook.presto.PositionBlock;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;

public class AverageAggregation
    implements AggregationFunction
{
    private long sum;
    private long count;

    @Override
    public TupleInfo getTupleInfo()
    {
        // TODO: value should be float (?)
        return new TupleInfo(TupleInfo.Type.FIXED_INT_64);
    }

    @Override
    public void add(ValueBlock values, PositionBlock relevantPositions)
    {
        // TODO: deal with overflow
        // TODO: optimize RLE blocks
        ValueBlock filtered = values.filter(relevantPositions);

        for (Tuple value : filtered) {
            sum += value.getLong(0);
            ++count;
        }
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(sum / count)
                .build();
    }
}
