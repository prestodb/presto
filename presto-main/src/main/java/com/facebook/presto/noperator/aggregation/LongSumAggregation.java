package com.facebook.presto.noperator.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.BlockCursor;

import javax.inject.Provider;

public class LongSumAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = provider(0);

    public static Provider<AggregationFunction> provider(final int field)
    {
        return new Provider<AggregationFunction>()
        {
            @Override
            public LongSumAggregation get()
            {
                return new LongSumAggregation(field);
            }
        };
    }

    private final int field;
    private long sum;

    public LongSumAggregation(int field)
    {
        this.field = field;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public void add(BlockCursor cursor)
    {
        sum += cursor.getLong(field);
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(sum)
                .build();
    }
}
