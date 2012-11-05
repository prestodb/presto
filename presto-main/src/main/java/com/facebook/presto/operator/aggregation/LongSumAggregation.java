package com.facebook.presto.operator.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockCursor;

import javax.inject.Provider;

public class LongSumAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = provider(0, 0);

    public static Provider<AggregationFunction> provider(final int channelIndex, final int field)
    {
        return new Provider<AggregationFunction>()
        {
            @Override
            public LongSumAggregation get()
            {
                return new LongSumAggregation(channelIndex, field);
            }
        };
    }

    private final int channelIndex;
    private final int fieldIndex;
    private long sum;

    public LongSumAggregation(int channelIndex, int fieldIndex)
    {
        this.channelIndex = channelIndex;
        this.fieldIndex = fieldIndex;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public void add(BlockCursor cursor)
    {
        sum += cursor.getLong(fieldIndex);
    }

    @Override
    public void add(BlockCursor[] cursors)
    {
        sum += cursors[channelIndex].getLong(fieldIndex);
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(sum)
                .build();
    }
}
