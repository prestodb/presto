package com.facebook.presto.noperator.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.BlockCursor;

import javax.inject.Provider;

public class LongAverageAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = provider(0, 0);

    public static Provider<AggregationFunction> provider(final int channelIndex, final int field)
    {
        return new Provider<AggregationFunction>()
        {
            @Override
            public LongAverageAggregation get()
            {
                return new LongAverageAggregation(channelIndex, field);
            }
        };
    }

    private final int channelIndex;
    private final int fieldIndex;
    private double sum;
    private long count;

    public LongAverageAggregation(int channelIndex, int fieldIndex)
    {
        this.channelIndex = channelIndex;
        this.fieldIndex = fieldIndex;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_DOUBLE;
    }

    @Override
    public void add(BlockCursor cursor)
    {
        sum += cursor.getDouble(fieldIndex);
        count++;
    }

    @Override
    public void add(BlockCursor[] cursors)
    {
        sum += cursors[channelIndex].getLong(fieldIndex);
        count++;
    }

    @Override
    public Tuple evaluate()
    {
        double value = sum / count;
        return getTupleInfo().builder()
                .append(value)
                .build();
    }
}
