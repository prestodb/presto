package com.facebook.presto.operator.aggregation;

import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.BlockCursor;

import javax.inject.Provider;

public class DoubleAverageAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = provider(0, 0);

    public static Provider<AggregationFunction> provider(final int channelIndex, final int field)
    {
        return new Provider<AggregationFunction>()
        {
            @Override
            public DoubleAverageAggregation get()
            {
                return new DoubleAverageAggregation(channelIndex, field);
            }
        };
    }

    private final int channelIndex;
    private final int fieldIndex;
    private double sum;
    private long count;

    public DoubleAverageAggregation(int channelIndex, int fieldIndex)
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
    public void add(BlockCursor... cursors)
    {
        sum += cursors[channelIndex].getDouble(fieldIndex);
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
