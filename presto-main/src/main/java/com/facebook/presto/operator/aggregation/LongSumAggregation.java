package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;

import javax.inject.Provider;

import static com.facebook.presto.tuple.Tuples.NULL_LONG_TUPLE;

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
    private boolean hasNonNullValue;
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
    public void add(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            hasNonNullValue = true;
            sum += cursor.getLong(fieldIndex);
        }
    }

    @Override
    public Tuple evaluate()
    {
        if (!hasNonNullValue) {
            return NULL_LONG_TUPLE;
        }
        return getTupleInfo().builder()
                .append(sum)
                .build();
    }
}
