package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;

import javax.inject.Provider;

import static com.facebook.presto.tuple.Tuples.NULL_LONG_TUPLE;
import static com.facebook.presto.tuple.Tuples.createTuple;

public class LongSumAggregation
        implements FullAggregationFunction
{
    public static Provider<LongSumAggregation> longSumAggregation(final int channelIndex, final int field)
    {
        return new Provider<LongSumAggregation>()
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
    public TupleInfo getFinalTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public void addInput(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            hasNonNullValue = true;
            sum += cursor.getLong(fieldIndex);
        }
    }

    @Override
    public void addIntermediate(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            hasNonNullValue = true;
            sum += cursor.getLong(fieldIndex);
        }
    }

    @Override
    public Tuple evaluateIntermediate()
    {
        if (!hasNonNullValue) {
            return NULL_LONG_TUPLE;
        }
        return createTuple(sum);
    }

    @Override
    public Tuple evaluateFinal()
    {
        if (!hasNonNullValue) {
            return NULL_LONG_TUPLE;
        }
        return createTuple(sum);
    }
}
