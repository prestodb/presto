package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;

import javax.inject.Provider;

import static com.facebook.presto.tuple.Tuples.NULL_DOUBLE_TUPLE;
import static com.facebook.presto.tuple.Tuples.createTuple;

public class DoubleSumAggregation
        implements FullAggregationFunction
{
    public static Provider<DoubleSumAggregation> doubleSumAggregation(final int channelIndex, final int field)
    {
        return new Provider<DoubleSumAggregation>()
        {
            @Override
            public DoubleSumAggregation get()
            {
                return new DoubleSumAggregation(channelIndex, field);
            }
        };
    }

    private final int channelIndex;
    private final int fieldIndex;
    private boolean hasNonNullValue;
    private double sum;

    public DoubleSumAggregation(int channelIndex, int fieldIndex)
    {
        this.channelIndex = channelIndex;
        this.fieldIndex = fieldIndex;
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return TupleInfo.SINGLE_DOUBLE;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return TupleInfo.SINGLE_DOUBLE;
    }

    @Override
    public void addInput(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            hasNonNullValue = true;
            sum += cursor.getDouble(fieldIndex);
        }
    }

    @Override
    public void addIntermediate(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            hasNonNullValue = true;
            sum += cursor.getDouble(fieldIndex);
        }
    }

    @Override
    public Tuple evaluateIntermediate()
    {
        if (!hasNonNullValue) {
            return NULL_DOUBLE_TUPLE;
        }
        return createTuple(sum);
    }

    @Override
    public Tuple evaluateFinal()
    {
        if (!hasNonNullValue) {
            return NULL_DOUBLE_TUPLE;
        }
        return createTuple(sum);
    }
}
