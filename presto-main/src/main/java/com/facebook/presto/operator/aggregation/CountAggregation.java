package com.facebook.presto.operator.aggregation;

import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.BlockCursor;

import javax.inject.Provider;

import static com.facebook.presto.tuple.Tuples.createTuple;

public class CountAggregation
        implements FullAggregationFunction
{
    public static Provider<CountAggregation> countAggregation(final int channelIndex, final int field)
    {
        return new Provider<CountAggregation>()
        {
            @Override
            public CountAggregation get()
            {
                return new CountAggregation(channelIndex, field);
            }
        };
    }

    private final int channelIndex;
    private final int fieldIndex;
    private long count;

    public CountAggregation(int channelIndex, int fieldIndex)
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
    public void addInput(Page page)
    {
        count += page.getPositionCount();
    }

    @Override
    public void addInput(BlockCursor... cursors)
    {
        count++;
    }

    @Override
    public void addIntermediate(Page page)
    {
        BlockCursor cursor = page.getBlock(channelIndex).cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(fieldIndex)) {
                count += cursor.getLong(fieldIndex);
            }
        }
    }

    @Override
    public void addIntermediate(BlockCursor... cursors)
    {
        count += cursors[channelIndex].getLong(fieldIndex);
    }

    @Override
    public Tuple evaluateIntermediate()
    {
        return createTuple(count);
    }

    @Override
    public Tuple evaluateFinal()
    {
        return createTuple(count);
    }
}
