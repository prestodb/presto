package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.FunctionBinder;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.BlockCursor;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.inject.Provider;

import java.util.List;

import static com.facebook.presto.tuple.Tuples.createTuple;

public class CountAggregation
        implements FullAggregationFunction
{
    public static Provider<FullAggregationFunction> countAggregation(final int channelIndex, final int field)
    {
        return BINDER.bind(ImmutableList.of(new Input(channelIndex, field)));
    }

    public static final FunctionBinder BINDER = new FunctionBinder()
    {
        @Override
        public Provider<FullAggregationFunction> bind(List<Input> arguments)
        {
            Preconditions.checkArgument(arguments.size() == 0 || arguments.size() == 1, "count takes 0 or 1 parameters");
            final Input input = Iterables.getFirst(arguments, new Input(-1, -1));

            return new Provider<FullAggregationFunction>()
            {
                @Override
                public CountAggregation get()
                {
                    return new CountAggregation(input.getChannel(), input.getField());
                }
            };
        }
    };

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
