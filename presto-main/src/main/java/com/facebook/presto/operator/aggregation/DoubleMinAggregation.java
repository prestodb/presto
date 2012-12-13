package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.metadata.FunctionBinder;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.inject.Provider;
import java.util.List;

import static com.facebook.presto.tuple.Tuples.NULL_DOUBLE_TUPLE;
import static com.facebook.presto.tuple.Tuples.createTuple;

public class DoubleMinAggregation
    implements AggregationFunction
{
    public static Provider<AggregationFunction> doubleMinAggregation(int channelIndex, int field)
    {
        return BINDER.bind(ImmutableList.of(new Input(channelIndex, field)));
    }

    public static final FunctionBinder BINDER = new FunctionBinder()
    {
        @Override
        public Provider<AggregationFunction> bind(final List<Input> arguments)
        {
            Preconditions.checkArgument(arguments.size() == 1, "min takes 1 parameter");

            return new Provider<AggregationFunction>()
            {
                @Override
                public DoubleMinAggregation get()
                {
                    return new DoubleMinAggregation(arguments.get(0).getChannel(), arguments.get(0).getField());
                }
            };
        }
    };

    private final int channelIndex;
    private final int fieldIndex;
    private boolean hasNonNullValue;
    private double min;

    public DoubleMinAggregation(int channelIndex, int fieldIndex)
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
    public void addInput(Page page)
    {
        BlockCursor cursor = page.getBlock(channelIndex).cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(fieldIndex)) {
                hasNonNullValue = true;
                min = Math.min(cursor.getDouble(fieldIndex), min);
            }
        }
    }

    @Override
    public void addInput(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            hasNonNullValue = true;
            min = Math.min(cursor.getDouble(fieldIndex), min);
        }
    }

    @Override
    public void addIntermediate(Page page)
    {
        BlockCursor cursor = page.getBlock(channelIndex).cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(fieldIndex)) {
                hasNonNullValue = true;
                min = Math.min(cursor.getDouble(fieldIndex), min);
            }
        }
    }

    @Override
    public void addIntermediate(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            hasNonNullValue = true;
            min = Math.min(cursor.getDouble(fieldIndex), min);
        }
    }

    @Override
    public Tuple evaluateIntermediate()
    {
        if (!hasNonNullValue) {
            return NULL_DOUBLE_TUPLE;
        }
        return createTuple(min);
    }

    @Override
    public Tuple evaluateFinal()
    {
        if (!hasNonNullValue) {
            return NULL_DOUBLE_TUPLE;
        }
        return createTuple(min);
    }
}
