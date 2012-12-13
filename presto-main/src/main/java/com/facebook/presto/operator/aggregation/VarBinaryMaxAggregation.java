package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.metadata.FunctionBinder;
import com.facebook.presto.operator.Page;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import javax.inject.Provider;
import java.util.List;

import static com.facebook.presto.tuple.Tuples.NULL_STRING_TUPLE;
import static com.facebook.presto.tuple.Tuples.createTuple;

public class VarBinaryMaxAggregation
    implements AggregationFunction
{
    public static Provider<AggregationFunction> varBinaryMaxAggregation(int channelIndex, int field)
    {
        return BINDER.bind(ImmutableList.of(new Input(channelIndex, field)));
    }

    public static final FunctionBinder BINDER = new FunctionBinder()
    {
        @Override
        public Provider<AggregationFunction> bind(final List<Input> arguments)
        {
            Preconditions.checkArgument(arguments.size() == 1, "max takes 1 parameter");

            return new Provider<AggregationFunction>()
            {
                @Override
                public VarBinaryMaxAggregation get()
                {
                    return new VarBinaryMaxAggregation(arguments.get(0).getChannel(), arguments.get(0).getField());
                }
            };
        }
    };

    private final int channelIndex;
    private final int fieldIndex;
    private Slice max;

    public VarBinaryMaxAggregation(int channelIndex, int fieldIndex)
    {
        this.channelIndex = channelIndex;
        this.fieldIndex = fieldIndex;
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return TupleInfo.SINGLE_VARBINARY;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return TupleInfo.SINGLE_VARBINARY;
    }

    @Override
    public void addInput(Page page)
    {
        BlockCursor cursor = page.getBlock(channelIndex).cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(fieldIndex)) {
                Slice slice = cursor.getSlice(fieldIndex);
                max = (max == null) ? slice : Ordering.natural().max(max, slice);
            }
        }
    }

    @Override
    public void addInput(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            Slice slice = cursor.getSlice(fieldIndex);
            max = (max == null) ? slice : Ordering.natural().max(max, slice);
        }
    }

    @Override
    public void addIntermediate(Page page)
    {
        BlockCursor cursor = page.getBlock(channelIndex).cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(fieldIndex)) {
                Slice slice = cursor.getSlice(fieldIndex);
                max = (max == null) ? slice : Ordering.natural().max(max, slice);
            }
        }
    }

    @Override
    public void addIntermediate(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            Slice slice = cursor.getSlice(fieldIndex);
            max = (max == null) ? slice : Ordering.natural().max(max, slice);
        }
    }

    @Override
    public Tuple evaluateIntermediate()
    {
        if (max == null) {
            return NULL_STRING_TUPLE;
        }
        return createTuple(max);
    }

    @Override
    public Tuple evaluateFinal()
    {
        if (max == null) {
            return NULL_STRING_TUPLE;
        }
        return createTuple(max);
    }
}
