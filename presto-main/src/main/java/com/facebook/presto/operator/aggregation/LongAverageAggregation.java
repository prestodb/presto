package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.ByteArraySlice;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;

import javax.inject.Provider;

import static com.facebook.presto.slice.SizeOf.SIZE_OF_DOUBLE;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.Tuples.NULL_DOUBLE_TUPLE;
import static com.facebook.presto.tuple.Tuples.NULL_STRING_TUPLE;
import static com.facebook.presto.tuple.Tuples.createTuple;

public class LongAverageAggregation
        implements FullAggregationFunction
{
    public static Provider<LongAverageAggregation> longAverageAggregation(final int channelIndex, final int field)
    {
        return new Provider<LongAverageAggregation>()
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
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_DOUBLE;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_VARBINARY;
    }

    @Override
    public void addInput(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            sum += cursor.getLong(fieldIndex);
            count++;
        }
    }

    @Override
    public void addIntermediate(BlockCursor... cursors)
    {
        BlockCursor cursor = cursors[channelIndex];
        if (!cursor.isNull(fieldIndex)) {
            Slice data = cursor.getSlice(fieldIndex);
            sum += data.getDouble(0);
            count += data.getLong(SIZE_OF_DOUBLE);
        }
    }

    @Override
    public Tuple evaluateIntermediate()
    {
        if (count == 0) {
            return NULL_STRING_TUPLE;
        }
        ByteArraySlice data = Slices.allocate(SIZE_OF_DOUBLE + SIZE_OF_LONG);
        data.setDouble(0, sum);
        data.setLong(SIZE_OF_DOUBLE, count);
        return SINGLE_VARBINARY.builder()
                .append(data)
                .build();
    }

    @Override
    public Tuple evaluateFinal()
    {
        if (count == 0) {
            return NULL_DOUBLE_TUPLE;
        }
        double value = sum / count;
        return createTuple(value);
    }
}
