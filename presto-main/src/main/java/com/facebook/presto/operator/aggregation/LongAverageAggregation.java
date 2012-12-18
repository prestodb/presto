package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;

import static com.facebook.presto.slice.SizeOf.SIZE_OF_DOUBLE;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

public class LongAverageAggregation
        implements FixedWidthAggregationFunction
{
    public static final LongAverageAggregation LONG_AVERAGE = new LongAverageAggregation();

    private static final TupleInfo TUPLE_INFO = new TupleInfo(Type.FIXED_INT_64, Type.DOUBLE);

    @Override
    public int getFixedSize()
    {
        return TUPLE_INFO.getFixedSize();
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
    public void initialize(Slice valueSlice, int valueOffset)
    {
        // mark value null
        TUPLE_INFO.setNull(valueSlice, valueOffset, 0);
    }

    @Override
    public void addInput(BlockCursor cursor, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(0)) {
            return;
        }

        // mark value not null
        TUPLE_INFO.setNotNull(valueSlice, valueOffset, 0);

        // increment count
        TUPLE_INFO.setLong(valueSlice, valueOffset, 0, TUPLE_INFO.getLong(valueSlice, valueOffset, 0) + 1);

        // add value to sum
        long newValue = cursor.getLong(0);
        TUPLE_INFO.setDouble(valueSlice, valueOffset, 1, TUPLE_INFO.getDouble(valueSlice, valueOffset, 1) + newValue);
    }

    @Override
    public void addIntermediate(BlockCursor cursor, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(0)) {
            return;
        }

        // mark value not null
        TUPLE_INFO.setNotNull(valueSlice, valueOffset, 0);

        // decode value
        Slice value = cursor.getSlice(0);
        long count = value.getLong(0);
        double sum = value.getDouble(SIZE_OF_LONG);

        // add counts
        TUPLE_INFO.setLong(valueSlice, valueOffset, 0, TUPLE_INFO.getLong(valueSlice, valueOffset, 0) + count);

        // add sums
        TUPLE_INFO.setDouble(valueSlice, valueOffset, 1, TUPLE_INFO.getDouble(valueSlice, valueOffset, 1) + sum);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (!TUPLE_INFO.isNull(valueSlice, valueOffset, 0)) {
            Slice value = Slices.allocate(SIZE_OF_LONG + SIZE_OF_DOUBLE);
            value.setLong(0, TUPLE_INFO.getLong(valueSlice, valueOffset, 0));
            value.setDouble(SIZE_OF_LONG, TUPLE_INFO.getDouble(valueSlice, valueOffset, 1));
            output.append(value);
        } else {
            output.appendNull();
        }
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (!TUPLE_INFO.isNull(valueSlice, valueOffset, 0)) {
            long count = TUPLE_INFO.getLong(valueSlice, valueOffset, 0);
            double sum = TUPLE_INFO.getDouble(valueSlice, valueOffset, 1);
            output.append(sum / count);
        } else {
            output.appendNull();
        }
    }
}
