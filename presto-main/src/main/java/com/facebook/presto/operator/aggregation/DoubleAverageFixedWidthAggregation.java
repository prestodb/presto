package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class DoubleAverageFixedWidthAggregation
        implements FixedWidthAggregationFunction
{
    public static final DoubleAverageFixedWidthAggregation DOUBLE_AVERAGE = new DoubleAverageFixedWidthAggregation();

    private static final TupleInfo TUPLE_INFO = new TupleInfo(Type.FIXED_INT_64, Type.DOUBLE);

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return TupleInfo.SINGLE_DOUBLE;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return TUPLE_INFO;
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
        double newValue = cursor.getDouble(0);
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

        // add counts
        long count = cursor.getLong(0);
        TUPLE_INFO.setLong(valueSlice, valueOffset, 0, TUPLE_INFO.getLong(valueSlice, valueOffset) + count);

        // add sums
        double sum = cursor.getDouble(1);
        TUPLE_INFO.setDouble(valueSlice, valueOffset, 1, TUPLE_INFO.getDouble(valueSlice, valueOffset) + sum);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (!TUPLE_INFO.isNull(valueSlice, valueOffset, 0)) {
            output.append(TUPLE_INFO.getLong(valueSlice, valueOffset, 0));
            output.append(TUPLE_INFO.getDouble(valueSlice, valueOffset, 1));
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
