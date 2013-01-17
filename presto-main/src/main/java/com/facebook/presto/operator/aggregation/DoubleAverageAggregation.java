package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

public class DoubleAverageAggregation
        implements FixedWidthAggregationFunction
{
    public static final DoubleAverageAggregation DOUBLE_AVERAGE = new DoubleAverageAggregation();

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
        // todo remove this assumption that the field is 0
        if (cursor.isNull(0)) {
            return;
        }

        // mark value not null
        TUPLE_INFO.setNotNull(valueSlice, valueOffset, 0);

        // increment count
        TUPLE_INFO.setLong(valueSlice, valueOffset, 0, TUPLE_INFO.getLong(valueSlice, valueOffset, 0) + 1);

        // add value to sum
        // todo remove this assumption that the field is 0
        double newValue = cursor.getDouble(0);
        TUPLE_INFO.setDouble(valueSlice, valueOffset, 1, TUPLE_INFO.getDouble(valueSlice, valueOffset, 1) + newValue);
    }

    @Override
    public void addInput(int positionCount, Block block, Slice valueSlice, int valueOffset)
    {
        // initialize with current value
        boolean hasNonNull = !TUPLE_INFO.isNull(valueSlice, valueOffset);
        long count = TUPLE_INFO.getLong(valueSlice, valueOffset, 0);
        double sum = TUPLE_INFO.getDouble(valueSlice, valueOffset, 1);

        // process block
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            // todo remove this assumption that the field is 0
            if (!cursor.isNull(0)) {
                hasNonNull = true;
                count++;
                // todo remove this assumption that the field is 0
                sum += cursor.getDouble(0);
            }
        }

        // write new value
        if (hasNonNull) {
            TUPLE_INFO.setNotNull(valueSlice, valueOffset, 0);
            TUPLE_INFO.setLong(valueSlice, valueOffset, 0, count);
            TUPLE_INFO.setDouble(valueSlice, valueOffset, 1, sum);
        }
    }

    @Override
    public void addIntermediate(BlockCursor cursor, Slice valueSlice, int valueOffset)
    {
        // todo remove this assumption that the field is 0
        if (cursor.isNull(0)) {
            return;
        }

        // mark value not null
        TUPLE_INFO.setNotNull(valueSlice, valueOffset, 0);

        // decode value
        // todo remove this assumption that the field is 0
        Slice value = cursor.getSlice(0);
        long count = TUPLE_INFO.getLong(value, 0);
        double sum = TUPLE_INFO.getDouble(value, 1);

        // add counts
        TUPLE_INFO.setLong(valueSlice, valueOffset, 0, TUPLE_INFO.getLong(valueSlice, valueOffset, 0) + count);

        // add sums
        TUPLE_INFO.setDouble(valueSlice, valueOffset, 1, TUPLE_INFO.getDouble(valueSlice, valueOffset, 1) + sum);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (!TUPLE_INFO.isNull(valueSlice, valueOffset, 0)) {
            Slice value = Slices.allocate(TUPLE_INFO.getFixedSize());
            TUPLE_INFO.setLong(value, 0, TUPLE_INFO.getLong(valueSlice, valueOffset, 0));
            TUPLE_INFO.setDouble(value, 1, TUPLE_INFO.getDouble(valueSlice, valueOffset, 1));
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
