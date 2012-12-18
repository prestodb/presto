package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;

public class DoubleMinAggregation
        implements FixedWidthAggregationFunction
{
    public static final DoubleMinAggregation DOUBLE_MIN = new DoubleMinAggregation();

    @Override
    public int getFixedSize()
    {
        return SINGLE_DOUBLE.getFixedSize();
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_DOUBLE;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_DOUBLE;
    }

    @Override
    public void initialize(Slice valueSlice, int valueOffset)
    {
        // mark value null
        SINGLE_DOUBLE.setNull(valueSlice, valueOffset, 0);
        SINGLE_DOUBLE.setDouble(valueSlice, valueOffset, 0, Double.POSITIVE_INFINITY);
    }

    @Override
    public void addInput(BlockCursor cursor, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(0)) {
            return;
        }

        // mark value not null
        SINGLE_DOUBLE.setNotNull(valueSlice, valueOffset, 0);

        // update current value
        double currentValue = SINGLE_DOUBLE.getDouble(valueSlice, valueOffset, 0);
        double newValue = cursor.getDouble(0);
        SINGLE_DOUBLE.setDouble(valueSlice, valueOffset, 0, Math.min(currentValue, newValue));
    }

    @Override
    public void addInput(int positionCount, Block block, Slice valueSlice, int valueOffset)
    {
        // initialize
        boolean hasNonNull = !SINGLE_DOUBLE.isNull(valueSlice, valueOffset);
        double min = SINGLE_DOUBLE.getDouble(valueSlice, valueOffset, 0);

        // process block
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(0)) {
                hasNonNull = true;
                min = Math.min(min, cursor.getDouble(0));
            }
        }

        // write new value
        if (hasNonNull) {
            SINGLE_DOUBLE.setNotNull(valueSlice, valueOffset, 0);
            SINGLE_DOUBLE.setDouble(valueSlice, valueOffset, 0, min);
        }
    }

    @Override
    public void addIntermediate(BlockCursor cursor, Slice valueSlice, int valueOffset)
    {
        addInput(cursor, valueSlice, valueOffset);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        evaluateFinal(valueSlice, valueOffset, output);
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (!SINGLE_DOUBLE.isNull(valueSlice, valueOffset, 0)) {
            double currentValue = SINGLE_DOUBLE.getDouble(valueSlice, valueOffset, 0);
            output.append(currentValue);
        } else {
            output.appendNull();
        }
    }
}
