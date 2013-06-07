package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.Slice;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;

public class BooleanMaxAggregation
        implements FixedWidthAggregationFunction
{
    public static final BooleanMaxAggregation BOOLEAN_MAX = new BooleanMaxAggregation();

    @Override
    public int getFixedSize()
    {
        return SINGLE_BOOLEAN.getFixedSize();
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_BOOLEAN;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_BOOLEAN;
    }

    @Override
    public void initialize(Slice valueSlice, int valueOffset)
    {
        // mark value null
        SINGLE_BOOLEAN.setNull(valueSlice, valueOffset, 0);
        SINGLE_BOOLEAN.setBoolean(valueSlice, valueOffset, 0, Boolean.FALSE);
    }

    @Override
    public void addInput(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(field)) {
            return;
        }

        // mark value not null
        SINGLE_BOOLEAN.setNotNull(valueSlice, valueOffset, 0);

        // update current value
        boolean newValue = cursor.getBoolean(field);
        if (newValue == true) {
            SINGLE_BOOLEAN.setBoolean(valueSlice, valueOffset, 0, true);
        }
    }

    @Override
    public void addInput(int positionCount, Block block, int field, Slice valueSlice, int valueOffset)
    {
        // initialize
        boolean hasNonNull = !SINGLE_BOOLEAN.isNull(valueSlice, valueOffset);

        // if we already have TRUE for the max value, don't process the block
        if (hasNonNull && SINGLE_BOOLEAN.getBoolean(valueSlice, valueOffset, field) == true) {
            return;
        }

        boolean max = false;

        // process block
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            if (!cursor.isNull(field)) {
                hasNonNull = true;
                if (cursor.getBoolean(field) == true) {
                    max = true;
                    break;
                }
            }
        }

        // write new value
        if (hasNonNull) {
            SINGLE_BOOLEAN.setNotNull(valueSlice, valueOffset, 0);
            SINGLE_BOOLEAN.setBoolean(valueSlice, valueOffset, 0, max);
        }
    }

    @Override
    public void addIntermediate(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        addInput(cursor, field, valueSlice, valueOffset);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        evaluateFinal(valueSlice, valueOffset, output);
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (!SINGLE_BOOLEAN.isNull(valueSlice, valueOffset, 0)) {
            boolean currentValue = SINGLE_BOOLEAN.getBoolean(valueSlice, valueOffset, 0);
            output.append(currentValue);
        }
        else {
            output.appendNull();
        }
    }
}
