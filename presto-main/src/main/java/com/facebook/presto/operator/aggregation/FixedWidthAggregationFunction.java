package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

public interface FixedWidthAggregationFunction
        extends AggregationFunction
{
    /**
     * @return the fixed width working size required by this aggregation
     */
    int getFixedSize();

    /**
     * Sets the initial value of the working value slice.
     */
    void initialize(Slice valueSlice, int valueOffset);

    /**
     * Add all of the values in the specified block to the aggregation.
     * @param positionCount number of positions in this page
     * @param block the block containing values for the aggregation; null for no-arg aggregations
     * @param field
     */
    void addInput(int positionCount, @Nullable Block block, int field, Slice valueSlice, int valueOffset);

    /**
     * Add the current value of the specified cursor to the aggregation.
     * @param cursor the value to add to the aggregation; null for no-arg aggregations
     * @param field
     */
    void addInput(@Nullable BlockCursor cursor, int field, Slice valueSlice, int valueOffset);

    /**
     * Add the intermediate value at specified cursor to the aggregation.
     * The intermediate value is a value produced by the <code>evaluateIntermediate</code> function.
     */
    void addIntermediate(BlockCursor cursor, int field, Slice valueSlice, int valueOffset);

    /**
     * Converts the current value to an intermediate value and adds it to the specified output.
     */
    void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output);

    /**
     * Converts the current value to a final value and adds it to the specified output.
     */
    void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output);
}
