package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;

import javax.annotation.Nullable;

public interface VariableWidthAggregationFunction<T>
        extends AggregationFunction
{
    /**
     * @return the initial value for the aggregation
     */
    T initialize();

    /**
     * Add all of the values in the specified block to the aggregation.
     * @param positionCount number of positions in this page
     * @param block the block containing values for the aggregation; null for no-arg aggregations
     * @param field
     */
    T addInput(int positionCount, @Nullable Block block, int field, T currentValue);

    /**
     * Add the current value of the specified cursor to the aggregation.
     * @param cursor the value to add to the aggregation; null for no-arg aggregations
     * @param field
     */
    T addInput(@Nullable BlockCursor cursor, int field, T currentValue);

    /**
     * Add the intermediate value at specified cursor to the aggregation.
     * The intermediate value is a value produced by the <code>evaluateIntermediate</code> function.
     * @param cursor the value to add to the aggregation; null for no-arg aggregations
     * @param field
     */
    T addIntermediate(BlockCursor cursor, int field, T currentValue);

    /**
     * Converts the current value to an intermediate value and adds it to the specified output.
     */
    void evaluateIntermediate(T currentValue, BlockBuilder output);

    /**
     * Converts the current value to a final value and adds it to the specified output.
     */
    void evaluateFinal(T currentValue, BlockBuilder output);

    long estimateSizeInBytes(T value);
}
