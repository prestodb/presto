package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.sql.tree.Input;

import javax.annotation.Nullable;
import java.util.List;

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
     * @param blocks the blocks containing values for the aggregation; empty for no-arg aggregations
     * @param fields
     */
    T addInput(int positionCount, Block[] blocks, int[] fields, T currentValue);

    /**
     * Add the current value of the specified cursor to the aggregation.
     * @param cursors the values to add to the aggregation; empty for no-arg aggregations
     * @param fields
     */
    T addInput(BlockCursor[] cursors, int[] fields, T currentValue);

    /**
     * Add the intermediate value at specified cursor to the aggregation.
     * The intermediate value is a value produced by the <code>evaluateIntermediate</code> function.
     * @param cursors the values to add to the aggregation; empty for no-arg aggregations
     * @param fields
     */
    T addIntermediate(BlockCursor[] cursors, int[] fields, T currentValue);

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
