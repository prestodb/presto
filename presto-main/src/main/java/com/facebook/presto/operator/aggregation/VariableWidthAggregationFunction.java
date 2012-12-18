package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;

public interface VariableWidthAggregationFunction<T>
        extends NewAggregationFunction
{
    T initialize();

    T addInput(BlockCursor cursor, T currentValue);

    T addIntermediate(BlockCursor cursor, T currentValue);

    void evaluateIntermediate(T currentValue, BlockBuilder output);

    void evaluateFinal(T currentValue, BlockBuilder output);
}
