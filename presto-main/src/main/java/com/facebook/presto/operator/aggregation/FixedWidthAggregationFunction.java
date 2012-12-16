package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;

public interface FixedWidthAggregationFunction
{
    TupleInfo getFinalTupleInfo();

    TupleInfo getIntermediateTupleInfo();

    void initialize(Slice valueSlice, int valueOffset);

    void addInput(BlockCursor cursor, Slice valueSlice, int valueOffset);

    void addIntermediate(BlockCursor cursor, Slice valueSlice, int valueOffset);

    void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output);

    void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output);
}
