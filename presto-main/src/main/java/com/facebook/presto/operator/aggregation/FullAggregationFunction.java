package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;

public interface FullAggregationFunction
{
    TupleInfo getFinalTupleInfo();

    TupleInfo getIntermediateTupleInfo();

    void addInput(BlockCursor... cursors);

    void addIntermediate(BlockCursor... cursors);

    Tuple evaluateIntermediate();

    Tuple evaluateFinal();
}
