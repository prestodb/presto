package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;

public interface AggregationFunction
{
    TupleInfo getFinalTupleInfo();

    TupleInfo getIntermediateTupleInfo();

    void addInput(Page page);

    void addInput(BlockCursor... cursors);

    void addIntermediate(Page page);

    void addIntermediate(BlockCursor... cursors);

    Tuple evaluateIntermediate();

    Tuple evaluateFinal();
}
