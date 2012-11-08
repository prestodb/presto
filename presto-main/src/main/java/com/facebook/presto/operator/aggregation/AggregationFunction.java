package com.facebook.presto.operator.aggregation;

import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.BlockCursor;

public interface AggregationFunction
{
    TupleInfo getTupleInfo();

    Tuple evaluate();

    void add(BlockCursor... cursors);
}
