package com.facebook.presto.noperator.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.BlockCursor;

public interface AggregationFunction
{
    TupleInfo getTupleInfo();

    void add(BlockCursor cursor, long endPosition);

    Tuple evaluate();
}
