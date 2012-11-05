package com.facebook.presto.operator.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockCursor;

public interface AggregationFunction
{
    TupleInfo getTupleInfo();

    void add(BlockCursor cursor);

    Tuple evaluate();

    void add(BlockCursor[] cursors);
}
