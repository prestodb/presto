package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;

public interface AggregationFunction
{
    TupleInfo getTupleInfo();

    void add(Page page);

    void add(BlockCursor... cursors);

    Tuple evaluate();
}
