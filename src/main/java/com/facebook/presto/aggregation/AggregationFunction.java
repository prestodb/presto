package com.facebook.presto.aggregation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;

public interface AggregationFunction
{
    TupleInfo getTupleInfo();

    void add(Cursor cursor, long endPosition);

    Tuple evaluate();

    void addCurrentPosition(Cursor cursor);
}
