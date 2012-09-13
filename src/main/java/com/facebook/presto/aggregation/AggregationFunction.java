package com.facebook.presto.aggregation;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;

public interface AggregationFunction
{
    TupleInfo getTupleInfo();

    void add(Cursor cursor, Range relevantRange);

    Tuple evaluate();
}
