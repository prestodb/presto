package com.facebook.presto.aggregations;

import com.facebook.presto.Cursor;
import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;

public interface AggregationFunction
{
    TupleInfo getTupleInfo();

    void add(Cursor cursor, Range relevantRange);

    Tuple evaluate();
}
