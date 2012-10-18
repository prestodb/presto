package com.facebook.presto.operation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;

public interface BinaryOperation
{
    TupleInfo getTupleInfo();
    Tuple evaluate(Cursor first, Cursor second);
    long evaluateAsLong(Cursor first, Cursor second);
}
