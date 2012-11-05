package com.facebook.presto.tuple;

import com.facebook.presto.slice.Slice;

public interface TupleReadable
{
    TupleInfo getTupleInfo();

    Tuple getTuple();

    long getLong(int index);

    double getDouble(int index);

    Slice getSlice(int index);
}
