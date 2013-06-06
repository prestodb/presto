package com.facebook.presto.tuple;

import io.airlift.slice.Slice;

public interface TupleReadable
{
    TupleInfo getTupleInfo();

    Tuple getTuple();

    boolean getBoolean(int index);

    long getLong(int index);

    double getDouble(int index);

    Slice getSlice(int index);

    boolean isNull(int field);
}
