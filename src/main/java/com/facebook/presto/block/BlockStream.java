package com.facebook.presto.block;

import com.facebook.presto.TupleInfo;

public interface BlockStream
{
    TupleInfo getTupleInfo();
    Cursor cursor();
}
