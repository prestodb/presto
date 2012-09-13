package com.facebook.presto.block;

import com.facebook.presto.TupleInfo;

public interface BlockStream<T extends ValueBlock>
    extends Iterable<T>
{
    TupleInfo getTupleInfo();
    Cursor cursor();
}
