package com.facebook.presto;

public interface BlockStream<T extends ValueBlock>
    extends Iterable<T>
{
    TupleInfo getTupleInfo();
    Cursor cursor();
}
