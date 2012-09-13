package com.facebook.presto.block;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.operator.ValueCursor;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Iterator;

public class ValueBlockStream<T extends Block>
        implements BlockStream, Iterable<T>
{
    private final TupleInfo info;
    private final Iterable<T> source;

    public ValueBlockStream(TupleInfo info, T... source)
    {
        this(info, Arrays.asList(source));
    }

    public ValueBlockStream(TupleInfo info, Iterable<T> source)
    {
        Preconditions.checkNotNull(info, "info is null");
        Preconditions.checkNotNull(source, "source is null");

        this.info = info;
        this.source = source;
    }

    @Override
    public Iterator<T> iterator()
    {
        return source.iterator();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public Cursor cursor()
    {
        return new ValueCursor(info, source.iterator());
    }
}
