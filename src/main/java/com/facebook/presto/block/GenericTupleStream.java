package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.operator.GenericCursor;
import com.google.common.base.Preconditions;

import java.util.Arrays;

import static com.facebook.presto.block.BlockIterators.toBlockIterable;
import static com.google.common.base.Preconditions.checkNotNull;

public class GenericTupleStream<T extends TupleStream>
        implements TupleStream, BlockIterable<T>
{
    private final TupleInfo info;
    private final BlockIterable<T> source;

    @SafeVarargs
    public GenericTupleStream(TupleInfo info, T... source)
    {
        this(info, Arrays.asList(source));
    }

    public GenericTupleStream(TupleInfo info, Iterable<T> source)
    {
        this(info, toBlockIterable(checkNotNull(source, "source is null")));
    }

    public GenericTupleStream(TupleInfo info, BlockIterable<T> source)
    {
        checkNotNull(info, "info is null");
        checkNotNull(source, "source is null");

        this.info = info;
        this.source = source;
    }

    @Override
    public BlockIterator<T> iterator(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return source.iterator(session);
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public Cursor cursor(QuerySession session)
    {
        Preconditions.checkNotNull(session, "session is null");
        return new GenericCursor(session, info, source.iterator(session));
    }
}
