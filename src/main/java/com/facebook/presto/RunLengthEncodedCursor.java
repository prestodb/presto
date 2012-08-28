package com.facebook.presto;

import com.facebook.presto.Cursor;
import com.facebook.presto.RunLengthEncodedBlock;
import com.facebook.presto.Slice;
import com.facebook.presto.TupleInfo;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class RunLengthEncodedCursor
        implements Cursor
{
    private final TupleInfo info;
    private final Iterator<RunLengthEncodedBlock> iterator;
    private RunLengthEncodedBlock current;

    public RunLengthEncodedCursor(TupleInfo info, Iterator<RunLengthEncodedBlock> iterator)
    {
        Preconditions.checkNotNull(info, "info is null");
        Preconditions.checkNotNull(iterator, "iterator is null");

        this.info = info;
        this.iterator = iterator;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public boolean hasNextValue()
    {
        return iterator.hasNext();
    }

    @Override
    public void advanceNextValue()
    {
        current = iterator.next();
    }

    @Override
    public boolean hasNextPosition()
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void advanceNextPosition()
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(current != null, "Need to call advanceNext() first");

        return current.getSingleValue().getLong(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(current != null, "Need to call advanceNext() first");

        return current.getSingleValue().getSlice(field);
    }

    @Override
    public boolean equals(Cursor other)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long getPosition()
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean equals(int field, Slice value)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
