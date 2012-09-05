package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class RunLengthEncodedCursor
        implements Cursor
{
    private final TupleInfo info;
    private final PeekingIterator<RunLengthEncodedBlock> iterator;
    private RunLengthEncodedBlock current;
    private long position;

    public RunLengthEncodedCursor(TupleInfo info, Iterator<RunLengthEncodedBlock> iterator)
    {
        Preconditions.checkNotNull(info, "info is null");
        Preconditions.checkNotNull(iterator, "iterator is null");

        this.info = info;
        this.iterator = Iterators.peekingIterator(iterator);
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
        position = current.getRange().getStart();
    }

    @Override
    public boolean hasNextPosition()
    {
        if (current == null || position == current.getRange().getEnd()) {
            return hasNextValue();
        }

        return true;
    }

    @Override
    public void advanceNextPosition()
    {
        if (current == null || position == current.getRange().getEnd()) {
            advanceNextValue();
        }
        else {
            position++;
        }
    }

    @Override
    public Tuple getTuple()
    {
        return current.getSingleValue();
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
        return position;
    }

    @Override
    public long peekNextValuePosition()
    {
        if (!iterator.hasNext()) {
            throw new NoSuchElementException();
        }

        return iterator.peek().getRange().getStart();
    }

    @Override
    public boolean equals(Tuple value)
    {
        Preconditions.checkState(current != null, "Need to call advanceNext() first");

        return current.getSingleValue().equals(value);
    }

    @Override
    public boolean equals(int field, Slice value)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public boolean nextValueEquals(Tuple value)
    {
        if (!iterator.hasNext()) {
            throw new NoSuchElementException();
        }

        return iterator.peek().getSingleValue().equals(value);
    }
}
