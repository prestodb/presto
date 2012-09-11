package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

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
        Preconditions.checkArgument(iterator.hasNext(), "iterator is empty");
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
    public boolean advanceNextValue()
    {
        if (!iterator.hasNext()) {
            return false;
        }
        current = iterator.next();
        position = current.getRange().getStart();
        return true;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (current == null || position == current.getRange().getEnd()) {
            return advanceNextValue();
        }
        else {
            position++;
            return true;
        }
    }

    @Override
    public boolean advanceToPosition(long position)
    {
        Preconditions.checkArgument(current == null || position >= getPosition(), "Can't advance backwards");

        if (current == null) {
            return advanceNextValue();
        }

        // skip to block containing requested position
        while (position > current.getRange().getEnd() && advanceNextPosition());
        if (position > current.getRange().getEnd()) {
            return false;
        }

        this.position = Math.max(position, current.getRange().getStart());
        return true;
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(current != null, "Need to call advanceNext() first");

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
    public long getPosition()
    {
        return position;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Preconditions.checkState(current != null, "Need to call advance first");

        return current.getRange().getEnd();
    }

    @Override
    public boolean currentValueEquals(Tuple value)
    {
        Preconditions.checkState(current != null, "Need to call advanceNext() first");

        return current.getSingleValue().equals(value);
    }

}
