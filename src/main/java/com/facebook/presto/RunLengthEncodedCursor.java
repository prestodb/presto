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
    private RunLengthEncodedBlock block;
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
    public boolean isFinished()
    {
        return !iterator.hasNext() && position > block.getRange().getEnd();
    }

    @Override
    public boolean advanceNextValue()
    {
        if (!iterator.hasNext()) {
            return false;
        }
        block = iterator.next();
        position = block.getRange().getStart();
        return true;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (block == null || position == block.getRange().getEnd()) {
            return advanceNextValue();
        }
        else {
            position++;
            return true;
        }
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        Preconditions.checkArgument(block == null || newPosition >= getPosition(), "Can't advance backwards");

        if (block == null) {
            if (iterator.hasNext()) {
                block = iterator.next();
            }
            else {
                return false;
            }
        }

        // skip to block containing requested position
        while (newPosition > block.getRange().getEnd() && iterator.hasNext()) {
            block = iterator.next();
        }

        if (newPosition > block.getRange().getEnd()) {
            block = null;
            return false;
        }

        this.position = Math.max(newPosition, block.getRange().getStart());
        return true;
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(block != null, "Need to call advanceNext() first");

        return block.getSingleValue();
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(block != null, "Need to call advanceNext() first");

        return block.getSingleValue().getLong(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(block != null, "Need to call advanceNext() first");

        return block.getSingleValue().getSlice(field);
    }

    @Override
    public long getPosition()
    {
        return position;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Preconditions.checkState(block != null, "Need to call advance first");

        return block.getRange().getEnd();
    }

    @Override
    public boolean currentValueEquals(Tuple value)
    {
        Preconditions.checkState(block != null, "Need to call advanceNext() first");

        return block.getSingleValue().equals(value);
    }

}
