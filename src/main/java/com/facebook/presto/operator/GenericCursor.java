/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class GenericCursor implements Cursor
{
    private final Iterator<? extends TupleStream> iterator;
    private final TupleInfo info;

    private Cursor blockCursor;
    private boolean hasAdvanced;

    public GenericCursor(TupleInfo info, Iterator<? extends TupleStream> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        Preconditions.checkNotNull(info, "info is null");

        this.info = info;
        this.iterator = iterator;

        if (iterator.hasNext()) {
            blockCursor = iterator.next().cursor();
        }
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
    public boolean isValid()
    {
        return hasAdvanced && blockCursor != null;
    }

    @Override
    public boolean isFinished()
    {
        return blockCursor == null;
    }

    @Override
    public boolean advanceNextValue()
    {
        if (blockCursor == null) {
            return false;
        }

        hasAdvanced = true;
        if (blockCursor.advanceNextValue()) {
            return true;
        }

        while (iterator.hasNext()) {
            blockCursor = iterator.next().cursor();
            if (blockCursor.advanceNextPosition()) {
                return true;
            }
        }

        blockCursor = null;
        return false;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (blockCursor == null) {
            return false;
        }

        hasAdvanced = true;
        return blockCursor.advanceNextPosition() || advanceNextValue();
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(hasAdvanced, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(hasAdvanced, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        Preconditions.checkState(hasAdvanced, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(hasAdvanced, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        Preconditions.checkState(hasAdvanced, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Preconditions.checkState(hasAdvanced, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getCurrentValueEndPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        Preconditions.checkState(hasAdvanced, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.currentTupleEquals(value);
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        Preconditions.checkArgument(!hasAdvanced || newPosition >= getPosition(), "Can't advance backwards");

        if (blockCursor == null) {
            return false;
        }

        if (hasAdvanced && newPosition == getPosition()) {
            // position to current position? => no op
            return true;
        }

        hasAdvanced = true;

        // skip to block containing requested position
        while (newPosition > blockCursor.getRange().getEnd() && iterator.hasNext()) {
            blockCursor = iterator.next().cursor();
        }

        // is the position off the end of the stream?
        if (newPosition > blockCursor.getRange().getEnd()) {
            blockCursor = null;
            return false;
        }

        if (!blockCursor.advanceToPosition(newPosition)){
            throw new IllegalStateException("Internal error: position not found");
        }
        return true;
    }
}
