/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Cursor;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class GenericCursor implements Cursor
{
    private final Iterator<? extends Block> iterator;
    private final TupleInfo info;

    private BlockCursor blockCursor;
    private boolean isValid;

    public GenericCursor(TupleInfo info, Iterator<? extends Block> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        Preconditions.checkArgument(iterator.hasNext(), "iterator is empty");
        Preconditions.checkNotNull(info, "info is null");

        this.info = info;
        this.iterator = iterator;

        blockCursor = iterator.next().blockCursor();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
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

        isValid = true;
        if (!blockCursor.advanceToNextValue()) {
            if (iterator.hasNext()) {
                blockCursor = iterator.next().blockCursor();
                blockCursor.advanceNextPosition();
            } else {
                blockCursor = null;
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (blockCursor == null) {
            return false;
        }

        isValid = true;
        return blockCursor.advanceNextPosition() || advanceNextValue();
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.getValuePositionEnd();
    }

    @Override
    public boolean currentValueEquals(Tuple value)
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        if (blockCursor == null)  {
            throw new NoSuchElementException();
        }
        return blockCursor.tupleEquals(value);
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        Preconditions.checkArgument(!isValid || newPosition >= getPosition(), "Can't advance backwards");

        if (blockCursor == null) {
            return false;
        }

        if (isValid && newPosition == getPosition()) {
            // position to current position? => no op
            return true;
        }

        isValid = true;

        // skip to block containing requested position
        while (newPosition > blockCursor.getRange().getEnd() && iterator.hasNext()) {
            blockCursor = iterator.next().blockCursor();
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
