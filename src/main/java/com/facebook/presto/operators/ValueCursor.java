/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operators;

import com.facebook.presto.Cursor;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.facebook.presto.block.cursor.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ValueCursor implements Cursor
{
    private final Iterator<? extends ValueBlock> iterator;
    private final TupleInfo info;

    private BlockCursor currentValueBlockCursor;
    private BlockCursor nextValueBlockCursor;
    private boolean isValid;

    public ValueCursor(TupleInfo info, Iterator<? extends ValueBlock> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        Preconditions.checkArgument(iterator.hasNext(), "iterator is empty");
        Preconditions.checkNotNull(info, "info is null");

        this.info = info;
        this.iterator = iterator;

        // next value is within the next block
        // advance to next block
        ValueBlock valueBlock = iterator.next();
        nextValueBlockCursor = valueBlock.blockCursor();
        currentValueBlockCursor = valueBlock.blockCursor();
        nextValueBlockCursor.advanceNextValue();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public boolean hasNextValue()
    {
        return nextValueBlockCursor != null;
    }

    @Override
    public void advanceNextValue()
    {
        if (nextValueBlockCursor == null) {
            throw new NoSuchElementException();
        }

        isValid = true;
        currentValueBlockCursor.moveTo(nextValueBlockCursor);

        if (nextValueBlockCursor.hasNextValue()) {
            nextValueBlockCursor.advanceNextValue();
        }
        else if (iterator.hasNext()) {
            // next value is within the next block
            // advance to next block
            nextValueBlockCursor = iterator.next().blockCursor();
            nextValueBlockCursor.advanceNextValue();
        }
        else {
            // no more data
            nextValueBlockCursor = null;
        }
    }

    @Override
    public boolean hasNextPosition()
    {
        // if current value has more positions or we have a next value
        return nextValueBlockCursor != null || isValid && currentValueBlockCursor.hasNextValuePosition();
    }

    @Override
    public void advanceNextPosition()
    {
        isValid = true;
        if (currentValueBlockCursor.hasNextValuePosition()) {
            // next position is in the current value
            currentValueBlockCursor.advanceNextValuePosition();
        }
        else {
            advanceNextValue();
        }
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        return currentValueBlockCursor.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        return currentValueBlockCursor.getLong(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        return currentValueBlockCursor.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        return currentValueBlockCursor.getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        return currentValueBlockCursor.getValuePositionEnd();
    }

    @Override
    public long peekNextValuePosition()
    {
        if (nextValueBlockCursor == null) {
            throw new NoSuchElementException();
        }
        return nextValueBlockCursor.getPosition();
    }

    @Override
    public boolean currentValueEquals(Tuple value)
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        return currentValueBlockCursor.tupleEquals(value);
    }

    @Override
    public boolean nextValueEquals(Tuple value)
    {
        if (nextValueBlockCursor == null) {
            throw new NoSuchElementException();
        }
        return nextValueBlockCursor.tupleEquals(value);
    }

    @Override
    public void advanceToPosition(long position)
    {
        Preconditions.checkArgument(currentValueBlockCursor == null || position >= getPosition(), "Can't advance backwards");

        if (currentValueBlockCursor != null && position == getPosition()) {
            // position to current position? => no op
            return;
        }

        if (nextValueBlockCursor == null) {
            throw new NoSuchElementException();
        }

        // skip to block containing requested position
        if (position > nextValueBlockCursor.getRange().getEnd()) {
            do {
                nextValueBlockCursor = iterator.next().blockCursor();
            }
            while (position > nextValueBlockCursor.getRange().getEnd());
        }

        // skip to index within block
        nextValueBlockCursor.advanceToPosition(position);

        // advance the current position to new next position (and advance the next position)
        advanceNextPosition();
    }
}
