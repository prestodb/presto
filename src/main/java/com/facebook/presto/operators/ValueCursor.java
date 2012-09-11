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
        currentValueBlockCursor = valueBlock.blockCursor();
        nextValueBlockCursor = valueBlock.blockCursor();
        nextValueBlockCursor.advanceNextPosition();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public boolean advanceNextValue()
    {
        if (nextValueBlockCursor == null) {
            return false;
        }

        isValid = true;
        currentValueBlockCursor.moveTo(nextValueBlockCursor);

        findNext();
        return true;
    }

    @Override
    public boolean advanceNextPosition()
    {
        isValid = true;
        if (currentValueBlockCursor.advanceNextPosition()) {
            // if current position caught up to next value cursor, advance the next value cursor
            if (nextValueBlockCursor != null && nextValueBlockCursor.getPosition() <= currentValueBlockCursor.getPosition()) {
                findNext();
            }
        } else if (nextValueBlockCursor != null) {
            // no more positions in the current block, move to the next block
            currentValueBlockCursor.moveTo(nextValueBlockCursor);

            findNext();
        } else {
            // no more data
            return false;
        }
        return true;
    }

    private void findNext()
    {
        if (!nextValueBlockCursor.advanceToNextValue()) {
            if (iterator.hasNext()) {
                // next value is within the next block
                // advance to next block
                nextValueBlockCursor = iterator.next().blockCursor();
                nextValueBlockCursor.advanceNextPosition();
            }
            else {
                // no more data
                nextValueBlockCursor = null;
            }
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
    public boolean currentValueEquals(Tuple value)
    {
        Preconditions.checkState(isValid, "Need to call advanceNext() first");
        return currentValueBlockCursor.tupleEquals(value);
    }

    @Override
    public boolean advanceToPosition(long position)
    {
        Preconditions.checkArgument(currentValueBlockCursor == null || position >= getPosition(), "Can't advance backwards");

        if (currentValueBlockCursor != null && position == getPosition()) {
            // position to current position? => no op
            return true;
        }

        if (nextValueBlockCursor == null) {
            return false;
        }

        // skip to block containing requested position
        if (iterator.hasNext() && position > nextValueBlockCursor.getRange().getEnd()) {
            do {
                nextValueBlockCursor = iterator.next().blockCursor();
            }
            while (iterator.hasNext() && position > nextValueBlockCursor.getRange().getEnd());
        }

        // skip to index within block
        nextValueBlockCursor.advanceToPosition(position);

        // advance the current position to new next position (and advance the next position)
        return advanceNextPosition();
    }
}
