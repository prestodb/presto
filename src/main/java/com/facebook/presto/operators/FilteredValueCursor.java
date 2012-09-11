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
import com.google.common.base.Predicate;

import java.util.Iterator;

public class FilteredValueCursor implements Cursor
{
    private final Predicate<BlockCursor> predicate;
    private final Iterator<? extends ValueBlock> iterator;
    private final TupleInfo info;

    private BlockCursor currentValueBlockCursor;
    private BlockCursor nextValueBlockCursor;
    private boolean isValid;

    public FilteredValueCursor(Predicate<BlockCursor> predicate, TupleInfo info, Iterator<? extends ValueBlock> iterator)
    {
        Preconditions.checkNotNull(predicate, "predicate is null");
        Preconditions.checkNotNull(iterator, "iterator is null");
        Preconditions.checkArgument(iterator.hasNext(), "iterator is empty");
        Preconditions.checkNotNull(info, "info is null");

        this.predicate = predicate;
        this.info = info;
        this.iterator = iterator;

        ValueBlock valueBlock = iterator.next();
        nextValueBlockCursor = valueBlock.blockCursor();
        currentValueBlockCursor = valueBlock.blockCursor();
        findNextValue();
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
        findNextValue();
        return true;
    }

    private void findNextValue()
    {
        // advance next position until predicate is satisfied
        do {
            if (!nextValueBlockCursor.advanceToNextValue()) {
                if (iterator.hasNext()) {
                    // next value is within the next block
                    // advance to next block
                    nextValueBlockCursor = iterator.next().blockCursor();
                    nextValueBlockCursor.advanceToNextValue();
                }
                else {
                    // no more data
                    nextValueBlockCursor = null;
                    return;
                }
            }
        } while (!predicate.apply(nextValueBlockCursor));
    }

    @Override
    public boolean advanceNextPosition()
    {
        isValid = true;
        return currentValueBlockCursor.advanceNextPosition() || advanceNextValue();
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
    public boolean advanceToPosition(long newPosition)
    {
        Preconditions.checkArgument(currentValueBlockCursor == null || newPosition >= getPosition(), "Can't advance backwards");

        if (currentValueBlockCursor != null && newPosition == getPosition()) {
            // position to current position => no op
            return true;
        }

        // todo this is not correct... can't advance to position in current value
        if (nextValueBlockCursor == null) {
            return false;
        }

        // skip to block containing requested position
        if (newPosition > nextValueBlockCursor.getRange().getEnd()) {
            do {
                nextValueBlockCursor = iterator.next().blockCursor();
            }
            while (newPosition > nextValueBlockCursor.getRange().getEnd());
        }

        // skip to index within block
        nextValueBlockCursor.advanceToPosition(newPosition);

        if (predicate.apply(nextValueBlockCursor)) {
            // advance the current position to new next position (and advance the next position)
            return advanceNextPosition();
        }
        else {
            // value at the position isn't valid, so advance until we find a value that satisfies the predicate
            return advanceNextValue();
        }
    }
}
