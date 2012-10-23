/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.*;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import static com.facebook.presto.block.Cursor.AdvanceResult.*;

public class GenericCursor implements Cursor
{
    private final QuerySession session;
    private final TupleInfo info;
    private final YieldingIterator<? extends TupleStream> iterator;

    private Cursor blockCursor;
    private boolean hasAdvanced;

    public GenericCursor(QuerySession session, TupleInfo info, YieldingIterator<? extends TupleStream> iterator)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(info, "info is null");
        Preconditions.checkNotNull(iterator, "iterator is null");

        this.session = session;
        this.info = info;
        this.iterator = iterator;

        if (iterator.hasNext()) {
            blockCursor = iterator.next().cursor(session);
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
    public AdvanceResult advanceNextValue()
    {
        if (blockCursor == null) {
            return FINISHED;
        }

        hasAdvanced = true;
        AdvanceResult result = blockCursor.advanceNextValue();
        if (result != FINISHED) {
            return result;
        }

        while (iterator.canAdvance()) {
            blockCursor = iterator.next().cursor(session);
            result = blockCursor.advanceNextPosition();
            if (result != FINISHED) {
                return result;
            }
        }
        if (iterator.mustYield()) {
            return MUST_YIELD;
        }

        blockCursor = null;
        return FINISHED;
    }

    @Override
    public AdvanceResult advanceNextPosition()
    {
        if (blockCursor == null) {
            return FINISHED;
        }

        hasAdvanced = true;

        AdvanceResult result = blockCursor.advanceNextPosition();
        if (result != FINISHED) {
            return result;
        }
        return advanceNextValue();
    }

    @Override
    public Tuple getTuple()
    {
        Cursors.checkReadablePosition(this);
        return blockCursor.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        Cursors.checkReadablePosition(this);
        return blockCursor.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        Cursors.checkReadablePosition(this);
        return blockCursor.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Cursors.checkReadablePosition(this);
        return blockCursor.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        Cursors.checkReadablePosition(this);
        return blockCursor.getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Cursors.checkReadablePosition(this);
        return blockCursor.getCurrentValueEndPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        Cursors.checkReadablePosition(this);
        return blockCursor.currentTupleEquals(value);
    }

    @Override
    public AdvanceResult advanceToPosition(long newPosition)
    {
        Preconditions.checkArgument(!hasAdvanced || newPosition >= getPosition(), "Can't advance backwards");

        if (blockCursor == null) {
            return FINISHED;
        }

        if (hasAdvanced && newPosition == getPosition()) {
            // position to current position? => no op
            return SUCCESS;
        }

        hasAdvanced = true;

        // skip to block containing requested position
        while (newPosition > blockCursor.getRange().getEnd() && iterator.canAdvance()) {
            blockCursor = iterator.next().cursor(session);
        }

        if (iterator.mustYield()) {
            return MUST_YIELD;
        }

        // is the position off the end of the stream?
        if (newPosition > blockCursor.getRange().getEnd()) {
            blockCursor = null;
            return FINISHED;
        }

        return blockCursor.advanceToPosition(newPosition);
    }
}
