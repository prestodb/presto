/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.rle;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.Range;
import com.google.common.base.Preconditions;

public final class RunLengthEncodedBlockCursor implements BlockCursor
{
    private final Tuple value;
    private final Range range;
    private final long endPosition;
    private final long startPosition;

    private long position = -1;

    public RunLengthEncodedBlockCursor(Tuple value, Range range)
    {
        this.value = value;
        this.range = range;

        endPosition = range.getEnd();
        startPosition = range.getStart();

        // start one position before the start
        position = startPosition - 1;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return value.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public boolean isValid()
    {
        return startPosition <= position && position <= endPosition;
    }

    @Override
    public boolean isFinished()
    {
        return position > endPosition;
    }

    private void checkReadablePosition()
    {
        Preconditions.checkState(isValid(), "cursor is not valid");
    }

    @Override
    public boolean advanceNextValue()
    {
        position = Long.MAX_VALUE;
        return false;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (position >= endPosition) {
            position = Long.MAX_VALUE;
            return false;
        }

        position++;
        return true;
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        if (newPosition > range.getEnd()) {
            position = Long.MAX_VALUE;
            return false;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        this.position = newPosition;
        return true;
    }

    @Override
    public Tuple getTuple()
    {
        checkReadablePosition();
        return value;
    }

    @Override
    public long getLong(int field)
    {
        checkReadablePosition();
        return value.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        checkReadablePosition();
        return value.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkReadablePosition();
        return value.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        checkReadablePosition();
        return position;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        checkReadablePosition();
        return range.getEnd();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        return this.value.equals(value);
    }
}
