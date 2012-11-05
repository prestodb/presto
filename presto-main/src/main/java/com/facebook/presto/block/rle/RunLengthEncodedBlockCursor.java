/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.rle;

import com.facebook.presto.util.Range;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkState;

public final class RunLengthEncodedBlockCursor implements BlockCursor
{
    private final Tuple value;
    private final Range range;
    private long position = -1;

    public RunLengthEncodedBlockCursor(Tuple value, Range range)
    {
        this.value = value;
        this.range = range;
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
        return range.contains(position);
    }

    @Override
    public boolean isFinished()
    {
        return position > range.getEnd();
    }

    private void checkReadablePosition()
    {
        if (position > range.getEnd()) {
            throw new NoSuchElementException("already finished");
        }
        checkState(position >= range.getStart(), "cursor not yet advanced");
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
        if (position >= range.getEnd()) {
            position = Long.MAX_VALUE;
            return false;
        }

        if (position < 0) {
            position = range.getStart();
        } else {
            position++;
        }
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
