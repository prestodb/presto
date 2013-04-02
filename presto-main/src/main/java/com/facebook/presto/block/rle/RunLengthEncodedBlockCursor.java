/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.rle;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;

public final class RunLengthEncodedBlockCursor implements BlockCursor
{
    private final Tuple value;
    private final int positionCount;

    private int position = -1;

    public RunLengthEncodedBlockCursor(Tuple value, int positionCount)
    {
        this.value = value;
        this.positionCount = positionCount;
        position = -1;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return value.getTupleInfo();
    }

    @Override
    public int getRemainingPositions()
    {
        return positionCount - (position + 1);
    }

    @Override
    public boolean isValid()
    {
        return 0 <= position && position < positionCount;
    }

    @Override
    public boolean isFinished()
    {
        return position >= positionCount;
    }

    private void checkReadablePosition()
    {
        Preconditions.checkState(isValid(), "cursor is not valid");
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (position >= positionCount - 1) {
            position = positionCount;
            return false;
        }

        position++;
        return true;
    }

    @Override
    public boolean advanceToPosition(int newPosition)
    {
        if (newPosition >= positionCount) {
            position = positionCount;
            return false;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        this.position = newPosition;
        return true;
    }

    @Override
    public Block getRegionAndAdvance(int length)
    {
        length = Math.min(length, getRemainingPositions());

        // advance to end of view port
        position += length;

        return new RunLengthEncodedBlock(value, length);
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
    public boolean isNull(int field)
    {
        checkReadablePosition();
        return value.isNull(field);
    }

    @Override
    public int getPosition()
    {
        checkReadablePosition();
        return position;
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        return this.value.equals(value);
    }

    @Override
    public int getRawOffset()
    {
        return 0;
    }

    @Override
    public Slice getRawSlice()
    {
        return value.getTupleSlice();
    }

    @Override
    public void appendTupleTo(BlockBuilder blockBuilder)
    {
        blockBuilder.append(value);
    }
}
