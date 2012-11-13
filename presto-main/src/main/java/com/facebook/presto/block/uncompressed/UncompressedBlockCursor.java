package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;

public class UncompressedBlockCursor
        implements BlockCursor
{
    private final TupleInfo tupleInfo;
    private final Slice slice;
    private final int positionCount;

    private long position;
    private int offset;
    private int size;

    public UncompressedBlockCursor(TupleInfo tupleInfo, int positionCount, Slice slice, int sliceOffset)
    {
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkArgument(positionCount >= 0, "positionCount is negative");
        Preconditions.checkNotNull(positionCount, "positionCount is null");
        Preconditions.checkPositionIndex(sliceOffset, slice.length(), "sliceOffset");

        this.tupleInfo = tupleInfo;
        this.positionCount = positionCount;

        this.slice = slice;
        offset = sliceOffset;
        size = 0;

        // start one position before the start
        position = -1;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public int getRemainingPositions()
    {
        return (int) (positionCount - (position + 1));
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
    public boolean advanceNextValue()
    {
        // every value is a new position
        return advanceNextPosition();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (position >= positionCount -1) {
            position = positionCount;
            return false;
        }

        position++;
        offset += size;
        size = tupleInfo.size(slice, offset);
        return true;
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        if (newPosition >= positionCount) {
            position = positionCount;
            return false;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        // advance to specified position
        while (position < newPosition) {
            position++;
            offset += size;
            size = tupleInfo.size(slice, offset);
        }
        return true;
    }

    @Override
    public Block createBlockViewPort(int length)
    {
        // view port starts at next position
        int startOffset = offset + size;
        length = Math.min(length, getRemainingPositions());

        // advance to end of view port
        for (int i = 0; i < length; i++) {
            position++;
            offset += size;
            size = tupleInfo.size(slice, offset);
        }

        return new UncompressedBlock(length, tupleInfo, slice, startOffset);
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
        return position;
    }

    @Override
    public Tuple getTuple()
    {
        checkReadablePosition();
        return new Tuple(slice.slice(offset, size), tupleInfo);
    }

    @Override
    public long getLong(int field)
    {
        checkReadablePosition();
        return tupleInfo.getLong(slice, offset, field);
    }

    @Override
    public double getDouble(int field)
    {
        checkReadablePosition();
        return tupleInfo.getDouble(slice, offset, field);
    }

    @Override
    public Slice getSlice(int field)
    {
        checkReadablePosition();
        return tupleInfo.getSlice(slice, offset, field);
    }

    @Override
    public boolean isNull(int field)
    {
        checkReadablePosition();
        return tupleInfo.isNull(slice, offset, field);
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        Slice tupleSlice = value.getTupleSlice();
        return slice.equals(offset, size, tupleSlice, 0, tupleSlice.length());
    }
}
