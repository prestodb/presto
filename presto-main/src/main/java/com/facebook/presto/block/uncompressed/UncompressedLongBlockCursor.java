package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;

import static com.facebook.presto.slice.SizeOf.SIZE_OF_BYTE;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class UncompressedLongBlockCursor
        implements BlockCursor
{
    private static final int ENTRY_SIZE = SIZE_OF_LONG + SIZE_OF_BYTE;
    private final Slice slice;
    private final int positionCount;

    private long position;
    private int offset;

    public UncompressedLongBlockCursor(int positionCount, Slice slice, int sliceOffset)
    {
        Preconditions.checkArgument(positionCount >= 0, "positionCount is negative");
        Preconditions.checkNotNull(positionCount, "positionCount is null");
        Preconditions.checkPositionIndex(sliceOffset, slice.length(), "sliceOffset");

        this.positionCount = positionCount;

        this.slice = slice;

        // start one position before the start
        position = -1;
        offset = sliceOffset - ENTRY_SIZE;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
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
        offset += ENTRY_SIZE;
        return true;
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        // if new position is out of range, return false
        if (newPosition >= positionCount) {
            position = positionCount;
            return false;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        offset += (int) ((newPosition - position) * ENTRY_SIZE);
        position = newPosition;

        return true;
    }

    @Override
    public Block createBlockViewPort(int length)
    {
        // view port starts at next position
        int startOffset = offset + ENTRY_SIZE;
        length = Math.min(length, getRemainingPositions());

        // advance to end of view port
        offset += length * ENTRY_SIZE;
        position += length;

        return new UncompressedBlock(length, SINGLE_LONG, slice, startOffset);
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
        return new Tuple(slice.slice(offset, ENTRY_SIZE), TupleInfo.SINGLE_LONG);
    }

    @Override
    public long getLong(int field)
    {
        checkReadablePosition();
        Preconditions.checkElementIndex(0, 1, "field");
        return slice.getLong(offset + SIZE_OF_BYTE);
    }

    @Override
    public double getDouble(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkReadablePosition();
        Preconditions.checkElementIndex(0, 1, "field");
        return slice.getByte(offset) != 0;
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        Slice tupleSlice = value.getTupleSlice();
        return tupleSlice.length() == SIZE_OF_LONG + SIZE_OF_BYTE && slice.getLong(offset + SIZE_OF_BYTE) == tupleSlice.getLong(SIZE_OF_BYTE);
    }
}
