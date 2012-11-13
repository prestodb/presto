package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;

import static com.facebook.presto.slice.SizeOf.SIZE_OF_BYTE;
import static com.facebook.presto.slice.SizeOf.SIZE_OF_INT;

public class UncompressedSliceBlockCursor
        implements BlockCursor
{
    private final Slice slice;
    private final int positionCount;

    private long position;
    private int offset;
    private int size;

    public UncompressedSliceBlockCursor(UncompressedBlock block)
    {
        Preconditions.checkNotNull(block, "block is null");

        this.slice = block.getSlice();
        this.positionCount = block.getPositionCount();

        // start one position before the start
        position = -1;
        offset = block.getRawOffset();
        size = 0;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_VARBINARY;
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
        size = slice.getInt(offset + SIZE_OF_BYTE);
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
            size = slice.getInt(offset + SIZE_OF_BYTE);
        }
        return true;
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
        return new Tuple(slice.slice(offset, size), TupleInfo.SINGLE_VARBINARY);
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        checkReadablePosition();
        Preconditions.checkElementIndex(0, 1, "field");
        return slice.slice(offset + SIZE_OF_INT + SIZE_OF_BYTE, size - SIZE_OF_INT - SIZE_OF_BYTE);
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
        return slice.equals(offset, size, tupleSlice, 0, tupleSlice.length());
    }
}
