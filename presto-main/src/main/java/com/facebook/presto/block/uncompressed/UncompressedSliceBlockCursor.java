package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.Range;
import com.google.common.base.Preconditions;

import static com.facebook.presto.slice.SizeOf.SIZE_OF_INT;

public class UncompressedSliceBlockCursor
        implements BlockCursor
{
    private final Slice slice;
    private final Range range;
    private final long endPosition;
    private final long startPosition;

    private long position;
    private int offset;
    private int size;

    public UncompressedSliceBlockCursor(UncompressedBlock block)
    {
        Preconditions.checkNotNull(block, "block is null");

        this.slice = block.getSlice();
        this.range = block.getRange();

        endPosition = range.getEnd();
        startPosition = range.getStart();

        // start one position before the start
        position = startPosition - 1;
        offset = block.getRawOffset();
        size = 0;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_VARBINARY;
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
        // every value is a new position
        return advanceNextPosition();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (position >= endPosition) {
            position = Long.MAX_VALUE;
            return false;
        }

        position++;
        offset += size;
        size = slice.getInt(offset) & 0x7F_FF_FF_FF;
        return true;
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        if (newPosition > endPosition) {
            position = newPosition;
            return false;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        // advance to specified position
        while (position < newPosition) {
            position++;
            offset += size;
            size = slice.getInt(offset) & 0x7F_FF_FF_FF;
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
        return slice.slice(offset + SIZE_OF_INT, size - SIZE_OF_INT);
    }

    @Override
    public boolean isNull(int field)
    {
        checkReadablePosition();
        Preconditions.checkElementIndex(0, 1, "field");
        return (slice.getInt(offset) & 0x80_00_00_00) != 0;
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        Slice tupleSlice = value.getTupleSlice();
        return slice.equals(offset, size, tupleSlice, 0, tupleSlice.length());
    }
}
