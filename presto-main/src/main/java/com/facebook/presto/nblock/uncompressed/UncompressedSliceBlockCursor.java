package com.facebook.presto.nblock.uncompressed;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.google.common.base.Preconditions.checkState;

public class UncompressedSliceBlockCursor
        implements BlockCursor
{
    private final Slice slice;
    private final Range range;
    private long position = -1;
    private int offset;
    private int size = -1;

    public UncompressedSliceBlockCursor(UncompressedBlock block)
    {
        Preconditions.checkNotNull(block, "block is null");

        this.slice = block.getSlice();
        this.range = block.getRange();
        this.offset = block.getRawOffset();
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
        // every position is a new value
        if (position >= range.getEnd()) {
            position = Long.MAX_VALUE;
            return false;
        }

        if (position < 0) {
            position = range.getStart();
        }
        else {
            position++;
            offset += size;
        }
        size = slice.getShort(offset);
        return true;
    }

    @Override
    public boolean advanceNextPosition()
    {
        return advanceNextValue();
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        if (newPosition > range.getEnd()) {
            position = newPosition;
            return false;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        // move to initial position
        if (position < 0) {
            position = range.getStart();
            size = slice.getShort(0);
        }

        // advance to specified position
        while (position < newPosition) {
            position++;
            offset += size;
            size = slice.getShort(offset);
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
        return slice.slice(offset + SIZE_OF_SHORT, size - SIZE_OF_SHORT);
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        Slice tupleSlice = value.getTupleSlice();
        return slice.equals(offset, size, tupleSlice, 0, tupleSlice.length());
    }
}
