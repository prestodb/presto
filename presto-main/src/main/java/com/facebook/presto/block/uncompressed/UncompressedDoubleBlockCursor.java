package com.facebook.presto.block.uncompressed;

import com.facebook.presto.Range;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

import static com.facebook.presto.SizeOf.SIZE_OF_DOUBLE;
import static com.google.common.base.Preconditions.checkState;

public class UncompressedDoubleBlockCursor
        implements BlockCursor
{
    private final Slice slice;
    private final Range range;
    private long position = -1;
    private int offset;

    public UncompressedDoubleBlockCursor(UncompressedBlock block)
    {
        Preconditions.checkNotNull(block, "block is null");

        this.slice = block.getSlice();
        this.range = block.getRange();
        this.offset = block.getRawOffset();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_DOUBLE;
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
        } else {
            position++;
            offset += SIZE_OF_DOUBLE;
        }
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
        // if new position is out of range, return false
        if (newPosition > range.getEnd()) {
            position = Long.MAX_VALUE;
            return false;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        if (position < 0) {
            position = range.getStart();
        }

        offset += (int) ((newPosition - position) * SIZE_OF_DOUBLE);
        position = newPosition;

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
        return new Tuple(slice.slice(offset, SIZE_OF_DOUBLE), TupleInfo.SINGLE_DOUBLE);
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(int field)
    {
        checkReadablePosition();
        Preconditions.checkElementIndex(0, 1, "field");
        return slice.getDouble(offset);
    }

    @Override
    public Slice getSlice(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        checkReadablePosition();
        Slice tupleSlice = value.getTupleSlice();
        return tupleSlice.length() == SIZE_OF_DOUBLE && slice.getDouble(offset) == tupleSlice.getDouble(0);
    }
}
