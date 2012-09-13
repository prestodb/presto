package com.facebook.presto.block.cursor;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.UncompressedValueBlock;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import static com.google.common.base.Preconditions.checkNotNull;

public class UncompressedBlockCursor
        implements BlockCursor
{
    private final TupleInfo info;
    private final Slice slice;
    private final Range range;
    private long position = -1;
    private int offset = -1;
    private int size = -1;

    public UncompressedBlockCursor(TupleInfo info, UncompressedValueBlock block)
    {
        this(checkNotNull(info, "info is null"), checkNotNull(block, "block is null").getSlice(), block.getRange());
    }

    public UncompressedBlockCursor(TupleInfo info, Slice slice, Range range)
    {
        this.info = info;
        this.slice = slice;
        this.range = range;
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public boolean advanceToNextValue()
    {
        // every position is a new value
        if (position >= range.getEnd()) {
            return false;
        }

        if (position < 0) {
            position = range.getStart();
            offset = 0;
        } else {
            position++;
            offset += size;
        }
        size = info.size(slice, offset);
        return true;
    }

    @Override
    public boolean advanceNextPosition()
    {
        return advanceToNextValue();
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        if (newPosition > range.getEnd()) {
            position = newPosition;
            return false;
        }

        // move to initial position
        if (position < 0) {
            position = range.getStart();
            offset = 0;
            size = info.size(slice, 0);
        }

        // advance to specified position
        while (position < newPosition) {
            position++;
            offset += size;
            size = info.size(slice, offset);
        }

        return true;
    }

    @Override
    public long getPosition()
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        return position;
    }

    @Override
    public long getValuePositionEnd()
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        return position;
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        return new Tuple(slice.slice(offset, size), info);
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        return info.getLong(slice, offset, field);
    }

    @Override
    public double getDouble(int field)
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        return info.getDouble(slice, offset, field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        return info.getSlice(slice, offset, field);
    }

    @Override
    public boolean tupleEquals(Tuple value)
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        Slice tupleSlice = value.getTupleSlice();
        return slice.equals(offset, size, tupleSlice, 0, tupleSlice.length());
    }
}
