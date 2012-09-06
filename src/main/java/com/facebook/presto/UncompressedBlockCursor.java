package com.facebook.presto;

import com.facebook.presto.operators.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

public class UncompressedBlockCursor
        implements BlockCursor
{
    private final TupleInfo info;
    private Slice slice;
    private Range range;

    //
    // Current value and position of the cursor
    // If cursor before the first element, these will be null and -1
    //
    private long position = -1;
    private int offset = -1;
    private int size = -1;


    public UncompressedBlockCursor(TupleInfo info, UncompressedValueBlock block)
    {
        Preconditions.checkNotNull(info, "info is null");
        Preconditions.checkNotNull(block, "block is null");
        this.info = info;
        slice = block.getSlice();
        range = block.getRange();
    }

    public UncompressedBlockCursor(TupleInfo info, Slice slice, Range range)
    {
        this.info = info;
        this.slice = slice;
        this.range = range;
    }

    public UncompressedBlockCursor(TupleInfo info, Slice slice, Range range, long position, int offset, int size)
    {
        this.info = info;
        this.slice = slice;
        this.range = range;
        this.position = position;
        this.offset = offset;
        this.size = size;
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public void advanceTo(BlockCursor cursor)
    {
        Preconditions.checkArgument(cursor instanceof UncompressedBlockCursor, "cursor is not an instance of UncompressedBlockCursor");
        UncompressedBlockCursor other = (UncompressedBlockCursor) cursor;

        // todo assure that the cursors are for the same block stream?

        this.slice = other.slice;
        this.range = other.range;
        this.position = other.position;
        this.offset = other.offset;
        this.size = other.size;
    }

    @Override
    public BlockCursor duplicate()
    {
        return new UncompressedBlockCursor(info, slice, range, position, offset, size);
    }

    @Override
    public boolean hasNextValue()
    {
        // every position is a new value
        return hasNextPosition();
    }

    @Override
    public void advanceNextValue()
    {
        // every position is a new value
        advanceNextPosition();
    }

    @Override
    public boolean hasNextPosition()
    {
        return position < range.getEnd();
    }

    @Override
    public void advanceNextPosition()
    {
        if (!hasNextPosition()) {
            throw new NoSuchElementException();
        }

        if (position < 0) {
            position = range.getStart();
            offset = 0;
            size = info.size(slice, 0);
        } else {
            position++;
            offset += size;
            size = info.size(slice, offset);
        }
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
    public Slice getSlice(int field)
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        return info.getSlice(slice, offset, field);
    }

    @Override
    public long getPosition()
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        return position;
    }

    @Override
    public boolean hasMorePositionsForCurrentValue()
    {
        return false;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        return position;
    }

    @Override
    public boolean tupleEquals(Tuple value)
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        Slice tupleSlice = value.getTupleSlice();
        return slice.equals(offset, size, tupleSlice, 0, tupleSlice.length());
    }
}
