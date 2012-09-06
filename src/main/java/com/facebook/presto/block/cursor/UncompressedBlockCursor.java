package com.facebook.presto.block.cursor;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.UncompressedValueBlock;
import com.facebook.presto.block.cursor.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkNotNull;

public class UncompressedBlockCursor
        implements BlockCursor
{
    private final TupleInfo info;
    private Slice slice;
    private Range range;
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
    public void moveTo(BlockCursor cursor)
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
        return position < range.getEnd();
    }

    @Override
    public void advanceNextValue()
    {
        if (!hasNextValue()) {
            throw new NoSuchElementException();
        }

        if (position < 0) {
            position = range.getStart();
            offset = 0;
        } else {
            position++;
            offset += size;
        }
        size = info.size(slice, offset);
    }

    @Override
    public boolean hasNextValuePosition()
    {
        return false;
    }

    @Override
    public void advanceNextValuePosition()
    {
        throw new NoSuchElementException();
    }

    @Override
    public void advanceToPosition(long newPosition)
    {
        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");
        Preconditions.checkArgument(newPosition <= this.range.getEnd(), "Can't advance off the end of the block");

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
