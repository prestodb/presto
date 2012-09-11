package com.facebook.presto.block.cursor;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.UncompressedValueBlock;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.NoSuchElementException;

import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;
import static com.google.common.base.Preconditions.checkNotNull;

public class UncompressedSliceBlockCursor
        implements BlockCursor
{
    private static final TupleInfo INFO = new TupleInfo(VARIABLE_BINARY);

    private Slice slice;
    private Range range;
    private long position = -1;
    private int offset = -1;
    private int size = -1;


    public UncompressedSliceBlockCursor(UncompressedValueBlock block)
    {
        this(checkNotNull(block, "block is null").getSlice(), block.getRange());
    }

    public UncompressedSliceBlockCursor(Slice slice, Range range)
    {
        this.slice = slice;
        this.range = range;
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public void moveTo(BlockCursor newPosition)
    {
        Preconditions.checkArgument(newPosition instanceof UncompressedSliceBlockCursor, "cursor is not an instance of UncompressedSliceBlockCursor");
        UncompressedSliceBlockCursor other = (UncompressedSliceBlockCursor) newPosition;

        // todo assure that the cursors are for the same block stream?

        this.slice = other.slice;
        this.range = other.range;
        this.position = other.position;
        this.offset = other.offset;
        this.size = other.size;
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
        size = slice.getShort(offset);
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
            size = slice.getShort(0);
        }

        // advance to specified position
        while (position < newPosition) {
            position++;
            offset += size;
            size = slice.getShort(offset);
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
        return new Tuple(slice.slice(offset, size), INFO);
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        Preconditions.checkElementIndex(0, 1, "field");
        return slice.slice(offset + SIZE_OF_SHORT, size - SIZE_OF_SHORT);
    }

    @Override
    public boolean tupleEquals(Tuple value)
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        Slice tupleSlice = value.getTupleSlice();
        return slice.equals(offset, size, tupleSlice, 0, tupleSlice.length());
    }
}
