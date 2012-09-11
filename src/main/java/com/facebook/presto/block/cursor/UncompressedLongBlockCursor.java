package com.facebook.presto.block.cursor;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.UncompressedValueBlock;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;
import static com.google.common.base.Preconditions.checkNotNull;

public class UncompressedLongBlockCursor
        implements BlockCursor
{
    private static final TupleInfo INFO = new TupleInfo(FIXED_INT_64);

    private Slice slice;
    private Range range;
    private long position = -1;
    private int offset = -1;

    public UncompressedLongBlockCursor(UncompressedValueBlock block)
    {
        this(checkNotNull(block, "block is null").getSlice(), block.getRange());
    }

    public UncompressedLongBlockCursor(Slice slice, Range range)
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
        Preconditions.checkArgument(newPosition instanceof UncompressedLongBlockCursor, "cursor is not an instance of UncompressedLongBlockCursor");
        UncompressedLongBlockCursor other = (UncompressedLongBlockCursor) newPosition;

        // todo assure that the cursors are for the same block stream?

        this.slice = other.slice;
        this.range = other.range;
        this.position = other.position;
        this.offset = other.offset;
    }

    @Override
    public boolean advanceToNextValue()
    {
        // every position is a new value
        if (!(range.getEnd() > position)) {
            return false;
        }

        if (position < 0) {
            position = range.getStart();
            offset = 0;
        } else {
            position++;
            offset += SIZE_OF_LONG;
        }
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

        // advance to specified position
        position = newPosition;
        offset = (int) ((position - this.range.getStart()) * 8);
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
        return new Tuple(slice.slice(offset, SIZE_OF_LONG), INFO);
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        Preconditions.checkElementIndex(0, 1, "field");
        return slice.getLong(offset);
    }

    @Override
    public Slice getSlice(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tupleEquals(Tuple value)
    {
        Preconditions.checkState(position >= 0, "Need to call advanceNext() first");
        Slice tupleSlice = value.getTupleSlice();
        return tupleSlice.length() == SIZE_OF_LONG && slice.getLong(offset) == tupleSlice.getLong(0);
    }
}
