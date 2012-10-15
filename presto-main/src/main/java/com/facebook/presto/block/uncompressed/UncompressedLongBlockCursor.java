package com.facebook.presto.block.uncompressed;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.block.Cursors;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;
import static com.google.common.base.Preconditions.checkNotNull;

public class UncompressedLongBlockCursor
        implements Cursor
{
    private final TupleInfo tupleInfo;
    private final Slice slice;
    private final Range range;
    private long position = -1;
    private int offset = -1;

    public UncompressedLongBlockCursor(UncompressedBlock block)
    {
        this(checkNotNull(block, "block is null").getTupleInfo(), block.getSlice(), block.getRange());
    }

    public UncompressedLongBlockCursor(TupleInfo tupleInfo, Slice slice, Range range)
    {
        this.tupleInfo = tupleInfo;
        this.slice = slice;
        this.range = range;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
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

    @Override
    public AdvanceResult advanceNextValue()
    {
        if (position >= range.getEnd()) {
            position = Long.MAX_VALUE;
            return FINISHED;
        }

        if (position < 0) {
            position = range.getStart();
            offset = 0;
        } else {
            position++;
            offset += SIZE_OF_LONG;
        }
        return SUCCESS;
    }

    @Override
    public AdvanceResult advanceNextPosition()
    {
        // every position is a new value
        return advanceNextValue();
    }

    @Override
    public AdvanceResult advanceToPosition(long newPosition)
    {
        // if new position is out of range, return false
        if (newPosition > range.getEnd()) {
            position = Long.MAX_VALUE;
            return FINISHED;
        }

        Preconditions.checkArgument(newPosition >= this.position, "Can't advance backwards");

        // advance to specified position
        position = newPosition;

        // adjust offset
        offset = (int) ((position - this.range.getStart()) * SIZE_OF_LONG);
        return SUCCESS;
    }

    @Override
    public long getPosition()
    {
        Cursors.checkReadablePosition(this);
        return position;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Cursors.checkReadablePosition(this);
        return position;
    }

    @Override
    public Tuple getTuple()
    {
        Cursors.checkReadablePosition(this);
        return new Tuple(slice.slice(offset, SIZE_OF_LONG), TupleInfo.SINGLE_LONG);
    }

    @Override
    public long getLong(int field)
    {
        Cursors.checkReadablePosition(this);
        Preconditions.checkElementIndex(0, 1, "field");
        return slice.getLong(offset);
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
    public boolean currentTupleEquals(Tuple value)
    {
        Cursors.checkReadablePosition(this);
        Slice tupleSlice = value.getTupleSlice();
        return tupleSlice.length() == SIZE_OF_LONG && slice.getLong(offset) == tupleSlice.getLong(0);
    }
}
