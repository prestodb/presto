package com.facebook.presto.block.uncompressed;

import com.facebook.presto.Range;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.facebook.presto.block.Cursor.AdvanceResult.FINISHED;
import static com.facebook.presto.block.Cursor.AdvanceResult.SUCCESS;

public class UncompressedCursor
        implements Cursor
{
    private final Iterator<UncompressedBlock> iterator;
    private final TupleInfo info;

    private UncompressedBlock block;
    private int index = -1;
    private int offset = -1;
    private int size = -1;

    public UncompressedCursor(TupleInfo info, Iterator<UncompressedBlock> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        Preconditions.checkNotNull(info, "info is null");

        this.info = info;
        this.iterator = iterator;

        if (iterator.hasNext()) {
            block = iterator.next();
        } else {
            // already finished
            block = null;
        }
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public boolean isValid()
    {
        return index >= 0 && block != null;
    }

    @Override
    public boolean isFinished()
    {
        return block == null;
    }

    @Override
    public AdvanceResult advanceNextValue()
    {
        if (block == null) {
            return FINISHED;
        }

        if (index < 0) {
            // next value is within the current block
            index = 0;
            offset = 0;
            size = info.size(block.getSlice(), offset);
            return SUCCESS;
        }
        else if (index < block.getCount() - 1) {
            // next value is within the current block
            index++;
            offset += size;
            size = info.size(block.getSlice(), offset);
            return SUCCESS;
        }
        else if (iterator.hasNext()) {
            // next value is within the next block
            // advance to next block
            block = iterator.next();
            index = 0;
            offset = 0;
            size = info.size(block.getSlice(), offset);
            return SUCCESS;
        }
        else {
            // no more data
            block = null;
            index = -1;
            offset = -1;
            size = -1;
            return FINISHED;
        }
    }

    @Override
    public AdvanceResult advanceNextPosition()
    {
        return advanceNextValue();
    }

    @Override
    public AdvanceResult advanceToPosition(long newPosition)
    {
        Preconditions.checkArgument(index < 0 || newPosition >= getPosition(), "Can't advance backwards");

        if (block == null) {
            return FINISHED;
        }

        if (index >= 0 && newPosition == getPosition()) {
            // position to current position? => no op
            return SUCCESS;
        }

        // skip to block containing requested position
        if (index < 0 || newPosition > block.getRange().getEnd()) {
            while (newPosition > block.getRange().getEnd() && iterator.hasNext()) {
                block = iterator.next();
            }

            // is the position off the end of the stream?
            if (newPosition > block.getRange().getEnd()) {
                block = null;
                index = -1;
                offset = -1;
                size = -1;
                return FINISHED;
            }

            // point to first entry in the block we skipped to
            index = 0;
            offset = 0;
            size = info.size(block.getSlice(), offset);
        }

        // skip to index within block
        while (block.getRange().getStart() + index < newPosition) {
            index++;
            offset += size;
            size = info.size(block.getSlice(), offset);
        }

        return SUCCESS;
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null)  {
            throw new NoSuchElementException();
        }
        Slice slice = block.getSlice();
        return new Tuple(slice.slice(offset, size), info);
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null)  {
            throw new NoSuchElementException();
        }
        return info.getLong(block.getSlice(), offset, field);
    }

    @Override
    public double getDouble(int field)
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null)  {
            throw new NoSuchElementException();
        }
        return info.getDouble(block.getSlice(), offset, field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null)  {
            throw new NoSuchElementException();
        }
        return info.getSlice(block.getSlice(), offset, field);
    }

    @Override
    public long getPosition()
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null)  {
            throw new NoSuchElementException();
        }
        return block.getRange().getStart() + index;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        return getPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null)  {
            throw new NoSuchElementException();
        }
        Slice tupleSlice = value.getTupleSlice();
        return block.getSlice().equals(offset, size, tupleSlice, 0, tupleSlice.length());
    }
}
