package com.facebook.presto.block.uncompressed;

import com.facebook.presto.Range;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.SizeOf;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;

public class UncompressedLongCursor
        implements Cursor
{
    private static final TupleInfo INFO = new TupleInfo(FIXED_INT_64);

    private final Iterator<UncompressedBlock> iterator;

    private UncompressedBlock block;
    private int index = -1;
    private int offset = -1;

    public UncompressedLongCursor(Iterator<UncompressedBlock> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        Preconditions.checkArgument(iterator.hasNext(), "iterator is empty");
        this.iterator = iterator;
        block = iterator.next();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return INFO;
    }

    @Override
    public Range getRange()
    {
        return Range.ALL;
    }

    @Override
    public boolean isFinished()
    {
        return block == null;
    }

    @Override
    public boolean advanceNextValue()
    {
        if (block == null) {
            return false;
        }

        if (index < 0) {
            // next value is within the current block
            index = 0;
            offset = 0;
            return true;
        }
        else if (index < block.getCount() - 1) {
            // next value is within the current block
            index++;
            offset += SIZE_OF_LONG;
        return true;
    }
        else if (iterator.hasNext()) {
            // next value is within the next block
            // advance to next block
            block = iterator.next();
            index = 0;
            offset = 0;
            return true;
        }
        else {
            // no more data
            block = null;
            index = -1;
            offset = -1;
            return false;
        }
    }

    @Override
    public boolean advanceNextPosition()
    {
        return advanceNextValue();
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        Preconditions.checkArgument(index < 0 || newPosition >= getPosition(), "Can't advance backwards");

        if (block == null) {
            return false;
        }

        if (index >= 0 && newPosition == getPosition()) {
            // position to current position? => no op
            return true;
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
                return false;
            }

            // point to first entry in the block we skipped to
            index = 0;
            offset = 0;
        }

        // skip to index within block
        while (block.getRange().getStart() + index < newPosition) {
            index++;
            offset += SIZE_OF_LONG;
        }

        return true;
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null)  {
            throw new NoSuchElementException();
        }
        return new Tuple(block.getSlice().slice(offset, SizeOf.SIZE_OF_LONG), INFO);
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null)  {
            throw new NoSuchElementException();
        }
        Preconditions.checkElementIndex(0, 1, "field");
        return block.getSlice().getLong(offset);
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
        return tupleSlice.length() == SIZE_OF_LONG && block.getSlice().getLong(offset) == tupleSlice.getLong(0);
    }

}
