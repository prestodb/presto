package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;

public class UncompressedSliceCursor
        implements Cursor
{
    private static final TupleInfo INFO = new TupleInfo(VARIABLE_BINARY);

    private final Iterator<UncompressedValueBlock> iterator;

    private UncompressedValueBlock block;
    private int index = -1;
    private int offset = -1;
    private int size = -1;

    public UncompressedSliceCursor(Iterator<UncompressedValueBlock> iterator)
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
            size = block.getSlice().getShort(offset);
            return true;
        }
        else if (index < block.getCount() - 1) {
            // next value is within the current block
            index++;
            offset += size;
            size = block.getSlice().getShort(offset);
            return true;
        }
        else if (iterator.hasNext()) {
            // next value is within the next block
            // advance to next block
            block = iterator.next();
            index = 0;
            offset = 0;
            size = block.getSlice().getShort(offset);
            return true;
        }
        else {
            // no more data
            block = null;
            index = Integer.MAX_VALUE;
            offset = -1;
            size = -1;
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
                index = Integer.MAX_VALUE;
                offset = -1;
                size = -1;
                return false;
            }

            // point to first entry in the block we skipped to
            index = 0;
            offset = 0;
            size = block.getSlice().getShort(offset);
        }

        // skip to index within block
        while (block.getRange().getStart() + index < newPosition) {
            index++;
            offset += size;
            size = block.getSlice().getShort(offset);
        }

        return true;
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null) {
            throw new NoSuchElementException();
        }
        return new Tuple(block.getSlice().slice(offset, size), INFO);
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null) {
            throw new NoSuchElementException();
        }
        Preconditions.checkElementIndex(0, 1, "field");
        return block.getSlice().slice(offset + SIZE_OF_SHORT, size - SIZE_OF_SHORT);
    }

    @Override
    public long getPosition()
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null) {
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
    public boolean currentValueEquals(Tuple value)
    {
        Preconditions.checkState(index >= 0, "Need to call advanceNext() first");
        if (block == null) {
            throw new NoSuchElementException();
        }
        Slice tupleSlice = value.getTupleSlice();
        return block.getSlice().equals(offset, size, tupleSlice, 0, tupleSlice.length());
    }
}
