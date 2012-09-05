package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;

public class UncompressedLongCursor
        implements Cursor
{
    private static final TupleInfo INFO = new TupleInfo(FIXED_INT_64);

    private final PeekingIterator<UncompressedValueBlock> iterator;

    private UncompressedValueBlock currentBlock;
    private int currentBlockIndex;
    private int currentOffset;

    private UncompressedValueBlock nextBlock;
    private int nextBlockIndex;
    private int nextOffset;

    public UncompressedLongCursor(Iterator<UncompressedValueBlock> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        this.iterator = Iterators.peekingIterator(iterator);

        moveToNextValue();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return INFO;
    }

    @Override
    public boolean hasNextValue()
    {
        return nextBlock != null;
    }

    @Override
    public void advanceNextValue()
    {
        if (nextBlock == null) {
            throw new NoSuchElementException();
        }

        currentBlock = nextBlock;
        currentBlockIndex = nextBlockIndex;
        currentOffset = nextOffset;

        if (nextBlock != null && nextBlockIndex < nextBlock.getCount() - 1) {
            // next value is within the current block
            nextBlock = currentBlock;
            nextBlockIndex++;
            nextOffset += SIZE_OF_LONG;
        }
        else {
            // next value is within the next block

            // consume current block
            iterator.next();

            moveToNextValue();
        }
    }

    private void moveToNextValue()
    {
        if (iterator.hasNext()) {
            // advance to next block
            nextBlock = iterator.peek();
            nextBlockIndex = 0;
            nextOffset = 0;
        }
        else {
            // no more data
            nextBlock = null;
            nextBlockIndex = -1;
            nextOffset = -1;
        }
    }

    @Override
    public boolean hasNextPosition()
    {
        return hasNextValue();
    }

    @Override
    public void advanceNextPosition()
    {
        advanceNextValue();
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(currentBlock != null, "Need to call advanceNext() first");
        return new Tuple(currentBlock.getSlice().slice(currentOffset, SizeOf.SIZE_OF_LONG), INFO);
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(currentBlock != null, "Need to call advanceNext() first");
        Preconditions.checkElementIndex(0, 1, "field");
        return currentBlock.getSlice().getLong(currentOffset);
    }

    @Override
    public Slice getSlice(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Cursor other)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long getPosition()
    {
        Preconditions.checkState(currentBlock != null, "Need to call advanceNext() first");
        return currentBlock.getRange().getStart() + currentBlockIndex;
    }

    @Override
    public long peekNextValuePosition()
    {
        if (nextBlock == null) {
            throw new NoSuchElementException();
        }
        return nextBlock.getRange().getStart() + nextBlockIndex;
    }

    @Override
    public boolean equals(Tuple value)
    {
        Preconditions.checkState(currentBlock != null, "Need to call advanceNext() first");
        Slice tupleSlice = value.getTupleSlice();
        return tupleSlice.length() == SIZE_OF_LONG && currentBlock.getSlice().getLong(currentOffset) == tupleSlice.getLong(0);
    }

    @Override
    public boolean equals(int field, Slice value)
    {
        if (currentBlock == null) {
            throw new NoSuchElementException();
        }
        Preconditions.checkElementIndex(0, 1, "field");
        return value.length() == SIZE_OF_LONG && currentBlock.getSlice().getLong(currentOffset) == value.getLong(0);
    }

    @Override
    public boolean nextValueEquals(Tuple value)
    {
        if (nextBlock == null) {
            throw new NoSuchElementException();
        }
        Slice tupleSlice = value.getTupleSlice();
        return tupleSlice.length() == SIZE_OF_LONG && nextBlock.getSlice().getLong(nextOffset) == tupleSlice.getLong(0);
    }
}
