package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class UncompressedCursor
        implements Cursor
{
    private final PeekingIterator<UncompressedValueBlock> iterator;
    private final TupleInfo info;

    private UncompressedValueBlock currentBlock;
    private int currentBlockIndex;
    private int currentOffset;
    private int currentSize;

    private UncompressedValueBlock nextBlock;
    private int nextBlockIndex;
    private int nextOffset;
    private int nextSize;

    public UncompressedCursor(TupleInfo info, Iterator<UncompressedValueBlock> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        Preconditions.checkNotNull(info, "info is null");

        this.info = info;
        this.iterator = Iterators.peekingIterator(iterator);

        moveToNextValue();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
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
        currentSize = nextSize;

        if (nextBlock != null && nextBlockIndex < nextBlock.getCount() - 1) {
            // next value is within the current block
            nextBlock = currentBlock;
            nextBlockIndex++;
            nextOffset = currentOffset + currentSize;
            nextSize = info.size(nextBlock.getSlice(), nextOffset);
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
            nextSize = info.size(nextBlock.getSlice(), nextOffset);
        }
        else {
            // no more data
            nextBlock = null;
            nextBlockIndex = -1;
            nextOffset = -1;
            nextSize = -1;
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
        Slice slice = currentBlock.getSlice();
        return new Tuple(slice.slice(currentOffset, currentSize), info);
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(currentBlock != null, "Need to call advanceNext() first");
        return info.getLong(currentBlock.getSlice(), currentOffset, field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(currentBlock != null, "Need to call advanceNext() first");
        return info.getSlice(currentBlock.getSlice(), currentOffset, field);
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
        return currentBlock.getSlice().equals(currentOffset, currentSize, tupleSlice, 0, tupleSlice.length());
    }

    @Override
    public boolean equals(int field, Slice value)
    {
        if (currentBlock == null) {
            throw new NoSuchElementException();
        }
        return info.equals(field, currentBlock.getSlice(), currentOffset, value);
    }

    @Override
    public boolean nextValueEquals(Tuple value)
    {
        if (nextBlock == null) {
            throw new NoSuchElementException();
        }
        Slice tupleSlice = value.getTupleSlice();
        return nextBlock.getSlice().equals(nextOffset, nextSize, tupleSlice, 0, tupleSlice.length());
    }
}
