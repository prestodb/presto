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

    //
    // Current value and position of the cursor
    // If cursor before the first element, these will be null and -1
    //
    private UncompressedValueBlock blockForCurrentValue;
    private int currentBlockIndex = -1;
    private int currentOffset = -1;
    private int currentSize = -1;

    //
    // Next value and position of the cursor
    // If the cursor is within the middle of a block, the currentBlock
    // and nextBlock will point to the same object
    // If cursor is at the end, these will be null and -1
    //
    private UncompressedValueBlock blockForNextValue;
    private int nextBlockIndex;
    private int nextOffset;
    private int nextSize;

    public UncompressedSliceCursor(Iterator<UncompressedValueBlock> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        Preconditions.checkArgument(iterator.hasNext(), "iterator is empty");
        this.iterator = iterator;

        moveToNextBlock();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return INFO;
    }

    @Override
    public boolean hasNextValue()
    {
        return blockForNextValue != null;
    }

    @Override
    public void advanceNextValue()
    {
        if (blockForNextValue == null) {
            throw new NoSuchElementException();
        }

        blockForCurrentValue = blockForNextValue;
        currentBlockIndex = nextBlockIndex;
        currentOffset = nextOffset;
        currentSize = nextSize;

        if (nextBlockIndex < blockForNextValue.getCount() - 1) {
            // next value is within the current block
            nextBlockIndex++;
            nextOffset = currentOffset + currentSize;
            nextSize = blockForNextValue.getSlice().getShort(nextOffset);
        }
        else {
            // next value is within the next block
            moveToNextBlock();
        }
    }

    private void moveToNextBlock()
    {
        if (iterator.hasNext()) {
            // advance to next block
            blockForNextValue = iterator.next();
            nextBlockIndex = 0;
            nextOffset = 0;
            nextSize = blockForNextValue.getSlice().getShort(nextOffset);
        }
        else {
            // no more data
            blockForNextValue = null;
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
    public void advanceToPosition(long position)
    {
        Preconditions.checkArgument(blockForCurrentValue == null || position >= getPosition(), "Can't advance backwards");

        if (blockForCurrentValue != null && position == getPosition()) {
            // position to current position? => no op
            return;
        }

        if (blockForNextValue == null) {
            throw new NoSuchElementException();
        }

        // skip to block containing requested position
        if (position > blockForNextValue.getRange().getEnd()) {
            do {
                blockForNextValue = iterator.next();
            }
            while (position > blockForNextValue.getRange().getEnd());

            // point to first entry in the block we skipped to
            nextBlockIndex = 0;
            nextOffset = 0;
            nextSize = blockForNextValue.getSlice().getShort(nextOffset);
        }

        // skip to index within block
        while (blockForNextValue.getRange().getStart() + nextBlockIndex < position) {
            nextBlockIndex++;
            nextOffset += nextSize;
            nextSize = blockForNextValue.getSlice().getShort(nextOffset);
        }

        // adjust current and next pointers
        blockForCurrentValue = blockForNextValue;
        currentBlockIndex = nextBlockIndex;
        currentOffset = nextOffset;
        currentSize = nextSize;

        // adjust next block
        if (nextBlockIndex < blockForNextValue.getCount() - 1) {
            // next value is within the current block
            nextBlockIndex++;
            nextOffset = currentOffset + currentSize;
            nextSize = blockForNextValue.getSlice().getShort(nextOffset);
        }
        else {
            // next value is within the next block
            moveToNextBlock();
        }
    }

    @Override
    public Tuple getTuple()
    {
        Preconditions.checkState(blockForCurrentValue != null, "Need to call advanceNext() first");
        return new Tuple(blockForCurrentValue.getSlice().slice(currentOffset, currentSize), INFO);
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkState(blockForCurrentValue != null, "Need to call advanceNext() first");
        Preconditions.checkElementIndex(0, 1, "field");
        return blockForCurrentValue.getSlice().slice(currentOffset + SIZE_OF_SHORT, currentSize - SIZE_OF_SHORT);
    }

    @Override
    public long getPosition()
    {
        Preconditions.checkState(blockForCurrentValue != null, "Need to call advanceNext() first");
        return blockForCurrentValue.getRange().getStart() + currentBlockIndex;
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        return getPosition();
    }

    @Override
    public long peekNextValuePosition()
    {
        if (blockForNextValue == null) {
            throw new NoSuchElementException();
        }
        return blockForNextValue.getRange().getStart() + nextBlockIndex;
    }

    @Override
    public boolean currentValueEquals(Tuple value)
    {
        Preconditions.checkState(blockForCurrentValue != null, "Need to call advanceNext() first");
        Slice tupleSlice = value.getTupleSlice();
        return blockForCurrentValue.getSlice().equals(currentOffset, currentSize, tupleSlice, 0, tupleSlice.length());
    }

    @Override
    public boolean nextValueEquals(Tuple value)
    {
        if (blockForNextValue == null) {
            throw new NoSuchElementException();
        }
        Slice tupleSlice = value.getTupleSlice();
        return blockForNextValue.getSlice().equals(nextOffset, nextSize, tupleSlice, 0, tupleSlice.length());
    }
}
