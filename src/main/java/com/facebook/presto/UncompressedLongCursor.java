package com.facebook.presto;

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

    private final Iterator<UncompressedValueBlock> iterator;

    //
    // Current value and position of the cursor
    // If cursor before the first element, these will be null and -1
    //
    private UncompressedValueBlock blockForCurrentValue;
    private int currentBlockIndex = -1;
    private int currentOffset = -1;

    //
    // Next value and position of the cursor
    // If the cursor is within the middle of a block, the currentBlock
    // and nextBlock will point to the same object
    // If cursor is at the end, these will be null and -1
    //
    private UncompressedValueBlock blockForNextValue;
    private int nextBlockIndex;
    private int nextOffset;

    public UncompressedLongCursor(Iterator<UncompressedValueBlock> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        this.iterator = iterator;

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

        if (blockForNextValue != null && nextBlockIndex < blockForNextValue.getCount() - 1) {
            // next value is within the current block
            nextBlockIndex++;
            nextOffset += SIZE_OF_LONG;
        }
        else {
            // next value is within the next block
            moveToNextValue();
        }
    }

    private void moveToNextValue()
    {
        if (iterator.hasNext()) {
            // advance to next block
            blockForNextValue = iterator.next();
            nextBlockIndex = 0;
            nextOffset = 0;
        }
        else {
            // no more data
            blockForNextValue = null;
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
        Preconditions.checkState(blockForCurrentValue != null, "Need to call advanceNext() first");
        return new Tuple(blockForCurrentValue.getSlice().slice(currentOffset, SizeOf.SIZE_OF_LONG), INFO);
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkState(blockForCurrentValue != null, "Need to call advanceNext() first");
        Preconditions.checkElementIndex(0, 1, "field");
        return blockForCurrentValue.getSlice().getLong(currentOffset);
    }

    @Override
    public Slice getSlice(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPosition()
    {
        Preconditions.checkState(blockForCurrentValue != null, "Need to call advanceNext() first");
        return blockForCurrentValue.getRange().getStart() + currentBlockIndex;
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
        return tupleSlice.length() == SIZE_OF_LONG && blockForCurrentValue.getSlice().getLong(currentOffset) == tupleSlice.getLong(0);
    }

    @Override
    public boolean nextValueEquals(Tuple value)
    {
        if (blockForNextValue == null) {
            throw new NoSuchElementException();
        }
        Slice tupleSlice = value.getTupleSlice();
        return tupleSlice.length() == SIZE_OF_LONG && blockForNextValue.getSlice().getLong(nextOffset) == tupleSlice.getLong(0);
    }
}
