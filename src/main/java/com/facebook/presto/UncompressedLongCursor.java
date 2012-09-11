package com.facebook.presto;

import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

import java.util.Iterator;

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
    public boolean advanceNextValue()
    {
        if (blockForNextValue == null) {
            return false;
        }

        blockForCurrentValue = blockForNextValue;
        currentBlockIndex = nextBlockIndex;
        currentOffset = nextOffset;

        if (nextBlockIndex < blockForNextValue.getCount() - 1) {
            // next value is within the current block
            nextBlockIndex++;
            nextOffset += SIZE_OF_LONG;
        }
        else {
            // next value is within the next block
            moveToNextBlock();
        }
        return true;
    }

    private void moveToNextBlock()
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
    public boolean advanceNextPosition()
    {
        return advanceNextValue();
    }

    @Override
    public boolean advanceToPosition(long position)
    {
        Preconditions.checkArgument(blockForCurrentValue == null || position >= getPosition(), "Can't advance backwards");

        if (blockForCurrentValue != null && position == getPosition()) {
            // position to current position? => no op
            return true;
        }

        if (blockForNextValue == null) {
            return false;
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
        }

        // skip to index within block
        while (blockForNextValue.getRange().getStart() + nextBlockIndex < position) {
            nextBlockIndex++;
            nextOffset += SIZE_OF_LONG;
        }

        // adjust current and next pointers
        blockForCurrentValue = blockForNextValue;
        currentBlockIndex = nextBlockIndex;
        currentOffset = nextOffset;

        // adjust next block
        if (nextBlockIndex < blockForNextValue.getCount() - 1) {
            // next value is within the current block
            nextBlockIndex++;
            nextOffset = currentOffset + SIZE_OF_LONG;
        }
        else {
            // next value is within the next block
            moveToNextBlock();
        }
        return true;
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
    public long getCurrentValueEndPosition()
    {
        return getPosition();
    }

    @Override
    public boolean currentValueEquals(Tuple value)
    {
        Preconditions.checkState(blockForCurrentValue != null, "Need to call advanceNext() first");
        Slice tupleSlice = value.getTupleSlice();
        return tupleSlice.length() == SIZE_OF_LONG && blockForCurrentValue.getSlice().getLong(currentOffset) == tupleSlice.getLong(0);
    }

}
