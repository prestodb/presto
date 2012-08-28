package com.facebook.presto;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.facebook.presto.SizeOf.SIZE_OF_SHORT;
import static com.facebook.presto.TupleInfo.Type.VARIABLE_BINARY;

public class UncompressedSliceCursor
        implements Cursor
{
    private static final TupleInfo info = new TupleInfo(VARIABLE_BINARY);

    private final Iterator<UncompressedValueBlock> iterator;

    private UncompressedValueBlock currentBlock;
    private int index;
    private int offset;
    private int currentSize;

    public UncompressedSliceCursor(Iterator<UncompressedValueBlock> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        this.iterator = iterator;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public boolean hasNextValue()
    {
        return (currentBlock != null && index < currentBlock.getCount() - 1) || iterator.hasNext();
    }

    @Override
    public void advanceNextValue()
    {
        if (currentBlock == null || index >= currentBlock.getCount() - 1) {
            currentBlock = iterator.next();
            index = 0;
            offset = 0;
        }
        else if (index < currentBlock.getCount() - 1) {
            index++;
            offset += currentSize;
        }
        else {
            throw new NoSuchElementException();
        }

        // read the value size
        currentSize = currentBlock.getSlice().getShort(offset) - SIZE_OF_SHORT;
        offset += SIZE_OF_SHORT;
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
        // full tuple slice includes the size (prior two bytes)
        return new Tuple(currentBlock.getSlice().slice(offset - SIZE_OF_SHORT, currentSize + SIZE_OF_SHORT), info);
    }

    @Override
    public long getLong(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Slice getSlice(int field)
    {
        Preconditions.checkElementIndex(0, 1, "field");
        return currentBlock.getSlice().slice(offset, currentSize);
    }

    @Override
    public boolean equals(Cursor other)
    {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long getPosition()
    {
        return currentBlock.getRange().getStart() + index;
    }

    @Override
    public boolean equals(Tuple value)
    {
        Slice tupleSlice = value.getTupleSlice();
        return currentBlock.getSlice().equals(offset, currentSize, tupleSlice, SIZE_OF_SHORT, tupleSlice.length() - SIZE_OF_SHORT);
    }

    @Override
    public boolean equals(int field, Slice value)
    {
        Preconditions.checkElementIndex(0, 1, "field");
        return currentBlock.getSlice().equals(offset, currentSize, value, 0, value.length());
    }
}
