package com.facebook.presto;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.facebook.presto.SizeOf.SIZE_OF_LONG;
import static com.facebook.presto.TupleInfo.Type.FIXED_INT_64;

public class UncompressedLongCursor
        implements Cursor
{
    private static final TupleInfo info = new TupleInfo(FIXED_INT_64);

    private final Iterator<UncompressedValueBlock> iterator;

    private UncompressedValueBlock currentBlock;
    private int index;
    private int offset;

    public UncompressedLongCursor(Iterator<UncompressedValueBlock> iterator)
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
            offset += SIZE_OF_LONG;
        }
        else {
            throw new NoSuchElementException();
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
        return new Tuple(currentBlock.getSlice().slice(offset, SizeOf.SIZE_OF_LONG), info);
    }

    @Override
    public long getLong(int field)
    {
        Preconditions.checkElementIndex(0, 1, "field");
        return currentBlock.getSlice().getLong(offset);
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
        return currentBlock.getRange().getStart() + index;
    }

    @Override
    public boolean equals(Tuple value)
    {
        Slice tupleSlice = value.getTupleSlice();
        return tupleSlice.length() == SIZE_OF_LONG && currentBlock.getSlice().getLong(offset) == tupleSlice.getLong(0);
    }

    @Override
    public boolean equals(int field, Slice value)
    {
        Preconditions.checkElementIndex(0, 1, "field");
        return value.length() == SIZE_OF_LONG && currentBlock.getSlice().getLong(offset) == value.getLong(0);
    }
}
