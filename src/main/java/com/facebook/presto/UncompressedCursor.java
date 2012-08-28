package com.facebook.presto;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class UncompressedCursor
        implements Cursor
{
    private final Iterator<UncompressedValueBlock> iterator;
    private final TupleInfo info;

    private UncompressedValueBlock currentBlock;
    private int index;
    private int offset;

    public UncompressedCursor(TupleInfo info, Iterator<UncompressedValueBlock> iterator)
    {
        Preconditions.checkNotNull(iterator, "iterator is null");
        Preconditions.checkNotNull(info, "info is null");

        this.info = info;
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
            offset += info.size(currentBlock.getSlice(), offset);
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
    public long getLong(int field)
    {
        return info.getLong(currentBlock.getSlice(), offset, field);
    }

    @Override
    public Slice getSlice(int field)
    {
        return info.getSlice(currentBlock.getSlice(), offset, field);
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
    public boolean equals(int field, Slice value)
    {
        return info.equals(field, currentBlock.getSlice(), offset, value);
    }
}
