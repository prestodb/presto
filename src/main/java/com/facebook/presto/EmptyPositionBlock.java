package com.facebook.presto;

import com.google.common.collect.ImmutableList;



import javax.annotation.Nullable;

public final class EmptyPositionBlock
    implements PositionBlock
{
    public static final EmptyPositionBlock INSTANCE = new EmptyPositionBlock();

    private EmptyPositionBlock()
    {
    }

    public PositionBlock filter(PositionBlock positionBlock) {
        return this;
    }

    @Override
    public boolean isEmpty()
    {
        return true;
    }

    @Override
    public int getCount()
    {
        return 0;
    }

    @Override
    public boolean isSorted()
    {
        return false;
    }

    @Override
    public boolean isSingleValue()
    {
        return false;
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return false;
    }

    @Override
    public Iterable<Long> getPositions()
    {
        return ImmutableList.of();
    }

    @Override
    public Range getRange()
    {
        throw new UnsupportedOperationException("Empty block doesn't have a range");
    }

    @Override
    public boolean apply(@Nullable Long input)
    {
        return false;
    }
}
