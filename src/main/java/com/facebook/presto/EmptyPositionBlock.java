package com.facebook.presto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import javax.annotation.Nullable;

public class EmptyPositionBlock
    implements PositionBlock
{
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
    public Range<Long> getRange()
    {
        return Ranges.open(0L, 0L);
    }

    @Override
    public boolean apply(@Nullable Long input)
    {
        return false;
    }
}
