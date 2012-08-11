package com.facebook.presto;

import com.google.common.base.Function;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Range;

import javax.annotation.Nullable;

public class RangePositionBlock
        implements PositionBlock
{
    private final Range<Long> range;

    public RangePositionBlock(Range<Long> range)
    {
        this.range = range;
    }

    @Override
    public boolean isEmpty()
    {
        return range.isEmpty();
    }

    @Override
    public int getCount()
    {
        return (int) (range.upperEndpoint() - range.lowerEndpoint() + 1);
    }

    @Override
    public boolean isSorted()
    {
        return false;
    }

    @Override
    public boolean isSingleValue()
    {
        return getCount() == 1;
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return true;
    }

    @Override
    public Iterable<Long> getPositions()
    {
        return range.asSet(DiscreteDomains.longs());
    }

    @Override
    public Range<Long> getRange()
    {
        return range;
    }

    @Override
    public boolean apply(@Nullable Long input)
    {
        return range.apply(input);
    }

    @Override
    public String toString()
    {
        return String.format("[%s..%s]", range.lowerEndpoint(), range.upperEndpoint());
    }

    public static Function<RangePositionBlock, Range<Long>> rangeGetter()
    {
        return new Function<RangePositionBlock, Range<Long>>()
        {
            @Override
            public Range<Long> apply(RangePositionBlock input)
            {
                return input.getRange();
            }
        };
    }

}
