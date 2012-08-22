package com.facebook.presto;

import com.google.common.base.Function;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.ImmutableList;
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
    public PositionBlock filter(PositionBlock positionBlock) {
        if (positionBlock.isEmpty()) {
            return positionBlock;
        }

        if (positionBlock.isPositionsContiguous()) {
            Range<Long> intersection = range.intersection(positionBlock.getRange());
            if (intersection.isEmpty()) {
                return EmptyPositionBlock.INSTANCE;
            }
            return new RangePositionBlock(intersection);
        }

        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        // todo optimize when bit vector position is added
        for (Long position : positionBlock.getPositions()) {
            if (this.apply(position)) {
                builder.add(position);
            }
        }
        if (positionBlock.isEmpty()) {
            return EmptyPositionBlock.INSTANCE;
        }
        return new UncompressedPositionBlock(builder.build());
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
