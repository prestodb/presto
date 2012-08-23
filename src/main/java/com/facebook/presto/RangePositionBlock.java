package com.facebook.presto;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

public class RangePositionBlock
        implements PositionBlock
{
    private final Range range;

    public RangePositionBlock(Range range)
    {
        this.range = range;
    }

    @Override
    public PositionBlock filter(PositionBlock positionBlock) {
        if (positionBlock.isEmpty()) {
            return positionBlock;
        }

        if (positionBlock.isPositionsContiguous()) {
            if (!range.overlaps(positionBlock.getRange())) {
                return EmptyPositionBlock.INSTANCE;
            }

            return new RangePositionBlock(range.intersect(positionBlock.getRange()));
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
        return false;
    }

    @Override
    public int getCount()
    {
        return (int) range.length();
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
        return range;
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public boolean apply(Long input)
    {
        return range.contains(input);
    }

    @Override
    public String toString()
    {
        return String.format("[%s..%s]", range.getStart(), range.getEnd());
    }

    public static Function<RangePositionBlock, Range> rangeGetter()
    {
        return new Function<RangePositionBlock, Range>()
        {
            @Override
            public Range apply(RangePositionBlock input)
            {
                return input.getRange();
            }
        };
    }

}
