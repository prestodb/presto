package com.facebook.presto;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

public class UncompressedValueBlock
        implements ValueBlock
{
    private final Range range;
    private final TupleInfo tupleInfo;
    private final Slice slice;

    public UncompressedValueBlock(Range range, TupleInfo tupleInfo, Slice slice)
    {
        Preconditions.checkNotNull(range, "range is null");
        Preconditions.checkArgument(range.getStart() >= 0, "range start position is negative");
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkNotNull(slice, "data is null");

        this.tupleInfo = tupleInfo;
        this.slice = slice;
        this.range = range;
    }

    @Override
    public Optional<PositionBlock> selectPositions(Predicate<Tuple> predicate)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<ValueBlock> selectPairs(Predicate<Tuple> predicate)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PositionBlock toPositionBlock()
    {
        return new RangePositionBlock(range);
    }

    /**
     * Build a new block with only the selected value positions
     */
    @Override
    public Optional<ValueBlock> filter(PositionBlock positions)
    {
        return MaskedValueBlock.maskBlock(this, positions);
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return new AbstractIterator<Tuple>()
        {
            private int currentOffset = 0;
            private long index = 0;

            @Override
            protected Tuple computeNext()
            {
                if (index >= getCount()) {
                    endOfData();
                    return null;
                }

                Slice currentPositionToEnd = slice.slice(currentOffset, slice.length() - currentOffset);

                int size = tupleInfo.size(currentPositionToEnd);
                index++;
                currentOffset += size;

                Slice row = currentPositionToEnd.slice(0, size);
                return new Tuple(row, tupleInfo);
            }
        };
    }

    @Override
    public PeekingIterator<Pair> pairIterator()
    {
        return Iterators.peekingIterator(new AbstractIterator<Pair>()
        {
            private int currentOffset = 0;
            private long index = 0;

            @Override
            protected Pair computeNext()
            {
                if (index >= getCount()) {
                    endOfData();
                    return null;
                }

                Slice currentPositionToEnd = slice.slice(currentOffset, slice.length() - currentOffset);

                int size = tupleInfo.size(currentPositionToEnd);
                currentOffset += size;

                Slice row = currentPositionToEnd.slice(0, size);

                long position = index + range.getStart();
                index++;
                return new Pair(position, new Tuple(row, tupleInfo));
            }
        });
    }

    @Override
    public int getCount()
    {
        return (int) (range.getEnd() - range.getStart() + 1);
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
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append("FixedWidthValueBlock");
        sb.append("{range=").append(range);
        sb.append(", tupleInfo=").append(tupleInfo);
        sb.append(", slice=").append(slice);
        sb.append('}');
        return sb.toString();
    }
}
