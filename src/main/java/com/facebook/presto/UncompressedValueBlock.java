package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UncompressedValueBlock
        implements ValueBlock
{
    private final Range<Long> range;
    private final TupleInfo tupleInfo;
    private final Slice slice;

    public UncompressedValueBlock(long startPosition, TupleInfo tupleInfo, Slice slice)
    {
        Preconditions.checkArgument(startPosition >= 0, "startPosition is negative");
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkNotNull(slice, "data is null");

        this.tupleInfo = tupleInfo;
        this.slice = slice;

        Preconditions.checkArgument(slice.length() % tupleInfo.size() == 0, "data must be a multiple of tuple length");

        int rows = slice.length() / tupleInfo.size();
        range = Ranges.closed(startPosition, startPosition + rows - 1);
    }

    @Override
    public PositionBlock selectPositions(Predicate<Tuple> predicate)
    {
        return null;
    }

    @Override
    public ValueBlock selectPairs(Predicate<Tuple> predicate)
    {
        return null;
    }

    /**
     * Build a new block with only the selected value positions
     */
    @Override
    public ValueBlock filter(PositionBlock positions)
    {
        List<Long> indexes = new ArrayList<>();
        for (long position : positions.getPositions()) {
            if (range.contains(position)) {
                indexes.add(position - range.lowerEndpoint());
            }
        }
        if (indexes.isEmpty()) {
            return new EmptyValueBlock();
        }

        Slice newSlice = Slices.allocate(indexes.size() * tupleInfo.size());
        SliceOutput sliceOutput = newSlice.output();
        for (long index : indexes) {
            sliceOutput.writeBytes(slice, (int) (index * tupleInfo.size()), tupleInfo.size());
        }

        // todo what is the start position
        return new UncompressedValueBlock(0, tupleInfo, newSlice);
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return new AbstractIterator<Tuple>()
        {
            private long index = 0;

            @Override
            protected Tuple computeNext()
            {
                if (index >= getCount()) {
                    endOfData();
                    return null;
                }
                Slice row = slice.slice((int) (index * tupleInfo.size()), tupleInfo.size());
                index++;
                return new Tuple(row, tupleInfo);
            }
        };
    }

    @Override
    public PeekingIterator<Pair> pairIterator()
    {
        return Iterators.peekingIterator(new AbstractIterator<Pair>()
        {
            private long index = 0;

            @Override
            protected Pair computeNext()
            {
                if (index >= getCount()) {
                    endOfData();
                    return null;
                }
                Slice row = slice.slice((int) (index * tupleInfo.size()), tupleInfo.size());
                long position = index + range.lowerEndpoint();
                index++;
                return new Pair(position, new Tuple(row, tupleInfo));
            }
        });
    }

    @Override
    public boolean isEmpty()
    {
        return false;
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
        return false;
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
