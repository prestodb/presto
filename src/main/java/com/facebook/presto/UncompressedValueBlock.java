package com.facebook.presto;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class UncompressedValueBlock
        implements ValueBlock
{
    private final Range<Long> range;
    private final TupleInfo tupleInfo;
    private final Slice slice;

    public UncompressedValueBlock(Range<Long> range, TupleInfo tupleInfo, Slice slice)
    {
        Preconditions.checkNotNull(range, "range is null");
        Preconditions.checkArgument(range.lowerEndpoint() >= 0, "range start position is negative");
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        Preconditions.checkNotNull(slice, "data is null");

        this.tupleInfo = tupleInfo;
        this.slice = slice;
        this.range = range;
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
        // find selected positions
        Set<Integer> indexes = new HashSet<>();
        for (long position : positions.getPositions()) {
            if (range.contains(position)) {
                indexes.add((int) (position - range.lowerEndpoint()));
            }
        }

        // if no positions are selected, we are done
        if (indexes.isEmpty()) {
            return EmptyValueBlock.INSTANCE;
        }


        // build a buffer containing only the tuples from the selected positions
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);

        int currentOffset = 0;
        for (int index = 0; index < getCount(); ++index) {
            Slice currentPositionToEnd = slice.slice(currentOffset, slice.length() - currentOffset);
            int size = tupleInfo.size(currentPositionToEnd);

            // only write selected tuples
            if (indexes.contains(index)) {
                sliceOutput.writeBytes(slice, currentOffset, size);
            }

            currentOffset += size;
        }

        // todo what is the start position
        return new UncompressedValueBlock(Ranges.closed(0L, (long) indexes.size() - 1), tupleInfo, sliceOutput.slice());
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
