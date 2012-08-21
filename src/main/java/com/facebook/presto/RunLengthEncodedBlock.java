package com.facebook.presto;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import java.util.Collections;
import java.util.Iterator;

public class RunLengthEncodedBlock
        implements ValueBlock
{
    private final Tuple value;
    private final Range<Long> range;

    public RunLengthEncodedBlock(Tuple value, Range<Long> range)
    {
        this.value = value;
        this.range = range;
    }

    public Tuple getValue()
    {
        return value;
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

    @Override
    public ValueBlock filter(PositionBlock positions)
    {
        // todo this is wrong, it should produce either another RLE block or a BitVectorBlock
        int matches = 0;
        for (long position : positions.getPositions()) {
            if (range.contains(position)) {
                matches++;
            }
        }
        if (matches == 0) {
            return EmptyValueBlock.INSTANCE;
        }


        Slice newSlice = Slices.allocate(matches * value.size());
        SliceOutput sliceOutput = newSlice.output();
        for (int i = 0; i < matches; i++) {
            value.writeTo(sliceOutput);
        }

        // todo what is the start position
        return new UncompressedValueBlock(Ranges.closed(0L, (long) matches), value.getTupleInfo(), newSlice);
    }

    @Override
    public PeekingIterator<Pair> pairIterator()
    {
        return Iterators.peekingIterator(Iterators.transform(getPositions().iterator(), new Function<Long, Pair>()
        {
            @Override
            public Pair apply(Long position)
            {
                return new Pair(position, value);
            }
        }));
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return Iterators.peekingIterator(Collections.nCopies(getCount(), value).iterator());
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
        return true;
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

    public String toString()
    {
        return Iterators.toString(pairIterator());
    }
}
