package com.facebook.presto;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;

import java.util.Collections;
import java.util.Iterator;

public class RunLengthEncodedBlock
    implements ValueBlock
{
    private final Object value;
    private final Range<Long> range;

    public RunLengthEncodedBlock(Object value, Range<Long> range)
    {
        this.value = value;
        this.range = range;
    }

    public Object getValue()
    {
        return value;
    }

    @Override
    public PositionBlock selectPositions(Predicate<Object> predicate)
    {
        return null;
    }

    @Override
    public ValueBlock selectPairs(Predicate<Object> predicate)
    {
        return null;
    }

    @Override
    public ValueBlock filter(PositionBlock positions)
    {
        ImmutableList.Builder<Pair> builder = ImmutableList.builder();
        for (Long position : positions.getPositions()) {
            if (range.contains(position)) {
                builder.add(new Pair(position, value));
            }
        }

        ImmutableList<Pair> pairs = builder.build();
        if (pairs.isEmpty()) {
            return new EmptyValueBlock();
        }

        return new UncompressedValueBlock(pairs);
    }

    @Override
    public PeekingIterator<Pair> pairIterator()
    {
        return Iterators.peekingIterator(Iterators.transform(getPositions().iterator(), new Function<Long, Pair>() {
            @Override
            public Pair apply(Long position)
            {
                return new Pair(position, value);
            }
        }));
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

    @Override
    public Iterator<Object> iterator()
    {
        return Iterators.peekingIterator(Collections.nCopies(getCount(), value).iterator());
    }
}
