package com.facebook.presto;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;

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
    public PositionBlock toPositionBlock()
    {
        return new RangePositionBlock(range);
    }

    @Override
    public ValueBlock filter(PositionBlock positions)
    {
        return MaskedValueBlock.maskBlock(this, positions);
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
        return true;
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
