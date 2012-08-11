package com.facebook.presto;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import java.util.Iterator;

public class EmptyValueBlock
    implements ValueBlock
{
    @Override
    public PositionBlock selectPositions(Predicate<Object> predicate)
    {
        return new EmptyPositionBlock();
    }

    @Override
    public ValueBlock selectPairs(Predicate<Object> predicate)
    {
        return this;
    }

    @Override
    public ValueBlock filter(PositionBlock positions)
    {
        return this;
    }

    @Override
    public PeekingIterator<Pair> pairIterator()
    {
        return Iterators.peekingIterator(Iterators.<Pair>emptyIterator());
    }

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
    public Iterator<Object> iterator()
    {
        return Iterators.emptyIterator();
    }
}
