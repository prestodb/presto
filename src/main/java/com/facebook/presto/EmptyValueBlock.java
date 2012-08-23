package com.facebook.presto;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;



import java.util.Iterator;

public final class EmptyValueBlock
        implements ValueBlock
{
    public final static EmptyValueBlock INSTANCE = new EmptyValueBlock();

    private EmptyValueBlock()
    {
    }

    @Override
    public PositionBlock selectPositions(Predicate<Tuple> predicate)
    {
        return EmptyPositionBlock.INSTANCE;
    }

    @Override
    public ValueBlock selectPairs(Predicate<Tuple> predicate)
    {
        return this;
    }

    @Override
    public PositionBlock toPositionBlock()
    {
        return EmptyPositionBlock.INSTANCE;
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
    public Range getRange()
    {
        throw new UnsupportedOperationException("Empty block doesn't have a range");
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return Iterators.emptyIterator();
    }
}
