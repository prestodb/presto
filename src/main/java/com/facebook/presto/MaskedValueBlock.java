/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;

import java.util.Iterator;

import static com.facebook.presto.Pair.valueGetter;
import static com.google.common.collect.Iterators.transform;

public class MaskedValueBlock implements ValueBlock
{
    public static ValueBlock maskBlock(ValueBlock valueBlock, PositionBlock positions)
    {
        if (positions.isEmpty()) {
            return EmptyValueBlock.INSTANCE;
        }

        Range<Long> intersection = valueBlock.getRange().intersection(positions.getRange());
        if (intersection.isEmpty()) {
            return EmptyValueBlock.INSTANCE;
        }

        if (valueBlock.isSingleValue() && positions.isPositionsContiguous()) {
            Tuple value = valueBlock.iterator().next();
            return new RunLengthEncodedBlock(value, intersection);
        }

        PositionBlock newPositionBlock;
        if (valueBlock.isPositionsContiguous()) {
            newPositionBlock = positions.filter(new RangePositionBlock(valueBlock.getRange()));
        }
        else {
            newPositionBlock = positions.filter(valueBlock.toPositionBlock());
        }

        return new MaskedValueBlock(valueBlock, newPositionBlock);
    }

    private final ValueBlock valueBlock;
    private final PositionBlock positionBlock;

    private MaskedValueBlock(ValueBlock valueBlock, PositionBlock positionBlock)
    {
        this.valueBlock = valueBlock;
        this.positionBlock = positionBlock;
    }

    @Override
    public PositionBlock selectPositions(Predicate<Tuple> predicate)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValueBlock selectPairs(Predicate<Tuple> predicate)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public PositionBlock toPositionBlock()
    {
        return positionBlock;
    }

    @Override
    public ValueBlock filter(PositionBlock positions)
    {
        if (positions.isEmpty()) {
            return EmptyValueBlock.INSTANCE;
        }

        PositionBlock activePositions = positionBlock.filter(positions);
        if (activePositions.isEmpty()) {
            return EmptyValueBlock.INSTANCE;
        }
        return new MaskedValueBlock(valueBlock, activePositions);
    }

    @Override
    public PeekingIterator<Pair> pairIterator()
    {
        return Iterators.peekingIterator(Iterators.filter(valueBlock.pairIterator(), new Predicate<Pair>()
        {
            @Override
            public boolean apply(Pair pair)
            {
                return positionBlock.apply(pair.getPosition());
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
        return positionBlock.getCount();
    }

    @Override
    public boolean isSorted()
    {
        return valueBlock.isSorted();
    }

    @Override
    public boolean isSingleValue()
    {
        return valueBlock.isSingleValue();
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return positionBlock.isPositionsContiguous() && valueBlock.isPositionsContiguous();
    }

    @Override
    public Iterable<Long> getPositions()
    {
        return positionBlock.getPositions();
    }

    @Override
    public Range<Long> getRange()
    {
        return positionBlock.getRange();
    }

    @Override
    public Iterator<Tuple> iterator()
    {
        return transform(Iterators.filter(valueBlock.pairIterator(), new Predicate<Pair>()
        {
            @Override
            public boolean apply(Pair pair)
            {
                return positionBlock.apply(pair.getPosition());
            }
        }), valueGetter());
    }
}
