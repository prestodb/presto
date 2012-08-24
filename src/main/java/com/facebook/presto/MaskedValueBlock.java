/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

import static com.facebook.presto.Pair.valueGetter;
import static com.google.common.collect.Iterators.transform;

public class MaskedValueBlock implements ValueBlock
{
    public static Optional<ValueBlock> maskBlock(ValueBlock valueBlock, PositionBlock positions)
    {
        if (!valueBlock.getRange().overlaps(positions.getRange())) {
            return Optional.absent();
        }

        Range intersection = valueBlock.getRange().intersect(positions.getRange());

        if (valueBlock.isSingleValue() && positions.isPositionsContiguous()) {
            Tuple value = valueBlock.iterator().next();
            return Optional.<ValueBlock>of(new RunLengthEncodedBlock(value, intersection));
        }

        Optional<PositionBlock> newPositionBlock;
        if (valueBlock.isPositionsContiguous()) {
            newPositionBlock = positions.filter(new RangePositionBlock(valueBlock.getRange()));
        }
        else {
            newPositionBlock = positions.filter(valueBlock.toPositionBlock());
        }

        if (newPositionBlock.isPresent()) {
            return Optional.<ValueBlock>of(new MaskedValueBlock(valueBlock, newPositionBlock.get()));
        }

        return Optional.absent();
    }

    private final ValueBlock valueBlock;
    private final PositionBlock positionBlock;

    private MaskedValueBlock(ValueBlock valueBlock, PositionBlock positionBlock)
    {
        this.valueBlock = valueBlock;
        this.positionBlock = positionBlock;
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
        return positionBlock;
    }

    @Override
    public Optional<ValueBlock> filter(PositionBlock positions)
    {
        Optional<PositionBlock> activePositions = positionBlock.filter(positions);
        if (!activePositions.isPresent()) {
            return Optional.absent();
        }
        return Optional.<ValueBlock>of(new MaskedValueBlock(valueBlock, activePositions.get()));
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
    public Tuple getSingleValue()
    {
        return valueBlock.getSingleValue();
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
    public Range getRange()
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
