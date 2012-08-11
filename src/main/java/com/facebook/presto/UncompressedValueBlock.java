package com.facebook.presto;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.Pair.positionGetter;
import static com.facebook.presto.Pair.valueGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public class UncompressedValueBlock
    implements ValueBlock
{
    private final Range<Long> range;
    private final List<Pair> pairs;

    public UncompressedValueBlock(long startPosition, List<?> values)
    {
        ImmutableList.Builder<Pair> builder = ImmutableList.builder();

        long index = startPosition;
        for (Object value : values) {
            if (value != null) {
                builder.add(new Pair(index, value));
            }

            ++index;
        }

        pairs = builder.build();
        range = Ranges.closed(pairs.get(0).getPosition(), pairs.get(pairs.size() - 1).getPosition());
    }

    public UncompressedValueBlock(long startPosition, Object... values)
    {
        this(startPosition, asList(values));
    }

    public UncompressedValueBlock(List<Pair> pairs)
    {
        checkNotNull(pairs, "pairs is null");
        checkArgument(!pairs.isEmpty(), "pairs is empty");

        this.pairs = pairs;

        range = Ranges.closed(pairs.get(0).getPosition(), pairs.get(pairs.size() - 1).getPosition());
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
        ImmutableList<Pair> pairs = ImmutableList.copyOf(Iterables.filter(this.pairs, Predicates.compose(positions, positionGetter())));

        if (pairs.isEmpty()) {
            return new EmptyValueBlock();
        }

        return new UncompressedValueBlock(pairs);
    }

    @Override
    public PeekingIterator<Pair> pairIterator()
    {
        return Iterators.peekingIterator(pairs.iterator());
    }

    @Override
    public boolean isEmpty()
    {
        return false;
    }

    @Override
    public int getCount()
    {
        return pairs.size();
    }

    @Override
    public boolean isSorted()
    {
        return false;
    }

    @Override
    public boolean isSingleValue()
    {
        return pairs.size() == 1;
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return false;
    }

    @Override
    public Iterable<Long> getPositions()
    {
        return Lists.transform(pairs, positionGetter());
    }

    @Override
    public Range<Long> getRange()
    {
        return range;
    }

    public String toString()
    {
        return pairs.toString();
    }

    @Override
    public Iterator<Object> iterator()
    {
        return Lists.transform(pairs, valueGetter()).iterator();
    }
}
