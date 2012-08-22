package com.facebook.presto;

import com.google.common.base.Predicate;
import com.google.common.collect.PeekingIterator;

public interface ValueBlock
        extends Block, Iterable<Tuple>
{
    PositionBlock selectPositions(Predicate<Tuple> predicate);
    PositionBlock toPositionBlock();

    ValueBlock selectPairs(Predicate<Tuple> predicate);

    ValueBlock filter(PositionBlock positions);

    PeekingIterator<Pair> pairIterator();
}
