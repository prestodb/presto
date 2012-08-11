package com.facebook.presto;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.PeekingIterator;

import java.util.Iterator;

public interface ValueBlock
    extends Block, Iterable<Object>
{
    PositionBlock selectPositions(Predicate<Object> predicate);
    ValueBlock selectPairs(Predicate<Object> predicate);
    ValueBlock filter(PositionBlock positions);

    PeekingIterator<Pair> pairIterator();
}
