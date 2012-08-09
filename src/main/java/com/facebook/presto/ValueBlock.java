package com.facebook.presto;

import com.google.common.base.Predicate;

public interface ValueBlock
    extends Block, Iterable<Object>
{
    PositionBlock selectPositions(Predicate<Object> predicate);
    ValueBlock selectPairs(Predicate<Object> predicate);
    ValueBlock filter(PositionBlock positions);
}
