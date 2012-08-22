package com.facebook.presto;

import com.google.common.base.Predicate;

public interface PositionBlock
    extends Block, Predicate<Long>
{
    PositionBlock filter(PositionBlock positionBlock);
}
