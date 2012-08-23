package com.facebook.presto;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;

public interface PositionBlock
    extends Block, Predicate<Long>
{
    Optional<PositionBlock> filter(PositionBlock positionBlock);
}
