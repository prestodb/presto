package com.facebook.presto.block.position;

import com.facebook.presto.block.Block;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;

public interface PositionBlock
    extends Block, Predicate<Long>
{
    Optional<PositionBlock> filter(PositionBlock positionBlock);
}
