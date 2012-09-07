package com.facebook.presto;

import com.facebook.presto.block.cursor.BlockCursor;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.PeekingIterator;

public interface ValueBlock
        extends Block, Iterable<Tuple>
{
    Optional<PositionBlock> selectPositions(Predicate<Tuple> predicate);
    PositionBlock toPositionBlock();

    Optional<ValueBlock> selectPairs(Predicate<Tuple> predicate);

    Optional<ValueBlock> filter(PositionBlock positions);

    PeekingIterator<Pair> pairIterator();

    /**
     * @throws IllegalStateException if this block contains more than one value
     */
    Tuple getSingleValue();

    BlockCursor blockCursor();
}
