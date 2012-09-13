package com.facebook.presto;

import com.facebook.presto.block.Block;
import com.google.common.collect.PeekingIterator;

public interface SeekableIterator<T extends Block>
    extends PeekingIterator<T>
{
    /**
     * Seek to the block whose range contains the specified position. The position must be equal or greater than
     * the lower bound of the next available block.
     *
     * Note that if the block is sparse it may not contain an actual element at the position specified.
     *
     * If a block is is found, peek() and next() will return it. Otherwise, they will return the next block whose position
     * is greater that the specified position.
     *
     * @return true if a block whose range includes the specified position was found. Otherwise, false
     * @throws IllegalArgumentException if the requested position is before the lower end of the range of the next available block
     */
    boolean seekTo(long position);
}
