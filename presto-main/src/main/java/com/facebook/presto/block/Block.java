package com.facebook.presto.block;

import com.facebook.presto.serde.BlockEncoding;
import com.facebook.presto.tuple.TupleInfo;

public interface Block
{
    /**
     * Gets the type of all tuples in this block
     */
    TupleInfo getTupleInfo();

    /**
     * Returns the number of positions in this block.
     */
    int getPositionCount();

    /**
     * Gets a cursor over the block
     */
    BlockCursor cursor();

    /**
     * Get the encoding for this block
     */
    BlockEncoding getEncoding();

    /**
     * Returns a block starting at the specified position and extends for the
     * specified length.  The specified region must be entirely contained
     * within this block.
     */
    Block getRegion(int positionOffset, int length);
}
