package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;

public interface Block
{
    /**
     * Gets the type of all tuples in this cursor
     */
    TupleInfo getTupleInfo();

    /**
     * Returns the number of positions in this block.
     */
    int getPositionCount();

    /**
     * Gets the start and end positions of the block.
     */
    Range getRange();

    /**
     * Gets a cursor over the block
     */
    BlockCursor cursor();


    /**
     * Gets the start and end positions of the underlying storage block.
     */
    Range getRawRange();

    /**
     * Returns a block which is restricted to the specified range.  The view
     * port range can be out side of this block but must be within the range
     * of the underlying storage block.
     */
    Block createViewPort(Range viewPortRange);

}
