package com.facebook.presto.block;

import com.facebook.presto.serde.BlockEncoding;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.Range;

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
     * Gets the start and end positions of the block.
     * INVARIANT: getRange() will always contained within getRawRange()
     */
    Range getRange();

    /**
     * Gets a cursor over the block
     */
    BlockCursor cursor();

    /**
     * Gets the start and end positions of the underlying storage block.
     * NOTE: This should only be used by the alignment operator.
     */
    Range getRawRange();

    /**
     * Get the encoding for this block
     */
    BlockEncoding getEncoding();

    /**
     * Returns a block which is restricted to the specified range.  The view
     * port range can be out side of this block but must be within the range
     * of the underlying storage block.
     */
    Block createViewPort(Range viewPortRange);
}
