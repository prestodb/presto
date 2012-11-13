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
     * Gets the number of position of the underlying storage block.
     * NOTE: This should only be used by the alignment operator.
     */
    int getRawPositionCount();

    /**
     * Returns a block which is restricted to the specified range.  The view
     * port range can be out side of this block but must be within the range
     * of the underlying storage block.
     */
    Block createViewPort(int rawPosition, int length);
}
