package com.facebook.presto.block;


import com.facebook.presto.Range;

public interface Block
{
    /**
     * Gets the number of positions in the block
     */
    int getCount();

    /**
     * Gets the start and end positions of the block.
     */
    Range getRange();

    /**
     * Gets a cursor over the block
     */
    BlockCursor blockCursor();
}
