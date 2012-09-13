package com.facebook.presto.block;


import com.facebook.presto.Range;

public interface Block
{
    int getCount();

    /**
     * Whether the values in this block are sorted
     */
    boolean isSorted();

    /**
     * Whether the block contains a single value (with one or more positions for that value)
     */
    boolean isSingleValue();

    /**
     * Whether the block contains gaps in its positions
     */
    boolean isPositionsContiguous();

    Range getRange();
}
