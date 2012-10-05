package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;

public interface TupleStream
{
    /**
     * Gets the type of all tuples in this cursor
     */
    TupleInfo getTupleInfo();

    /**
     * Gets the start and end positions of the block.
     */
    Range getRange();

    /**
     * Gets a cursor over the block
     * @param session
     */
    Cursor cursor(QuerySession session);
}
