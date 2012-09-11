/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block.cursor;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.slice.Slice;

public interface BlockCursor
{
    Range getRange();

    void moveTo(BlockCursor newPosition);

    boolean hasNextValue();
    void advanceNextValue();

    boolean hasNextValuePosition();
    void advanceNextValuePosition();
    void advanceToPosition(long position);

    Tuple getTuple();
    long getLong(int field);
    Slice getSlice(int field);
    long getPosition();
    long getValuePositionEnd();
    boolean tupleEquals(Tuple value);
}
