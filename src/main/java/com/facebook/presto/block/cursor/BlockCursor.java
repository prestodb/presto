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

    boolean advanceToNextValue();
    boolean advanceNextPosition();
    boolean advanceToPosition(long position);

    Tuple getTuple();
    long getLong(int field);
    double getDouble(int field);
    Slice getSlice(int field);
    long getPosition();
    long getValuePositionEnd();
    boolean tupleEquals(Tuple value);
}
