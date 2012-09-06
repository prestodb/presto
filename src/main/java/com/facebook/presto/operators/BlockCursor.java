/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operators;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.slice.Slice;

public interface BlockCursor
{
    Range getRange();

    BlockCursor duplicate();
    void advanceTo(BlockCursor blockForNextValue);

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
