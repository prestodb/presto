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

    boolean hasNextPosition();
    void advanceNextPosition();
    boolean hasMorePositionsForCurrentValue();

    Tuple getTuple();
    long getLong(int field);
    Slice getSlice(int field);
    long getPosition();
    long getCurrentValueEndPosition();
    boolean tupleEquals(Tuple value);
}
