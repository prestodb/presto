/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;

public class ForwardingCursor implements Cursor
{
    private final Cursor cursor;

    public ForwardingCursor(Cursor cursor)
    {
        this.cursor = cursor;
    }

    public Cursor getDelegate()
    {
        return cursor;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return cursor.getTupleInfo();
    }

    @Override
    public boolean isFinished()
    {
        return cursor.isFinished();
    }

    @Override
    public boolean advanceNextValue()
    {
        return cursor.advanceNextValue();
    }

    @Override
    public boolean advanceNextPosition()
    {
        return cursor.advanceNextPosition();
    }

    @Override
    public boolean advanceToPosition(long position)
    {
        return cursor.advanceToPosition(position);
    }

    @Override
    public Tuple getTuple()
    {
        return cursor.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        return cursor.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        return cursor.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        return cursor.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        return cursor.getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        return cursor.getCurrentValueEndPosition();
    }

    @Override
    public boolean currentValueEquals(Tuple value)
    {
        return cursor.currentValueEquals(value);
    }
}
