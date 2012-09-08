/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto;

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
    public boolean hasNextValue()
    {
        return cursor.hasNextValue();
    }

    @Override
    public void advanceNextValue()
    {
        cursor.advanceNextValue();
    }

    @Override
    public boolean hasNextPosition()
    {
        return cursor.hasNextPosition();
    }

    @Override
    public void advanceNextPosition()
    {
        cursor.advanceNextPosition();
    }

    @Override
    public void advanceToPosition(long position)
    {
        cursor.advanceToPosition(position);
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
    public long peekNextValuePosition()
    {
        return cursor.peekNextValuePosition();
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

    @Override
    public boolean nextValueEquals(Tuple value)
    {
        return cursor.nextValueEquals(value);
    }
}
