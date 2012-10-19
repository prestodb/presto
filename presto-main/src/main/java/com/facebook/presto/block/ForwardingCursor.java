/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.block;

import com.facebook.presto.Range;
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
    public Range getRange()
    {
        return cursor.getRange();
    }

    @Override
    public boolean isValid()
    {
        return cursor.isValid();
    }

    @Override
    public boolean isFinished()
    {
        return cursor.isFinished();
    }

    @Override
    public AdvanceResult advanceNextValue()
    {
        return cursor.advanceNextValue();
    }

    @Override
    public AdvanceResult advanceNextPosition()
    {
        return cursor.advanceNextPosition();
    }

    @Override
    public AdvanceResult advanceToPosition(long position)
    {
        return cursor.advanceToPosition(position);
    }

    @Override
    public Tuple getTuple()
    {
        Cursors.checkReadablePosition(this);
        return cursor.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        Cursors.checkReadablePosition(this);
        return cursor.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        Cursors.checkReadablePosition(this);
        return cursor.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        Cursors.checkReadablePosition(this);
        return cursor.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        Cursors.checkReadablePosition(this);
        return cursor.getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        Cursors.checkReadablePosition(this);
        return cursor.getCurrentValueEndPosition();
    }

    @Override
    public boolean currentTupleEquals(Tuple value)
    {
        Cursors.checkReadablePosition(this);
        return cursor.currentTupleEquals(value);
    }
}
