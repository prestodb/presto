package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;

/**
 * Class that provides a positional view of a TupleStream that is backed by a Cursor.
 * Changes to the underlying cursor will also be reflected by this view.
 */
public class TupleStreamPosition
{
    private final Cursor cursor;

    public TupleStreamPosition(Cursor cursor)
    {
        this.cursor = Preconditions.checkNotNull(cursor, "cursor is null");
    }

    public TupleInfo getTupleInfo()
    {
        return cursor.getTupleInfo();
    }

    public Range getRange()
    {
        return cursor.getRange();
    }

    public Tuple getTuple()
    {
        return cursor.getTuple();
    }

    public long getLong(int field)
    {
        return cursor.getLong(field);
    }

    public double getDouble(int field)
    {
        return cursor.getDouble(field);
    }

    public Slice getSlice(int field)
    {
        return cursor.getSlice(field);
    }

    public long getPosition()
    {
        return cursor.getPosition();
    }

    public long getCurrentValueEndPosition()
    {
        return cursor.getCurrentValueEndPosition();
    }

    public boolean currentTupleEquals(Tuple value)
    {
        return cursor.currentTupleEquals(value);
    }
}
