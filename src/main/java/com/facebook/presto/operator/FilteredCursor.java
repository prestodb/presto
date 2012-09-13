/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.slice.Slice;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

public class FilteredCursor implements Cursor
{
    private final Predicate<Cursor> predicate;
    private final Cursor delegate;

    public FilteredCursor(Predicate<Cursor> predicate, Cursor delegate)
    {
        Preconditions.checkNotNull(predicate, "predicate is null");
        Preconditions.checkNotNull(delegate, "delegate is null");

        this.predicate = predicate;
        this.delegate = delegate;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return delegate.getTupleInfo();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public boolean advanceNextValue()
    {
        do {
            if (!delegate.advanceNextValue()) {
                return false;
            }
        } while (predicate.apply(delegate));

        return true;
    }

    @Override
    public boolean advanceNextPosition()
    {
        // todo only apply predicate when value changes
        do {
            if (!delegate.advanceNextPosition()) {
                return false;
            }
        } while (predicate.apply(delegate));

        return true;
    }

    @Override
    public boolean advanceToPosition(long newPosition)
    {
        return delegate.advanceToPosition(newPosition) &&
                (predicate.apply(delegate) || advanceNextPosition());
    }

    @Override
    public Tuple getTuple()
    {
        return delegate.getTuple();
    }

    @Override
    public long getLong(int field)
    {
        return delegate.getLong(field);
    }

    @Override
    public double getDouble(int field)
    {
        return delegate.getDouble(field);
    }

    @Override
    public Slice getSlice(int field)
    {
        return delegate.getSlice(field);
    }

    @Override
    public long getPosition()
    {
        return delegate.getPosition();
    }

    @Override
    public long getCurrentValueEndPosition()
    {
        return delegate.getCurrentValueEndPosition();
    }

    @Override
    public boolean currentValueEquals(Tuple value)
    {
        return delegate.currentValueEquals(value);
    }
}
