package com.facebook.presto.block;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;

import static com.google.common.base.Preconditions.checkNotNull;

public class RangeBoundedTupleStream
        implements TupleStream
{
    private final Range validRange;
    private final Cursor baseCursor;

    public RangeBoundedTupleStream(Range validRange, Cursor cursor)
    {
        this.validRange = checkNotNull(validRange, "validRange is null");
        this.baseCursor = checkNotNull(cursor, "cursor is null");
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return baseCursor.getTupleInfo();
    }

    @Override
    public Range getRange()
    {
        // TODO: we can improve the range tightness by taking the overlap with the delegate range
        return validRange;
    }

    @Override
    public Cursor cursor()
    {
        return new RangeBoundedCursor(validRange, baseCursor);
    }
}
