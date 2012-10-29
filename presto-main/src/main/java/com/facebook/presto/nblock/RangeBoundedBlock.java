package com.facebook.presto.nblock;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;

import static com.google.common.base.Preconditions.checkNotNull;

public class RangeBoundedBlock
        implements Block
{
    private final Range validRange;
    private final BlockCursor baseCursor;

    public RangeBoundedBlock(Range validRange, BlockCursor cursor)
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
    public BlockCursor cursor()
    {
        return new RangeBoundedBlockCursor(validRange, baseCursor);
    }
}
