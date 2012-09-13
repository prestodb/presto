package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Cursor;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

public class DataScan2
        implements BlockStream
{
    private final BlockStream source;
    private final Predicate<Cursor> predicate;

    public DataScan2(BlockStream source, Predicate<Cursor> predicate)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(predicate, "predicate is null");
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return source.getTupleInfo();
    }

    @Override
    public Cursor cursor()
    {
        return new FilteredCursor(predicate, source.cursor());
    }
}
