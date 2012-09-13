package com.facebook.presto.operator;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Cursor;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import java.util.Iterator;

public class DataScan2
        implements BlockStream<Block>
{
    private final BlockStream<? extends Block> source;
    private final Predicate<Cursor> predicate;

    public DataScan2(BlockStream<? extends Block> source, Predicate<Cursor> predicate)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(predicate, "predicate is null");
        this.source = source;
        this.predicate = predicate;
    }

    public Iterator<Block> iterator()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return source.getTupleInfo();
    }

    @Override
    public Cursor cursor()
    {
        return new FilteredValueCursor(predicate, source.cursor());
    }
}
