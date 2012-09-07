package com.facebook.presto.operators;

import com.facebook.presto.BlockStream;
import com.facebook.presto.Cursor;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.ValueBlock;
import com.facebook.presto.block.cursor.BlockCursor;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

import java.util.Iterator;

public class DataScan1
        implements BlockStream<ValueBlock>
{
    private final BlockStream<?> source;
    private final Predicate<BlockCursor> predicate;

    public DataScan1(BlockStream<?> source, Predicate<BlockCursor> predicate)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(predicate, "predicate is null");
        this.source = source;
        this.predicate = predicate;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<ValueBlock> iterator()
    {
        // todo maybe have a position block
        throw new UnsupportedOperationException();
    }

    @Override
    public Cursor cursor()
    {
        // todo this is not correct... maybe add a position cursor
        return new FilteredValueCursor(predicate, source.getTupleInfo(), source.iterator());
    }
}
