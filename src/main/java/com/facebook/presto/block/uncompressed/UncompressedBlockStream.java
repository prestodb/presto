package com.facebook.presto.block.uncompressed;

import com.facebook.presto.block.BlockStream;
import com.facebook.presto.block.Cursor;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.operator.ValueCursor;
import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Iterator;

public class UncompressedBlockStream
        implements BlockStream<UncompressedValueBlock>
{
    private final TupleInfo info;
    private final Iterable<UncompressedValueBlock> source;

    public UncompressedBlockStream(TupleInfo info, UncompressedValueBlock... source)
    {
        this(info, Arrays.asList(source));
    }

    public UncompressedBlockStream(TupleInfo info, Iterable<UncompressedValueBlock> source)
    {
        Preconditions.checkNotNull(info, "info is null");
        Preconditions.checkNotNull(source, "source is null");

        this.info = info;
        this.source = source;
    }

    @Override
    public Iterator<UncompressedValueBlock> iterator()
    {
        return source.iterator();
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return info;
    }

    @Override
    public Cursor cursor()
    {
        return new ValueCursor(info, source.iterator());
    }
}
