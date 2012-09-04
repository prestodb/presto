package com.facebook.presto;

import com.facebook.presto.TupleInfo.Type;
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
        if (info.getFieldCount() == 1) {
            Type type = info.getTypes().get(0);
            if (type == Type.FIXED_INT_64) {
                return new UncompressedLongCursor(source.iterator());
            }
            if (type == Type.VARIABLE_BINARY) {
                return new UncompressedSliceCursor(source.iterator());
            }
        }
        return new UncompressedCursor(info, source.iterator());
    }
}
