package com.facebook.presto.operation;

import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Cursor;

public class SubtractionOperation
    implements BinaryOperation
{
    @Override
    public TupleInfo getTupleInfo()
    {
        return new TupleInfo(TupleInfo.Type.FIXED_INT_64);
    }

    @Override
    public Tuple evaluate(Cursor first, Cursor second)
    {
        return getTupleInfo().builder()
            .append(first.getLong(0) - second.getLong(0))
            .build();
    }

    @Override
    public long evaluateAsLong(Cursor first, Cursor second)
    {
        return first.getLong(0) - second.getLong(0);
    }
}
