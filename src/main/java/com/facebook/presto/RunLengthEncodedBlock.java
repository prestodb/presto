package com.facebook.presto;

import com.facebook.presto.block.cursor.BlockCursor;
import com.google.common.base.Objects;

import java.util.Iterator;

public class RunLengthEncodedBlock
        implements ValueBlock
{
    private final Tuple value;
    private final Range range;

    public RunLengthEncodedBlock(Tuple value, Range range)
    {
        this.value = value;
        this.range = range;
    }

    public Tuple getValue()
    {
        return value;
    }

    @Override
    public int getCount()
    {
        return (int) (range.getEnd() - range.getStart() + 1);
    }

    @Override
    public boolean isSorted()
    {
        return true;
    }

    @Override
    public boolean isSingleValue()
    {
        return true;
    }

    public Tuple getSingleValue()
    {
        return value;
    }

    @Override
    public boolean isPositionsContiguous()
    {
        return true;
    }

    @Override
    public Range getRange()
    {
        return range;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("value", value)
                .add("range", range)
                .toString();
    }

    @Override
    public BlockCursor blockCursor()
    {
        throw new UnsupportedOperationException();
    }
}
