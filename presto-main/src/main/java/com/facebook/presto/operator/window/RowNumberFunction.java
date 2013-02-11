package com.facebook.presto.operator.window;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;

public class RowNumberFunction
        implements WindowFunction
{
    private long rowNumber;

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public void reset()
    {
        rowNumber = 0;
    }

    @Override
    public void processRow(BlockBuilder output)
    {
        rowNumber++;
        output.append(rowNumber);
    }
}
