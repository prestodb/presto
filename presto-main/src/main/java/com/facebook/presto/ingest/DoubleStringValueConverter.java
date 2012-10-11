package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockBuilder;

public class DoubleStringValueConverter
    implements StringValueConverter
{
    public static final DoubleStringValueConverter INSTANCE = new DoubleStringValueConverter();

    @Override
    public void convert(String value, BlockBuilder blockBuilder)
    {
        blockBuilder.append(Double.valueOf(value));
    }

    @Override
    public void convert(String value, TupleInfo.Builder tupleBuilder)
    {
        tupleBuilder.append(Double.valueOf(value));
    }
}
