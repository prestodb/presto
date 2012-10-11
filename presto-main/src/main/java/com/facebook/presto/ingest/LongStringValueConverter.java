package com.facebook.presto.ingest;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.BlockBuilder;

public class LongStringValueConverter
    implements StringValueConverter
{
    public static final LongStringValueConverter INSTANCE = new LongStringValueConverter();

    @Override
    public void convert(String value, BlockBuilder blockBuilder)
    {
        blockBuilder.append(Long.valueOf(value));
    }

    @Override
    public void convert(String value, TupleInfo.Builder tupleBuilder)
    {
        tupleBuilder.append(Long.valueOf(value));
    }
}
