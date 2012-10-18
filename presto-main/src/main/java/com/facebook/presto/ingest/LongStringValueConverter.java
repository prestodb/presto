package com.facebook.presto.ingest;

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
}
