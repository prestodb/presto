package com.facebook.presto.ingest;

import com.facebook.presto.block.BlockBuilder;

public class LongFieldValueConverter
    implements FieldValueConverter
{
    public static final LongFieldValueConverter INSTANCE = new LongFieldValueConverter();

    @Override
    public void convert(String value, BlockBuilder blockBuilder)
    {
        blockBuilder.append(Long.valueOf(value));
    }
}
