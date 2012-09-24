package com.facebook.presto.ingest;

import com.facebook.presto.block.BlockBuilder;
import com.google.common.base.Charsets;

public class StringFieldValueConverter
    implements FieldValueConverter
{
    public static final StringFieldValueConverter INSTANCE = new StringFieldValueConverter();

    @Override
    public void convert(String value, BlockBuilder blockBuilder)
    {
        blockBuilder.append(value.getBytes(Charsets.UTF_8));
    }
}
