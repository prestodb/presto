package com.facebook.presto.ingest;

import com.facebook.presto.block.BlockBuilder;

public class DoubleFieldValueConverter
    implements FieldValueConverter
{
    public static final DoubleFieldValueConverter INSTANCE = new DoubleFieldValueConverter();

    @Override
    public void convert(String value, BlockBuilder blockBuilder)
    {
        blockBuilder.append(Double.valueOf(value));
    }
}
