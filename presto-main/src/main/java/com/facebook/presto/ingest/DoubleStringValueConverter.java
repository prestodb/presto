package com.facebook.presto.ingest;

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
}
