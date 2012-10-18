package com.facebook.presto.ingest;

import com.facebook.presto.block.BlockBuilder;
import com.google.common.base.Charsets;

public class VarBinaryStringValueConverter
    implements StringValueConverter
{
    public static final VarBinaryStringValueConverter INSTANCE = new VarBinaryStringValueConverter();

    @Override
    public void convert(String value, BlockBuilder blockBuilder)
    {
        blockBuilder.append(value.getBytes(Charsets.UTF_8));
    }
}
