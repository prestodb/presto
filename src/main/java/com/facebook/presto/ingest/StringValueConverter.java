package com.facebook.presto.ingest;

import com.facebook.presto.block.BlockBuilder;

public interface StringValueConverter
{
    /**
     * Takes the provided value and converts it into a field in the provided blockBuilder
     */
    void convert(String value, BlockBuilder blockBuilder);
}
