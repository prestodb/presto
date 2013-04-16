/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;

public enum BlocksFileEncoding
{
    RAW("raw")
            {
                @Override
                public Encoder createBlocksWriter(SliceOutput sliceOutput)
                {
                    return new UncompressedEncoder(sliceOutput);
                }
            },
    RLE("rle")
            {
                @Override
                public Encoder createBlocksWriter(SliceOutput sliceOutput)
                {
                    return new RunLengthEncoder(sliceOutput);
                }
            },
    DIC_RAW("dic-raw")
            {
                @Override
                public Encoder createBlocksWriter(SliceOutput sliceOutput)
                {
                    return new DictionaryEncoder(new UncompressedEncoder(sliceOutput));
                }
            },
    DIC_RLE("dic-rle")
            {
                @Override
                public Encoder createBlocksWriter(SliceOutput sliceOutput)
                {
                    return new DictionaryEncoder(new RunLengthEncoder(sliceOutput));
                }
            },
    SNAPPY("snappy")
            {
                @Override
                public Encoder createBlocksWriter(SliceOutput sliceOutput)
                {
                    return new SnappyEncoder(sliceOutput);
                }
            };


    private final String name;

    BlocksFileEncoding(String name)
    {
        this.name = checkNotNull(name, "name is null");
    }

    public String getName()
    {
        return name;
    }

    public abstract Encoder createBlocksWriter(SliceOutput sliceOutput);
}
