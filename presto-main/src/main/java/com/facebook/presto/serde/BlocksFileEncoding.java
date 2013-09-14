/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
