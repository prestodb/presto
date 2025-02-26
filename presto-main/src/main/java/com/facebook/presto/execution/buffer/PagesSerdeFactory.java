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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.CompressionCodec;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.spiller.SpillCipher;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoCompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;

import java.util.Optional;
import java.util.OptionalInt;

import static java.util.Objects.requireNonNull;

public class PagesSerdeFactory
{
    private final BlockEncodingSerde blockEncodingSerde;
    private final CompressionCodec compressionCodec;
    private final boolean checksumEnabled;

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, CompressionCodec compressionCodec)
    {
        this(blockEncodingSerde, compressionCodec, false);
    }

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, CompressionCodec compressionCodec, boolean checksumEnabled)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.compressionCodec = compressionCodec;
        this.checksumEnabled = checksumEnabled;
    }

    public PagesSerde createPagesSerde()
    {
        return createPagesSerdeInternal(Optional.empty());
    }

    public PagesSerde createPagesSerdeForSpill(Optional<SpillCipher> spillCipher)
    {
        return createPagesSerdeInternal(spillCipher);
    }

    private PagesSerde createPagesSerdeInternal(Optional<SpillCipher> spillCipher)
    {
        switch (compressionCodec) {
            case LZ4:
                return new PagesSerde(blockEncodingSerde,
                        Optional.of(new AirliftCompressorAdapter(new Lz4Compressor())),
                        Optional.of(new AirliftDecompressorAdapter(new Lz4Decompressor())),
                        spillCipher, checksumEnabled);
            case SNAPPY:
                return new PagesSerde(blockEncodingSerde,
                        Optional.of(new AirliftCompressorAdapter(new SnappyCompressor())),
                        Optional.of(new AirliftDecompressorAdapter(new SnappyDecompressor())),
                        spillCipher, checksumEnabled);
            case LZO:
                return new PagesSerde(blockEncodingSerde,
                        Optional.of(new AirliftCompressorAdapter(new LzoCompressor())),
                        Optional.of(new AirliftDecompressorAdapter(new LzoDecompressor())),
                        spillCipher, checksumEnabled);
            case ZSTD:
                return new PagesSerde(blockEncodingSerde,
                        Optional.of(new AirliftCompressorAdapter(new ZstdCompressor())),
                        Optional.of(new AirliftDecompressorAdapter(new ZstdDecompressor())),
                        spillCipher, checksumEnabled);
            case GZIP:
            case ZLIB:
                return new PagesSerde(blockEncodingSerde,
                        Optional.of(new AirliftCompressorAdapter(new DeflateCompressor(OptionalInt.empty()))),
                        Optional.of(new AirliftDecompressorAdapter(new InflateDecompressor())),
                        spillCipher, checksumEnabled);
            case NONE:
            default:
                return new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), spillCipher, checksumEnabled);
        }
    }
}
