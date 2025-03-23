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
import com.facebook.presto.spi.page.PageCompressor;
import com.facebook.presto.spi.page.PageDecompressor;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.spiller.SpillCipher;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;

import java.util.Optional;

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
        this.compressionCodec = requireNonNull(compressionCodec, "compressionCodec is null");
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
        return new PagesSerde(blockEncodingSerde, getPageCompressor(), getPageDecompressor(), spillCipher, checksumEnabled);
    }

    private Optional<PageCompressor> getPageCompressor()
    {
        switch (compressionCodec) {
            case LZ4:
                return Optional.of(new AirliftCompressorAdapter(new Lz4Compressor()));
            case NONE:
            default:
                return Optional.empty();
        }
    }

    private Optional<PageDecompressor> getPageDecompressor()
    {
        switch (compressionCodec) {
            case LZ4:
                return Optional.of(new AirliftDecompressorAdapter(new Lz4Decompressor()));
            case NONE:
            default:
                return Optional.empty();
        }
    }
}
