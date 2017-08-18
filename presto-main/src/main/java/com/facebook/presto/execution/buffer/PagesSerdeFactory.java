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

import com.facebook.presto.spi.block.BlockEncodingSerde;
import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.lz4.Lz4Decompressor;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PagesSerdeFactory
{
    private final BlockEncodingSerde blockEncodingSerde;
    private final boolean compressionEnabled;
    private final boolean encryptionEnabled;

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled)
    {
        this(blockEncodingSerde, compressionEnabled, false);
    }

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled, boolean encryptionEnabled)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.compressionEnabled = compressionEnabled;
        this.encryptionEnabled = encryptionEnabled;
    }

    public PagesSerde createPagesSerde()
    {
        Optional<Compressor> compressor = Optional.empty();
        Optional<Decompressor> decompressor = Optional.empty();
        Optional<Encryptor> encryptor = Optional.empty();

        if (compressionEnabled) {
            compressor = Optional.of(new Lz4Compressor());
            decompressor = Optional.of(new Lz4Decompressor());
        }

        if (encryptionEnabled) {
            encryptor = Optional.of(new AES256Encryptor());
        }

        return new PagesSerde(blockEncodingSerde, compressor, decompressor, encryptor);
    }
}
