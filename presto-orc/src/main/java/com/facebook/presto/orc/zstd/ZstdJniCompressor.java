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
package com.facebook.presto.orc.zstd;

import com.facebook.presto.orc.CompressionLevel;
import com.github.luben.zstd.Zstd;
import com.google.common.collect.ImmutableMap;
import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.facebook.presto.orc.CompressionLevel.BEST_COMPRESSION;
import static com.facebook.presto.orc.CompressionLevel.BEST_SPEED;
import static com.facebook.presto.orc.CompressionLevel.BETTER_COMPRESSION;
import static com.facebook.presto.orc.CompressionLevel.BETTER_SPEED;
import static com.facebook.presto.orc.CompressionLevel.DEFAULT_COMPRESSION;
import static com.facebook.presto.orc.CompressionLevel.ULTRA_COMPRESSION;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ZstdJniCompressor
        implements Compressor
{
    // see https://facebook.github.io/zstd/zstd_manual.html#Chapter5
    private static final Map<CompressionLevel, Integer> COMPRESSION_LEVELS = ImmutableMap.<CompressionLevel, Integer>builder()
            .put(BEST_SPEED, 1)
            .put(BETTER_SPEED, 2)
            .put(DEFAULT_COMPRESSION, 3)
            .put(BETTER_COMPRESSION, 6)
            .put(BEST_COMPRESSION, 7)
            .put(ULTRA_COMPRESSION, 9)
            .build();

    private final int compressionLevel;

    public ZstdJniCompressor(CompressionLevel compressionLevel)
    {
        requireNonNull(compressionLevel, "compressionLevel is null");
        this.compressionLevel = requireNonNull(COMPRESSION_LEVELS.get(compressionLevel),
                () -> String.format("Cannot resolve %s compression level", compressionLevel));
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        return toIntExact(Zstd.compressBound(uncompressedSize));
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        long size = Zstd.compressByteArray(output, outputOffset, maxOutputLength, input, inputOffset, inputLength, compressionLevel);
        if (Zstd.isError(size)) {
            throw new RuntimeException(Zstd.getErrorName(size));
        }
        return toIntExact(size);
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output)
    {
        if (input.isDirect() || output.isDirect() || !input.hasArray() || !output.hasArray()) {
            throw new IllegalArgumentException("Non-direct byte buffer backed by byte array required");
        }
        int inputOffset = input.arrayOffset() + input.position();
        int outputOffset = output.arrayOffset() + output.position();

        int written = compress(input.array(), inputOffset, input.remaining(), output.array(), outputOffset, output.remaining());
        output.position(output.position() + written);
    }
}
