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
package com.facebook.presto.orc.zlib;

import com.facebook.presto.orc.CompressionLevel;
import com.google.common.collect.ImmutableMap;
import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.zip.Deflater;

import static com.facebook.presto.orc.CompressionLevel.BEST_COMPRESSION;
import static com.facebook.presto.orc.CompressionLevel.BEST_SPEED;
import static com.facebook.presto.orc.CompressionLevel.BETTER_COMPRESSION;
import static com.facebook.presto.orc.CompressionLevel.BETTER_SPEED;
import static com.facebook.presto.orc.CompressionLevel.DEFAULT_COMPRESSION;
import static com.facebook.presto.orc.CompressionLevel.ULTRA_COMPRESSION;
import static java.util.Objects.requireNonNull;
import static java.util.zip.Deflater.FULL_FLUSH;

public class DeflateCompressor
        implements Compressor
{
    private static final int EXTRA_COMPRESSION_SPACE = 16;

    // ultra compression level is same as the best compression level
    private static final Map<CompressionLevel, Integer> COMPRESSION_LEVELS = ImmutableMap.<CompressionLevel, Integer>builder()
            .put(BEST_SPEED, 1)
            .put(BETTER_SPEED, 2)
            .put(DEFAULT_COMPRESSION, 4)
            .put(BETTER_COMPRESSION, 7)
            .put(BEST_COMPRESSION, 9)
            .put(ULTRA_COMPRESSION, 9)
            .build();

    private final int compressionLevel;

    public DeflateCompressor(CompressionLevel compressionLevel)
    {
        requireNonNull(compressionLevel, "compressionLevel is null");
        this.compressionLevel = requireNonNull(COMPRESSION_LEVELS.get(compressionLevel),
                () -> String.format("Cannot resolve %s compression level", compressionLevel));
    }

    @Override
    public int maxCompressedLength(int uncompressedSize)
    {
        // From Mark Adler's post http://stackoverflow.com/questions/1207877/java-size-of-compression-output-bytearray
        return uncompressedSize + ((uncompressedSize + 7) >> 3) + ((uncompressedSize + 63) >> 6) + 5 + EXTRA_COMPRESSION_SPACE;
    }

    @Override
    public int compress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        int maxCompressedLength = maxCompressedLength(inputLength);
        if (maxOutputLength < maxCompressedLength) {
            throw new IllegalArgumentException("Output buffer must be at least " + maxCompressedLength + " bytes");
        }

        Deflater deflater = new Deflater(compressionLevel, true);
        try {
            deflater.setInput(input, inputOffset, inputLength);
            deflater.finish();

            int compressedDataLength = deflater.deflate(output, outputOffset, maxOutputLength, FULL_FLUSH);
            if (!deflater.finished()) {
                throw new IllegalArgumentException("maxCompressedLength formula is incorrect, because deflate produced more data");
            }
            return compressedDataLength;
        }
        finally {
            deflater.end();
        }
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
