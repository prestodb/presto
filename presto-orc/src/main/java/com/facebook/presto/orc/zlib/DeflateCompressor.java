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

import io.airlift.compress.Compressor;

import java.nio.ByteBuffer;
import java.util.OptionalInt;
import java.util.zip.Deflater;

import static java.util.Objects.requireNonNull;
import static java.util.zip.Deflater.FULL_FLUSH;

public class DeflateCompressor
        implements Compressor
{
    private static final int EXTRA_COMPRESSION_SPACE = 16;
    private static final int DEFAULT_COMPRESSION_LEVEL = 4;

    private final int compressionLevel;

    public DeflateCompressor(OptionalInt compressionLevel)
    {
        requireNonNull(compressionLevel, "compressionLevel is null");
        this.compressionLevel = compressionLevel.orElse(DEFAULT_COMPRESSION_LEVEL);
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
