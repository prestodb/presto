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
package com.facebook.presto.parquet;

import io.airlift.compress.Decompressor;
import io.airlift.compress.lz4.Lz4Decompressor;
import io.airlift.compress.lzo.LzoDecompressor;
import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.airlift.slice.Slice;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.zip.GZIPInputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class ParquetCompressionUtils
{
    private static final int GZIP_BUFFER_SIZE = 8 * 1024;

    private ParquetCompressionUtils() {}

    public static Slice decompress(CompressionCodecName codec, Slice input, int uncompressedSize)
            throws IOException
    {
        requireNonNull(input, "input is null");

        if (input.length() == 0) {
            return EMPTY_SLICE;
        }

        switch (codec) {
            case GZIP:
                return decompressGzip(input, uncompressedSize);
            case SNAPPY:
                return decompressSnappy(input, uncompressedSize);
            case UNCOMPRESSED:
                return input;
            case LZO:
                return decompressLZO(input, uncompressedSize);
            case LZ4:
                return decompressLz4(input, uncompressedSize);
            case ZSTD:
                return decompressZstd(input, uncompressedSize);
            default:
                throw new ParquetCorruptionException("Codec not supported in Parquet: " + codec);
        }
    }

    private static Slice decompressSnappy(Slice input, int uncompressedSize)
    {
        byte[] buffer = new byte[uncompressedSize];
        decompress(new SnappyDecompressor(), input, 0, input.length(), buffer, 0);
        return wrappedBuffer(buffer);
    }

    private static Slice decompressZstd(Slice input, int uncompressedSize)
    {
        byte[] buffer = new byte[uncompressedSize];
        decompress(new ZstdDecompressor(), input, 0, input.length(), buffer, 0);
        return wrappedBuffer(buffer);
    }

    private static Slice decompressGzip(Slice input, int uncompressedSize)
            throws IOException
    {
        if (uncompressedSize == 0) {
            return EMPTY_SLICE;
        }

        byte[] buffer = new byte[uncompressedSize];
        int bytesRead = 0;
        boolean eos = false;
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(input.getInput(), min(GZIP_BUFFER_SIZE, input.length()))) {
            int n;
            do {
                n = gzipInputStream.read(buffer, bytesRead, buffer.length - bytesRead);
                if (n < 0) {
                    eos = true;
                    break;
                }
                bytesRead += n;
            } while (bytesRead < buffer.length);
            checkArgument(eos || gzipInputStream.read() == -1, "Invalid uncompressedSize for GZIP input. Actual size exceeds %s bytes", uncompressedSize);
        }
        if (bytesRead != uncompressedSize) {
            throw new IllegalArgumentException(format("Invalid uncompressedSize for GZIP input. Expected %s, actual: %s", uncompressedSize, bytesRead));
        }
        return wrappedBuffer(buffer, 0, bytesRead);
    }

    private static Slice decompressLz4(Slice input, int uncompressedSize)
    {
        byte[] buffer = new byte[uncompressedSize];
        decompress(new Lz4Decompressor(), input, 0, input.length(), buffer, 0);
        return wrappedBuffer(buffer);
    }

    private static Slice decompressLZO(Slice input, int uncompressedSize)
    {
        LzoDecompressor lzoDecompressor = new LzoDecompressor();
        long totalDecompressedCount = 0;
        // over allocate buffer which makes decompression easier
        byte[] output = new byte[uncompressedSize + SIZE_OF_LONG];
        int outputOffset = 0;
        int inputOffset = 0;
        int cumulativeUncompressedBlockLength = 0;

        while (totalDecompressedCount < uncompressedSize) {
            if (totalDecompressedCount == cumulativeUncompressedBlockLength) {
                cumulativeUncompressedBlockLength += Integer.reverseBytes(input.getInt(inputOffset));
                inputOffset += SIZE_OF_INT;
            }
            int compressedChunkLength = Integer.reverseBytes(input.getInt(inputOffset));
            inputOffset += SIZE_OF_INT;
            int decompressionSize = decompress(lzoDecompressor, input, inputOffset, compressedChunkLength, output, outputOffset);
            totalDecompressedCount += decompressionSize;
            outputOffset += decompressionSize;
            inputOffset += compressedChunkLength;
        }
        checkArgument(outputOffset == uncompressedSize);
        return wrappedBuffer(output, 0, uncompressedSize);
    }

    private static int decompress(Decompressor decompressor, Slice input, int inputOffset, int inputLength, byte[] output, int outputOffset)
    {
        byte[] byteArray = (byte[]) input.getBase();
        int byteArrayOffset = inputOffset + (int) (input.getAddress() - ARRAY_BYTE_BASE_OFFSET);
        int size = decompressor.decompress(byteArray, byteArrayOffset, inputLength, output, outputOffset, output.length - outputOffset);
        return size;
    }
}
