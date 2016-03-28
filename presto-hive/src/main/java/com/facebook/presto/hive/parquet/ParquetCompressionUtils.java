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
package com.facebook.presto.hive.parquet;

import io.airlift.compress.snappy.SnappyDecompressor;
import io.airlift.slice.Slice;
import parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;

public final class ParquetCompressionUtils
{
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
            default:
                // TODO add LZO support using pure java aircompressor
                throw new ParquetCorruptionException("Codec not supported in Parquet: " + codec);
        }
    }

    private static Slice decompressSnappy(Slice input, int uncompressedSize)
    {
        byte[] inArray = (byte[]) input.getBase();
        int inOffset = (int) (input.getAddress() - ARRAY_BYTE_BASE_OFFSET);
        int inLength = input.length();

        byte[] buffer = new byte[uncompressedSize];
        SnappyDecompressor decompressor = new SnappyDecompressor();
        decompressor.decompress(inArray, inOffset, inLength, buffer, 0, uncompressedSize);
        return wrappedBuffer(buffer);
    }

    private static Slice decompressGzip(Slice input, int uncompressedSize)
            throws IOException
    {
        ByteArrayOutputStream uncompressedStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[uncompressedSize];
        try (InputStream gzipInputStream = new GZIPInputStream(new ByteArrayInputStream(input.getBytes()))) {
            int bytesRead;
            while ((bytesRead = gzipInputStream.read(buffer)) != -1) {
                uncompressedStream.write(buffer, 0, bytesRead);
            }
            return wrappedBuffer(uncompressedStream.toByteArray());
        }
    }
}
