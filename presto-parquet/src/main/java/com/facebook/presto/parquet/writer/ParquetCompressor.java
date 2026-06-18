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
package com.facebook.presto.parquet.writer;

import io.airlift.compress.Compressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.slice.Slices;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import static com.facebook.presto.parquet.writer.ParquetDataOutput.createDataOutput;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

interface ParquetCompressor
{
    ParquetDataOutput compress(BytesInput bytesInput)
            throws IOException;

    static ParquetCompressor getCompressor(CompressionCodecName codec)
    {
        // TODO Support LZO and LZ4 compression
        // When using airlift LZO or LZ4 compressor, decompressing page in reader throws exception.
        switch (codec.getParquetCompressionCodec()) {
            case GZIP:
                return new GzipCompressor();
            case SNAPPY:
                return new AirLiftCompressor(new SnappyCompressor());
            case ZSTD:
                return new AirLiftCompressor(new ZstdCompressor());
            case UNCOMPRESSED:
                return null;
        }
        throw new RuntimeException(String.format("%s codec is not supported", codec));
    }

    class GzipCompressor
            implements ParquetCompressor
    {
        @Override
        public ParquetDataOutput compress(BytesInput bytesInput)
                throws IOException
        {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try (GZIPOutputStream outputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                outputStream.write(bytesInput.toByteArray(), 0, toIntExact(bytesInput.size()));
            }
            byte[] bytes = byteArrayOutputStream.toByteArray();
            return createDataOutput(Slices.wrappedBuffer(bytes, 0, bytes.length));
        }
    }

    class AirLiftCompressor
            implements ParquetCompressor
    {
        private final Compressor compressor;

        AirLiftCompressor(Compressor compressor)
        {
            this.compressor = requireNonNull(compressor, "compressor is null");
        }

        @Override
        public ParquetDataOutput compress(BytesInput bytesInput)
                throws IOException
        {
            int minCompressionBufferSize = compressor.maxCompressedLength(toIntExact(bytesInput.size()));
            byte[] compressionBuffer = new byte[minCompressionBufferSize];
            byte[] bytes = bytesInput.toByteArray();
            // TODO compressedDataSize > bytes.length?
            int compressedDataSize = compressor.compress(bytes, 0, bytes.length, compressionBuffer, 0, compressionBuffer.length);
            return createDataOutput(Slices.wrappedBuffer(compressionBuffer, 0, compressedDataSize));
        }
    }
}
