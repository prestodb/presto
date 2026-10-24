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
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.snappy.SnappyCompressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.slice.Slices;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;

import static com.facebook.presto.parquet.writer.ParquetDataOutput.createDataOutput;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.toIntExact;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.util.Objects.requireNonNull;

interface ParquetCompressor
{
    ParquetDataOutput compress(BytesInput bytesInput)
            throws IOException;

    static ParquetCompressor getCompressor(CompressionCodecName codec)
    {
        // TODO Support LZO compression
        switch (codec.getParquetCompressionCodec()) {
            case GZIP:
                return new GzipCompressor();
            case SNAPPY:
                return new AirLiftCompressor(new SnappyCompressor());
            case ZSTD:
                return new AirLiftCompressor(new ZstdCompressor());
            case LZ4:
                return new HadoopLz4Compressor();
            case LZ4_RAW:
                return new AirLiftCompressor(new Lz4Compressor());
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

    /**
     * Writes Parquet's LZ4 codec, which frames raw LZ4 blocks the way Hadoop's Lz4Codec does: each block is
     * preceded by its uncompressed and compressed lengths as big endian ints.
     */
    class HadoopLz4Compressor
            implements ParquetCompressor
    {
        // Hadoop's Lz4Decompressor holds a block in a 256KB buffer by default, so blocks stay well below it.
        private static final int MAX_BLOCK_SIZE = 128 * 1024;

        private final Lz4Compressor compressor = new Lz4Compressor();

        @Override
        public ParquetDataOutput compress(BytesInput bytesInput)
                throws IOException
        {
            byte[] bytes = bytesInput.toByteArray();
            int numBlocks = Math.max(1, (bytes.length + MAX_BLOCK_SIZE - 1) / MAX_BLOCK_SIZE);
            byte[] output = new byte[numBlocks * (2 * SIZE_OF_INT + compressor.maxCompressedLength(MAX_BLOCK_SIZE))];
            ByteBuffer outputBuffer = ByteBuffer.wrap(output).order(BIG_ENDIAN);
            int offset = 0;
            do {
                int blockSize = Math.min(MAX_BLOCK_SIZE, bytes.length - offset);
                int position = outputBuffer.position();
                int compressedSize = compressor.compress(bytes, offset, blockSize, output, position + 2 * SIZE_OF_INT, output.length - position - 2 * SIZE_OF_INT);
                outputBuffer.putInt(blockSize);
                outputBuffer.putInt(compressedSize);
                outputBuffer.position(outputBuffer.position() + compressedSize);
                offset += blockSize;
            }
            while (offset < bytes.length);
            return createDataOutput(Slices.wrappedBuffer(output, 0, outputBuffer.position()));
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
