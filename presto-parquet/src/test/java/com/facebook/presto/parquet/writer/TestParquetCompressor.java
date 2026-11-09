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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static com.facebook.presto.parquet.ParquetCompressionUtils.decompress;
import static io.airlift.slice.Slices.wrappedBuffer;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZ4;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZ4_RAW;
import static org.testng.Assert.assertEquals;

public class TestParquetCompressor
{
    private static final int[] INPUT_LENGTHS = {0, 1, 1000, 128 * 1024, 128 * 1024 + 1, 1024 * 1024 + 7};

    @Test
    public void testLz4RawRoundTrip()
            throws IOException
    {
        for (int length : INPUT_LENGTHS) {
            byte[] input = createInput(length);
            assertEquals(decompress(LZ4_RAW, compress(LZ4_RAW, input), length), wrappedBuffer(input));
        }
    }

    @Test
    public void testLz4RoundTrip()
            throws IOException
    {
        for (int length : INPUT_LENGTHS) {
            byte[] input = createInput(length);
            assertEquals(decompress(LZ4, compress(LZ4, input), length), wrappedBuffer(input));
        }
    }

    @Test
    public void testLz4IsReadableByHadoopLz4Codec()
            throws IOException
    {
        Lz4Codec codec = new Lz4Codec();
        codec.setConf(new Configuration(false));
        for (int length : INPUT_LENGTHS) {
            byte[] input = createInput(length);
            byte[] output = new byte[length];
            try (CompressionInputStream stream = codec.createInputStream(new ByteArrayInputStream(compress(LZ4, input).getBytes()))) {
                int offset = 0;
                while (offset < length) {
                    int read = stream.read(output, offset, length - offset);
                    if (read < 0) {
                        break;
                    }
                    offset += read;
                }
                assertEquals(offset, length);
                assertEquals(stream.read(), -1);
            }
            assertEquals(wrappedBuffer(output), wrappedBuffer(input));
        }
    }

    @Test
    public void testLz4FramesEveryBlockOf128KB()
            throws IOException
    {
        // Hadoop's Lz4Decompressor buffers a block in 256KB by default, so a 1MB page must be split into 128KB blocks.
        Slice compressed = compress(LZ4, createInput(1024 * 1024 + 7));
        int position = 0;
        int blocks = 0;
        while (position < compressed.length()) {
            int uncompressedLength = Integer.reverseBytes(compressed.getInt(position));
            int compressedLength = Integer.reverseBytes(compressed.getInt(position + Integer.BYTES));
            assertEquals(uncompressedLength, blocks < 8 ? 128 * 1024 : 7);
            position += 2 * Integer.BYTES + compressedLength;
            blocks++;
        }
        assertEquals(position, compressed.length());
        assertEquals(blocks, 9);
    }

    private static Slice compress(CompressionCodecName codec, byte[] input)
            throws IOException
    {
        ParquetDataOutput output = ParquetCompressor.getCompressor(codec).compress(BytesInput.from(input));
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(input.length + 64);
        output.writeData(sliceOutput);
        assertEquals(sliceOutput.size(), output.size());
        return sliceOutput.slice();
    }

    private static byte[] createInput(int length)
    {
        // Mix repeated runs with varying bytes so the blocks compress, but not to nothing.
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) ((i / 7) % 251);
        }
        return data;
    }
}
