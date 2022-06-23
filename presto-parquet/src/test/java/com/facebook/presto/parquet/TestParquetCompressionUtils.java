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

import io.airlift.compress.Compressor;
import io.airlift.compress.lz4.Lz4Compressor;
import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.slice.Slice;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.testng.annotations.Test;

import java.io.IOException;

import static io.airlift.slice.Slices.wrappedBuffer;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZ4;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD;
import static org.testng.Assert.assertEquals;

public class TestParquetCompressionUtils
{
    @Test
    public void testDecompressZSTD()
            throws IOException
    {
        performTest(ZSTD, 0);
        performTest(ZSTD, 1);
        performTest(ZSTD, 100);
        performTest(ZSTD, 256);
        performTest(ZSTD, 512);
        performTest(ZSTD, 1024);
    }

    @Test
    public void testDecompressLZ4()
            throws IOException
    {
        performTest(LZ4, 0);
        performTest(LZ4, 1);
        performTest(LZ4, 100);
        performTest(LZ4, 256);
        performTest(LZ4, 512);
        performTest(LZ4, 1024);
    }

    private void performTest(CompressionCodecName codec, int inputLength)
            throws IOException
    {
        Compressor compressor = null;
        if (codec.equals(ZSTD)) {
            compressor = new ZstdCompressor();
        }
        else if (codec.equals(LZ4)) {
            compressor = new Lz4Compressor();
        }

        byte[] input = createArray(inputLength);
        byte[] output = new byte[inputLength + 512];
        int retLength = compress(compressor, input, inputLength, output, 0);

        Slice decompressedSlice = ParquetCompressionUtils.decompress(codec, wrappedBuffer(output, 0, retLength), inputLength);
        assertEquals(decompressedSlice, wrappedBuffer(input));
    }

    private byte[] createArray(int length)
    {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (i % 256);
        }
        return data;
    }

    private static int compress(Compressor compressor, byte[] byteArray, int inputLength, byte[] output, int outputOffset)
    {
        return compressor.compress(byteArray, 0, inputLength, output, outputOffset, output.length - outputOffset);
    }
}
