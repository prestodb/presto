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
package com.facebook.presto.orc;

import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.writer.CompressionBufferPool;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.orc.CompressionLevel.BEST_COMPRESSION;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOrcOutputBuffer
{
    @Test
    public void testWriteHugeByteChucks()
    {
        int size = 1024 * 1024;
        byte[] largeByteArray = new byte[size];
        Arrays.fill(largeByteArray, (byte) 0xA);
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder().setCompressionKind(CompressionKind.NONE).build();
        OrcOutputBuffer sliceOutput = new OrcOutputBuffer(columnWriterOptions, Optional.empty());

        DynamicSliceOutput output = new DynamicSliceOutput(size);
        sliceOutput.writeBytes(largeByteArray, 10, size - 10);
        assertEquals(sliceOutput.writeDataTo(output), size - 10);
        assertEquals(output.slice(), wrappedBuffer(largeByteArray, 10, size - 10));

        sliceOutput.reset();
        output.reset();
        sliceOutput.writeBytes(wrappedBuffer(largeByteArray), 100, size - 100);
        assertEquals(sliceOutput.writeDataTo(output), size - 100);
        assertEquals(output.slice(), wrappedBuffer(largeByteArray, 100, size - 100));
    }

    @Test
    public void testWriteHugeByteChucksUsesMaxCompressionBufferSizeChunks()
    {
        int size = 1024 * 1024;
        byte[] largeByteArray = new byte[size];
        Arrays.fill(largeByteArray, (byte) 0xA);
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(ZSTD)
                .setCompressionLevel(BEST_COMPRESSION)
                .setCompressionMaxBufferSize(new DataSize(256, KILOBYTE))
                .build();
        OrcOutputBuffer sliceOutput = new OrcOutputBuffer(columnWriterOptions, Optional.empty());

        // Before the fix the compressed result would be around 90KB, after the fix it went down to 117 bytes.
        DynamicSliceOutput output = new DynamicSliceOutput(size);
        sliceOutput.writeBytes(largeByteArray, 10, size - 10);
        assertTrue(sliceOutput.writeDataTo(output) < 200);

        sliceOutput.reset();
        output.reset();
        sliceOutput.writeBytes(wrappedBuffer(largeByteArray), 100, size - 100);
        assertTrue(sliceOutput.writeDataTo(output) < 200);
    }

    @Test
    public void testGrowCapacity()
    {
        byte[] largeByteArray = new byte[4096];
        DataSize maxCompressionSize = new DataSize(3000, BYTE);
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(CompressionKind.NONE)
                .setCompressionMaxBufferSize(maxCompressionSize)
                .build();
        OrcOutputBuffer sliceOutput = new OrcOutputBuffer(columnWriterOptions, Optional.empty());

        // write some data that can fit the initial capacity = 256
        sliceOutput.writeBytes(largeByteArray, 0, 200);
        assertEquals(sliceOutput.getBufferCapacity(), 256);

        // write some more data to exceed the capacity = 256; the capacity will double
        sliceOutput.writeBytes(largeByteArray, 0, 200);
        assertEquals(sliceOutput.getBufferCapacity(), 512);

        // write a lot more data to exceed twice the capacity = 512 X 2; the capacity will be the required data size
        sliceOutput.writeBytes(largeByteArray, 0, 1200);
        assertEquals(sliceOutput.getBufferCapacity(), 1200);

        // write some more data to double the capacity again
        sliceOutput.writeBytes(largeByteArray, 0, 2000);
        assertEquals(sliceOutput.getBufferCapacity(), 2400);

        // make the buffer to reach the max buffer capacity
        sliceOutput.writeBytes(largeByteArray, 0, 2500);
        assertEquals(sliceOutput.getBufferCapacity(), 3000);

        // make sure we didn't miss anything
        DynamicSliceOutput output = new DynamicSliceOutput(6000);
        sliceOutput.close();
        assertEquals(sliceOutput.writeDataTo(output), 200 + 200 + 1200 + 2000 + 2500);
    }

    @Test
    public void testMinCompressibleSize()
    {
        int size = 1024;
        byte[] byteArray = new byte[size];
        Arrays.fill(byteArray, (byte) 0xA);

        CapturingCompressionBufferPool pool = new CapturingCompressionBufferPool();

        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(ZSTD)
                .setCompressionBufferPool(pool)
                .build();

        // if input size is below minCompressibleSize then OrcOutputBuffer won't attempt to compress
        OrcOutputBuffer orcOutputBuffer = new OrcOutputBuffer(columnWriterOptions, Optional.empty());
        orcOutputBuffer.writeBytes(byteArray, 0, ZSTD.getMinCompressibleSize() - 1);
        orcOutputBuffer.flush();
        assertEquals(pool.getLastUsedSize(), 0);

        // if input size equals or greater than minCompressibleSize then OrcOutputBuffer would attempt to compress
        orcOutputBuffer.reset();
        orcOutputBuffer.writeBytes(byteArray, 0, ZSTD.getMinCompressibleSize());
        orcOutputBuffer.flush();
        assertTrue(pool.getLastUsedSize() > ZSTD.getMinCompressibleSize());

        orcOutputBuffer.reset();
        orcOutputBuffer.writeBytes(byteArray, 0, ZSTD.getMinCompressibleSize() + 10);
        orcOutputBuffer.flush();
        assertTrue(pool.getLastUsedSize() > ZSTD.getMinCompressibleSize() + 10);
    }

    private static class CapturingCompressionBufferPool
            implements CompressionBufferPool
    {
        private int lastUsedSize;

        public int getLastUsedSize()
        {
            return lastUsedSize;
        }

        @Override
        public byte[] checkOut(int length)
        {
            this.lastUsedSize = length;
            return new byte[length];
        }

        @Override
        public void checkIn(byte[] buffer)
        {
            // do nothing
        }

        @Override
        public long getRetainedBytes()
        {
            return 0;
        }
    }
}
