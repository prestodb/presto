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
import com.facebook.presto.orc.stream.SharedBuffer;
import com.facebook.presto.orc.writer.CompressionBufferPool;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.NoopOrcLocalMemoryContext.NOOP_ORC_LOCAL_MEMORY_CONTEXT;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.Collections.reverse;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestOrcOutputBuffer
{
    private static final OrcDataSourceId DATA_SOURCE_ID = new OrcDataSourceId("test");

    private enum WriteMode
    {
        BYTES,
        SLICE
    }

    @Test
    public void testWriteHugeByteChucks()
    {
        int size = 1024 * 1024;
        byte[] largeByteArray = new byte[size];
        Arrays.fill(largeByteArray, (byte) 0xA);
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder().setCompressionKind(CompressionKind.NONE).build();
        OrcOutputBuffer orcOutputBuffer = new OrcOutputBuffer(columnWriterOptions, Optional.empty());

        DynamicSliceOutput output = new DynamicSliceOutput(size);
        orcOutputBuffer.writeBytes(largeByteArray, 10, size - 10);
        orcOutputBuffer.flush();
        assertEquals(orcOutputBuffer.writeDataTo(output), size - 10);
        assertEquals(output.slice(), wrappedBuffer(largeByteArray, 10, size - 10));

        orcOutputBuffer.reset();
        output.reset();
        orcOutputBuffer.writeBytes(wrappedBuffer(largeByteArray), 100, size - 100);
        orcOutputBuffer.flush();
        assertEquals(orcOutputBuffer.writeDataTo(output), size - 100);
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
                .setCompressionLevel(OptionalInt.of(7))
                .setCompressionMaxBufferSize(new DataSize(256, KILOBYTE))
                .build();
        OrcOutputBuffer orcOutputBuffer = new OrcOutputBuffer(columnWriterOptions, Optional.empty());

        // Before the fix the compressed result would be around 90KB, after the fix it went down to 117 bytes.
        DynamicSliceOutput output = new DynamicSliceOutput(size);
        orcOutputBuffer.writeBytes(largeByteArray, 10, size - 10);
        orcOutputBuffer.flush();
        assertTrue(orcOutputBuffer.writeDataTo(output) < 200);

        orcOutputBuffer.reset();
        output.reset();
        orcOutputBuffer.writeBytes(wrappedBuffer(largeByteArray), 100, size - 100);
        orcOutputBuffer.flush();
        assertTrue(orcOutputBuffer.writeDataTo(output) < 200);
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
        assertEquals(sliceOutput.getBufferCapacity(), 400 + 1200);

        // make the buffer to reach the max buffer capacity
        sliceOutput.writeBytes(largeByteArray, 0, 2500);
        assertEquals(sliceOutput.getBufferCapacity(), 3000);

        // make sure we didn't miss anything
        DynamicSliceOutput output = new DynamicSliceOutput(6000);
        sliceOutput.close();
        assertEquals(sliceOutput.writeDataTo(output), 200 + 200 + 1200 + 2500);
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

    @Test
    public void testWriteZeros()
    {
        DataSize chunkSize = new DataSize(700, BYTE);
        DataSize maxCompressionSize = new DataSize(chunkSize.toBytes() + 3, BYTE); // 3 accounts for the chunk header size
        DataSize dataSize = new DataSize(2 * 700 + 10, BYTE);
        byte[] testData = new byte[(int) dataSize.toBytes()];
        Arrays.fill(testData, (byte) 0);

        List<DataSize> expectedChunkSizes = buildExpectedChunks(chunkSize, dataSize);
        List<List<DataSize>> allWritePieces = buildWriteChunksCombos(dataSize, 3);

        for (List<DataSize> pieces : allWritePieces) {
            OrcOutputBuffer sliceOutput = createOrcOutputBuffer(maxCompressionSize);
            for (DataSize piece : pieces) {
                sliceOutput.writeZero((int) piece.toBytes());
            }
            sliceOutput.close();
            assertCompressedContent(sliceOutput, testData, expectedChunkSizes);
        }
    }

    @Test
    public void testWriteBytesFromInputStream()
            throws IOException
    {
        DataSize chunkSize = new DataSize(700, BYTE);
        DataSize maxCompressionSize = new DataSize(chunkSize.toBytes() + 3, BYTE); // 3 accounts for the chunk header size
        DataSize dataSize = new DataSize(2 * 700 + 10, BYTE);
        byte[] testData = createTestData(dataSize);
        InputStream inputStream = new ByteArrayInputStream(testData);

        List<DataSize> expectedChunkSizes = buildExpectedChunks(chunkSize, dataSize);
        List<List<DataSize>> allWriteChunks = buildWriteChunksCombos(dataSize, 3);

        for (List<DataSize> writeChunks : allWriteChunks) {
            OrcOutputBuffer sliceOutput = createOrcOutputBuffer(maxCompressionSize);
            inputStream.reset();

            for (DataSize writeChunk : writeChunks) {
                sliceOutput.writeBytes(inputStream, (int) writeChunk.toBytes());
            }

            assertCompressedContent(sliceOutput, testData, expectedChunkSizes);
        }
    }

    @Test
    public void testWriteBytes()
    {
        DataSize chunkSize = new DataSize(700, BYTE);
        DataSize maxCompressionSize = new DataSize(chunkSize.toBytes() + 3, BYTE); // 3 accounts for the chunk header size
        DataSize dataSize = new DataSize(2 * 700 + 10, BYTE);
        byte[] testData = createTestData(dataSize);

        List<DataSize> expectedChunkSizes = buildExpectedChunks(chunkSize, dataSize);
        List<List<DataSize>> allWriteChunks = buildWriteChunksCombos(dataSize, 3);

        for (List<DataSize> writeChunks : allWriteChunks) {
            assertWriteBytes(testData, maxCompressionSize, writeChunks, expectedChunkSizes);
        }
    }

    @Test
    public void testWriteBytesSliceView()
    {
        DataSize chunkSize = new DataSize(700, BYTE);
        DataSize maxCompressionSize = new DataSize(chunkSize.toBytes() + 3, BYTE); // 3 accounts for the chunk header size
        DataSize dataSize = new DataSize(2 * 700 + 10, BYTE);

        // create a slice view on top of a larger array of data that has the actual test data in the middle
        byte[] testData = createTestData(dataSize);
        byte[] testDataEx = new byte[testData.length + 10];
        System.arraycopy(testData, 0, testDataEx, 5, testData.length);
        Slice slice = wrappedBuffer(testDataEx).slice(5, testData.length);

        List<DataSize> expectedChunkSizes = buildExpectedChunks(chunkSize, dataSize);
        List<DataSize> writeChunks = new ArrayList<>(expectedChunkSizes);
        reverse(writeChunks);

        OrcOutputBuffer orcOutputBuffer = createOrcOutputBuffer(maxCompressionSize);
        int offset = 0;
        for (DataSize size : writeChunks) {
            int sizeBytes = (int) size.toBytes();
            orcOutputBuffer.writeBytes(slice, offset, sizeBytes);
            offset += sizeBytes;
        }
        assertCompressedContent(orcOutputBuffer, testData, expectedChunkSizes);
    }

    @Test
    public void testWriteBytesEmptySlice()
    {
        OrcOutputBuffer orcOutputBuffer = createOrcOutputBuffer(new DataSize(256, KILOBYTE));
        orcOutputBuffer.writeBytes(EMPTY_SLICE); // EMPTY_SLICE has null byte buffer
        assertCompressedContent(orcOutputBuffer, new byte[0], ImmutableList.of());

        orcOutputBuffer = createOrcOutputBuffer(new DataSize(256, KILOBYTE));
        orcOutputBuffer.writeBytes(EMPTY_SLICE, 0, 0);
        assertCompressedContent(orcOutputBuffer, new byte[0], ImmutableList.of());
    }

    @Test
    public void testWriteBytesEmptyBytes()
    {
        OrcOutputBuffer orcOutputBuffer = createOrcOutputBuffer(new DataSize(256, KILOBYTE));
        orcOutputBuffer.writeBytes(new byte[0]); // EMPTY_SLICE has null byte buffer
        assertCompressedContent(orcOutputBuffer, new byte[0], ImmutableList.of());

        orcOutputBuffer = createOrcOutputBuffer(new DataSize(256, KILOBYTE));
        orcOutputBuffer.writeBytes(new byte[0], 0, 0);
        assertCompressedContent(orcOutputBuffer, new byte[0], ImmutableList.of());
    }

    private void assertWriteBytes(byte[] byteArray, DataSize maxCompressionBufferSize, List<DataSize> writeChunks, List<DataSize> expectedDecompressedChunks)
    {
        OrcOutputBuffer orcOutputBufferBytes = writeToOrcOutputBuffer(byteArray, maxCompressionBufferSize, WriteMode.BYTES, writeChunks);
        assertCompressedContent(orcOutputBufferBytes, byteArray, expectedDecompressedChunks);

        OrcOutputBuffer orcOutputBufferSlice = writeToOrcOutputBuffer(byteArray, maxCompressionBufferSize, WriteMode.SLICE, writeChunks);
        assertCompressedContent(orcOutputBufferSlice, byteArray, expectedDecompressedChunks);
    }

    private OrcOutputBuffer writeToOrcOutputBuffer(byte[] bytes, DataSize maxBufferSize, WriteMode writeMode, List<DataSize> sizes)
    {
        OrcOutputBuffer orcOutputBuffer = createOrcOutputBuffer(maxBufferSize);
        int offset = 0;
        Slice slice = wrappedBuffer(bytes);
        for (DataSize size : sizes) {
            int sizeBytes = (int) size.toBytes();
            if (writeMode == WriteMode.BYTES) {
                orcOutputBuffer.writeBytes(bytes, offset, sizeBytes);
            }
            else {
                orcOutputBuffer.writeBytes(slice, offset, sizeBytes);
            }
            offset += sizeBytes;
        }

        orcOutputBuffer.flush();
        return orcOutputBuffer;
    }

    private void assertCompressedContent(OrcOutputBuffer orcOutputBuffer, byte[] expectedBytes, List<DataSize> expectedChunkSizes)
    {
        orcOutputBuffer.flush();
        DynamicSliceOutput output = new DynamicSliceOutput(orcOutputBuffer.size());
        orcOutputBuffer.writeDataTo(output);

        List<Integer> expectedSizes = expectedChunkSizes.stream().map(size -> (int) size.toBytes()).collect(toList());

        DecompressionResult result = decompress(output.slice());
        assertEquals(wrappedBuffer(result.bytes), wrappedBuffer(expectedBytes));
        assertEquals(result.sizes, expectedSizes);
    }

    private DecompressionResult decompress(Slice slice)
    {
        OrcDecompressor decompressor = OrcDecompressor.createOrcDecompressor(DATA_SOURCE_ID, ZSTD, 256 * 1024).get();
        SharedBuffer decompressionBuffer = new SharedBuffer(NOOP_ORC_LOCAL_MEMORY_CONTEXT);
        ImmutableList.Builder<Integer> sizes = ImmutableList.builder();
        BasicSliceInput input = slice.getInput();
        DecompressorOutputBuffer decompressorOutputBuffer = new DecompressorOutputBuffer();
        ByteArrayOutputStream decompressedStream = new ByteArrayOutputStream();

        while (input.isReadable()) {
            int b0 = input.readUnsignedByte();
            int b1 = input.readUnsignedByte();
            int b2 = input.readUnsignedByte();
            int chunkLength = (b2 << 15) | (b1 << 7) | (b0 >>> 1);
            boolean isUncompressed = (b0 & 0x01) == 1;

            decompressionBuffer.ensureCapacity(chunkLength);
            byte[] compressedBuffer = decompressionBuffer.get();
            int readCompressed = input.read(compressedBuffer, 0, chunkLength);
            if (isUncompressed) {
                decompressedStream.write(compressedBuffer, 0, chunkLength);
                sizes.add(chunkLength);
            }
            else {
                int length = decompressor.decompress(compressedBuffer, 0, readCompressed, decompressorOutputBuffer);
                decompressedStream.write(decompressorOutputBuffer.buffer, 0, length);
                sizes.add(length);
            }
        }
        return new DecompressionResult(sizes.build(), decompressedStream.toByteArray());
    }

    /**
     * Create OrcOutputBuffer with a given compression buffer size.
     */
    private static OrcOutputBuffer createOrcOutputBuffer(DataSize compressionMaxBufferSize)
    {
        ColumnWriterOptions columnWriterOptions = ColumnWriterOptions.builder()
                .setCompressionKind(ZSTD)
                .setCompressionLevel(OptionalInt.of(1))
                .setCompressionMaxBufferSize(compressionMaxBufferSize)
                .build();
        return new OrcOutputBuffer(columnWriterOptions, Optional.empty());
    }

    /**
     * Creates a list with all possible combinations of ranges with the sizes summing up to dataSize.
     */
    private static List<List<DataSize>> buildWriteChunksCombos(DataSize dataSize, int maxDepth)
    {
        int totalSize = (int) dataSize.toBytes();
        ImmutableList.Builder<List<DataSize>> builder = ImmutableList.builder();

        for (int levels = 1; levels <= maxDepth; levels++) {
            DataSize[] bus = new DataSize[levels];
            buildWriteChunksCombos(totalSize, levels - 1, bus, builder);
        }

        return builder.build();
    }

    private static void buildWriteChunksCombos(int remaining, int level, DataSize[] bus, ImmutableList.Builder<List<DataSize>> builder)
    {
        if (level == 0) {
            bus[level] = new DataSize(remaining, BYTE);
            builder.add(ImmutableList.copyOf(bus));
            return;
        }
        for (int i = 1; i < remaining; i++) {
            bus[level] = new DataSize(i, BYTE);
            buildWriteChunksCombos(remaining - i, level - 1, bus, builder);
        }
    }

    /**
     * Splits totalSize into a list of chunks with the total size of totalSize.
     */
    private static List<DataSize> buildExpectedChunks(DataSize chunkSize, DataSize totalSize)
    {
        long totalSizeBytes = totalSize.toBytes();
        long chunkSizeBytes = chunkSize.toBytes();
        ImmutableList.Builder<DataSize> builder = ImmutableList.builder();
        while (totalSizeBytes > 0) {
            long next = Math.min(chunkSizeBytes, totalSizeBytes);
            builder.add(new DataSize(next, BYTE));
            totalSizeBytes -= next;
        }
        return builder.build();
    }

    private static byte[] createTestData(DataSize size)
    {
        int sizeInBytes = (int) size.toBytes();
        byte[] bytes = new byte[sizeInBytes];
        for (int i = 0; i < sizeInBytes; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
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

    private static class DecompressionResult
    {
        // sizes of decompressed chunks
        final List<Integer> sizes;

        // decompressed data
        final byte[] bytes;

        public DecompressionResult(List<Integer> sizes, byte[] bytes)
        {
            this.sizes = sizes;
            this.bytes = bytes;
        }
    }

    private static class DecompressorOutputBuffer
            implements OrcDecompressor.OutputBuffer
    {
        byte[] buffer;

        @Override
        public byte[] initialize(int size)
        {
            buffer = ensureCapacity(buffer, size);
            return buffer;
        }

        @Override
        public byte[] grow(int size)
        {
            if (size > buffer.length) {
                buffer = Arrays.copyOfRange(buffer, 0, size);
            }
            return buffer;
        }
    }
}
