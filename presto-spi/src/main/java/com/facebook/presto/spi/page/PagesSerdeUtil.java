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
package com.facebook.presto.spi.page;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.zip.CRC32;

import static com.facebook.presto.common.block.BlockSerdeUtil.readBlock;
import static com.facebook.presto.common.block.BlockSerdeUtil.writeBlock;
import static com.facebook.presto.spi.page.PageCodecMarker.CHECKSUMMED;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class PagesSerdeUtil
{
    public static final int PAGE_METADATA_SIZE = SIZE_OF_INT * 3 + SIZE_OF_BYTE + SIZE_OF_LONG;

    private PagesSerdeUtil()
    {
    }

    static void writeRawPage(Page page, SliceOutput output, BlockEncodingSerde serde)
    {
        output.writeInt(page.getChannelCount());
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            writeBlock(serde, output, page.getBlock(channel));
        }
    }

    static Page readRawPage(int positionCount, SliceInput input, BlockEncodingSerde blockEncodingSerde)
    {
        int numberOfBlocks = input.readInt();
        Block[] blocks = new Block[numberOfBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = readBlock(blockEncodingSerde, input);
        }

        return new Page(positionCount, blocks);
    }

    public static void writeSerializedPage(SliceOutput output, SerializedPage page)
    {
        writeSerializedPageMetadata(output, page);
        output.writeBytes(page.getSlice());
    }

    public static void writeSerializedPageMetadata(SliceOutput output, SerializedPage page)
    {
        output.writeInt(page.getPositionCount());
        output.writeByte(page.getPageCodecMarkers());
        output.writeInt(page.getUncompressedSizeInBytes());
        output.writeInt(page.getSizeInBytes());
        output.writeLong(page.getChecksum());
    }

    public static SerializedPage readSerializedPage(SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        byte codecMarker = sliceInput.readByte();
        int uncompressedSizeInBytes = sliceInput.readInt();
        int sizeInBytes = sliceInput.readInt();
        long checksum = sliceInput.readLong();
        Slice slice = sliceInput.readSlice(toIntExact((sizeInBytes)));
        return new SerializedPage(slice, codecMarker, positionCount, uncompressedSizeInBytes, checksum);
    }

    public static long writeSerializedPages(SliceOutput sliceOutput, Iterable<SerializedPage> pages)
    {
        Iterator<SerializedPage> pageIterator = pages.iterator();
        long size = 0;
        while (pageIterator.hasNext()) {
            SerializedPage page = pageIterator.next();
            writeSerializedPage(sliceOutput, page);
            size += page.getSizeInBytes();
        }
        return size;
    }

    private static void updateCrc(CRC32 crc32, int value)
    {
        for (int i = 0; i < 32; i += 8) {
            crc32.update(value >> i);
        }
    }

    public static long computeSerializedPageChecksum(Slice pageData, byte markers, int positionCount, int uncompressedSize)
    {
        CRC32 crc32 = new CRC32();
        if (!pageData.hasByteArray()) {
            throw new IllegalArgumentException("pageData slice is expected to be based on byte array");
        }
        crc32.update(pageData.byteArray(), pageData.byteArrayOffset(), pageData.length());
        crc32.update(markers);
        updateCrc(crc32, positionCount);
        updateCrc(crc32, uncompressedSize);
        return crc32.getValue();
    }

    public static boolean isChecksumValid(SerializedPage serializedPage)
    {
        long actualChecksum = 0;
        if (CHECKSUMMED.isSet(serializedPage.getPageCodecMarkers())) {
            actualChecksum = computeSerializedPageChecksum(
                    serializedPage.getSlice(),
                    serializedPage.getPageCodecMarkers(),
                    serializedPage.getPositionCount(),
                    serializedPage.getUncompressedSizeInBytes());
        }
        long expectedChecksum = serializedPage.getChecksum();
        return actualChecksum == expectedChecksum;
    }

    public static long writePages(PagesSerde serde, SliceOutput sliceOutput, Page... pages)
    {
        return writePages(serde, sliceOutput, asList(pages).iterator());
    }

    public static long writePages(PagesSerde serde, SliceOutput sliceOutput, Iterator<Page> pages)
    {
        long size = 0;
        while (pages.hasNext()) {
            Page page = pages.next();
            writeSerializedPage(sliceOutput, serde.serialize(page));
            size += page.getSizeInBytes();
        }
        return size;
    }

    public static Iterator<Page> readPages(PagesSerde serde, SliceInput sliceInput)
    {
        return new PageReader(serde, sliceInput);
    }

    private static class PageReader
            implements Iterator<Page>
    {
        private final PagesSerde serde;
        private final SliceInput input;

        PageReader(PagesSerde serde, SliceInput input)
        {
            this.serde = requireNonNull(serde, "serde is null");
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public boolean hasNext()
        {
            return input.isReadable();
        }

        @Override
        public Page next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return serde.deserialize(readSerializedPage(input));
        }
    }

    public static Iterator<SerializedPage> readSerializedPages(SliceInput sliceInput)
    {
        return new SerializedPageReader(sliceInput);
    }

    private static class SerializedPageReader
            implements Iterator<SerializedPage>
    {
        private final SliceInput input;

        SerializedPageReader(SliceInput input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        public boolean hasNext()
        {
            return input.isReadable();
        }

        @Override
        public SerializedPage next()
        {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return readSerializedPage(input);
        }
    }
}
