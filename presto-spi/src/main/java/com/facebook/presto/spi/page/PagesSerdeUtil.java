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
import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.net.Inet6Address;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static com.facebook.presto.common.block.BlockSerdeUtil.readBlock;
import static com.facebook.presto.common.block.BlockSerdeUtil.writeBlock;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.page.PageCodecMarker.CHECKSUMMED;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class PagesSerdeUtil
{
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
        output.writeInt(page.getPositionCount());
        output.writeByte(page.getPageCodecMarkers());
        output.writeInt(page.getUncompressedSizeInBytes());
        output.writeInt(page.getSizeInBytes());
        if (CHECKSUMMED.isSet(page.getPageCodecMarkers())) {
            output.writeLong(page.getChecksum());
            try {
                String host = Inet6Address.getLocalHost().getHostAddress();
                output.writeInt(host.length());
                output.writeBytes(host.getBytes(StandardCharsets.UTF_8));
            }
            catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }
        }
        output.writeBytes(page.getSlice());
    }

    public static SerializedPage readSerializedPage(SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        byte codecMarker = sliceInput.readByte();
        int uncompressedSizeInBytes = sliceInput.readInt();
        int sizeInBytes = sliceInput.readInt();
        long checksum = 0;
        String host = "";
        if (CHECKSUMMED.isSet(codecMarker)) {
            checksum = sliceInput.readLong();
            int length = sliceInput.readInt();
            byte[] hostAddress = new byte[length];
            sliceInput.read(hostAddress);
            host = new String(hostAddress);
        }
        Slice slice = sliceInput.readSlice(toIntExact((sizeInBytes)));
        SerializedPage page = new SerializedPage(slice, codecMarker, positionCount, uncompressedSizeInBytes);

        if (CHECKSUMMED.isSet(codecMarker)) {
            if (checksum != page.getChecksum()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Incorrect checksum reading serialized page from host %s", host));
            }
        }
        return page;
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
