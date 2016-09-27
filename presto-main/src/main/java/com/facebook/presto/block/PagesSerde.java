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
package com.facebook.presto.block;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.collect.AbstractIterator;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Iterator;

import static com.facebook.presto.block.BlockSerdeUtil.readBlock;
import static com.facebook.presto.block.BlockSerdeUtil.writeBlock;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

// layout is:
//   - position count (int)
//   - number of blocks (int)
//   - sequence of:
//       - block encoding
//       - block
public final class PagesSerde
{
    private PagesSerde() {}

    public static long writePages(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Page... pages)
    {
        return writePages(blockEncodingSerde, sliceOutput, asList(pages).iterator());
    }

    public static long writePages(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Iterable<Page> pages)
    {
        return writePages(blockEncodingSerde, sliceOutput, pages.iterator());
    }

    public static long writePages(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Iterator<Page> pages)
    {
        long size = 0;
        PagesWriter pagesWriter = new PagesWriter(blockEncodingSerde, sliceOutput);
        while (pages.hasNext()) {
            Page page = pages.next();
            pagesWriter.append(page);
            size += page.getSizeInBytes();
        }
        return size;
    }

    public static Iterator<Page> readPages(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        return new PagesReader(blockEncodingSerde, sliceInput);
    }

    private static class PagesWriter
    {
        private final BlockEncodingSerde serde;
        private final SliceOutput output;

        private PagesWriter(BlockEncodingSerde serde, SliceOutput output)
        {
            this.serde = requireNonNull(serde, "serde is null");
            this.output = requireNonNull(output, "output is null");
        }

        public PagesWriter append(Page page)
        {
            requireNonNull(page, "page is null");

            Block[] blocks = page.getBlocks();

            output.writeInt(page.getPositionCount());
            output.writeInt(blocks.length);
            for (int i = 0; i < blocks.length; i++) {
                writeBlock(serde, output, blocks[i]);
            }

            return this;
        }
    }

    private static class PagesReader
            extends AbstractIterator<Page>
    {
        private final BlockEncodingSerde serde;
        private final SliceInput input;

        public PagesReader(BlockEncodingSerde serde, SliceInput input)
        {
            this.serde = requireNonNull(serde, "serde is null");
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        protected Page computeNext()
        {
            if (!input.isReadable()) {
                return endOfData();
            }

            int positions = input.readInt();
            int numberOfBlocks = input.readInt();
            Block[] blocks = new Block[numberOfBlocks];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = readBlock(serde, input);
            }

            @SuppressWarnings("UnnecessaryLocalVariable")
            Page page = new Page(positions, blocks);
            return page;
        }
    }
}
