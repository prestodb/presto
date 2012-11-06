/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.operator.Page;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.InputSupplier;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Arrays.asList;

public final class PagesSerde
{
    private PagesSerde()
    {
    }

    public static PagesWriter createPagesWriter(final SliceOutput sliceOutput)
    {
        checkNotNull(sliceOutput, "sliceOutput is null");
        return new PagesWriter()
        {
            private BlockEncoding[] blockEncodings;

            @Override
            public PagesWriter append(Page page)
            {
                Preconditions.checkNotNull(page, "page is null");

                if (blockEncodings == null) {
                    Block[] blocks = page.getBlocks();
                    blockEncodings = new BlockEncoding[blocks.length];
                    sliceOutput.writeInt(blocks.length);
                    for (int i = 0; i < blocks.length; i++) {
                        Block block = blocks[i];
                        BlockEncoding blockEncoding = block.getEncoding();
                        blockEncodings[i] = blockEncoding;
                        BlockEncodings.writeBlockEncoding(sliceOutput, blockEncoding);
                    }
                }

                Block[] blocks = page.getBlocks();
                for (int i = 0; i < blocks.length; i++) {
                    blockEncodings[i].writeBlock(sliceOutput, blocks[i]);
                }

                return this;
            }
        };
    }

    public static void writePages(SliceOutput sliceOutput, Page... pages)
    {
        writePages(sliceOutput, asList(pages).iterator());
    }

    public static void writePages(SliceOutput sliceOutput, Iterable<Page> pages)
    {
        writePages(sliceOutput, pages.iterator());
    }

    public static void writePages(SliceOutput sliceOutput, Iterator<Page> pages)
    {
        PagesWriter pagesWriter = createPagesWriter(sliceOutput);
        while (pages.hasNext()) {
            pagesWriter.append(pages.next());
        }
    }

    public Iterable<Page> readPages(final InputSupplier<SliceInput> sliceInputSupplier)
    {
        Preconditions.checkNotNull(sliceInputSupplier, "sliceInputSupplier is null");

        return new Iterable<Page>()
        {
            @Override
            public Iterator<Page> iterator()
            {
                try {
                    return readPages(sliceInputSupplier.getInput());
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }

    public static Iterator<Page> readPages(SliceInput sliceInput)
    {
        Preconditions.checkNotNull(sliceInput, "sliceInput is null");
        return new PagesReader(sliceInput, 0);
    }

    private static class PagesReader extends AbstractIterator<Page>
    {
        private final BlockEncoding[] blockEncodings;
        private final SliceInput sliceInput;
        private long positionOffset;

        public PagesReader(SliceInput sliceInput, long startPosition)
        {
            this.sliceInput = sliceInput;
            this.positionOffset = startPosition;

            int channelCount = sliceInput.readInt();

            blockEncodings = new BlockEncoding[channelCount];
            for (int i = 0; i < blockEncodings.length; i++) {
                blockEncodings[i] = BlockEncodings.readBlockEncoding(sliceInput);
            }
        }

        @Override
        protected Page computeNext()
        {
            if (!sliceInput.isReadable()) {
                return endOfData();
            }

            Block[] blocks = new Block[blockEncodings.length];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = blockEncodings[i].readBlock(sliceInput, positionOffset);
            }
            Page page = new Page(blocks);
            positionOffset += page.getPositionCount();
            return page;
        }
    }
}
