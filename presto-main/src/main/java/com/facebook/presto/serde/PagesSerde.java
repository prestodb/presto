/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
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
            private BlockSerde[] blockSerdes;

            @Override
            public PagesWriter append(Page page)
            {
                Preconditions.checkNotNull(page, "page is null");

                if (blockSerdes == null) {
                    Block[] blocks = page.getBlocks();
                    blockSerdes = new BlockSerde[blocks.length];
                    sliceOutput.writeInt(blocks.length);
                    for (int i = 0; i < blocks.length; i++) {
                        Block block = blocks[i];
                        BlockSerde blockSerde = getSerdeForBlock(block);
                        blockSerdes[i] = blockSerde;

                        BlockSerdeSerde.writeBlockSerde(sliceOutput, blockSerde);
                        TupleInfoSerde.writeTupleInfo(sliceOutput, block.getTupleInfo());
                    }
                }

                Block[] blocks = page.getBlocks();
                for (int i = 0; i < blocks.length; i++) {
                    blockSerdes[i].writeBlock(sliceOutput, blocks[i]);
                }

                return this;
            }

            @Override
            public void finish()
            {
            }
        };
    }

    private static BlockSerde getSerdeForBlock(Block block)
    {
        BlockSerde blockSerde;
        if (block instanceof UncompressedBlock) {
            blockSerde = new UncompressedBlockSerde();
        } else {
            throw new IllegalArgumentException("Unsupported block type " + block.getClass().getSimpleName());
        }
        return blockSerde;
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
        pagesWriter.finish();
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
        private final BlockSerde[] blockSerdes;
        private final TupleInfo[] tupleInfos;
        private final SliceInput sliceInput;
        private long positionOffset;

        public PagesReader(SliceInput sliceInput, long startPosition)
        {
            this.sliceInput = sliceInput;
            this.positionOffset = startPosition;

            int channelCount = sliceInput.readInt();
            blockSerdes = new BlockSerde[channelCount];
            tupleInfos = new TupleInfo[channelCount];
            for (int i = 0; i < blockSerdes.length; i++) {
                blockSerdes[i] = BlockSerdeSerde.readBlockSerde(sliceInput);
                tupleInfos[i] = TupleInfoSerde.readTupleInfo(sliceInput);
            }
        }

        @Override
        protected Page computeNext()
        {
            if (!sliceInput.isReadable()) {
                return endOfData();
            }

            Block[] blocks = new Block[tupleInfos.length];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = blockSerdes[i].readBlock(sliceInput, tupleInfos[i], positionOffset);
            }
            Page page = new Page(blocks);
            positionOffset += page.getPositionCount();
            return page;
        }
    }
}
