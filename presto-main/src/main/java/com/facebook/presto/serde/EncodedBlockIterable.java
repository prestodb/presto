/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class EncodedBlockIterable implements BlockIterable
{
    private final BlockEncoding blockEncoding;
    private final Slice blocksSlice;
    private final long positionOffset;

    public EncodedBlockIterable(BlockEncoding blockEncoding, Slice blocksSlice, long positionOffset)
    {
        Preconditions.checkNotNull(blockEncoding, "blockEncoding is null");
        Preconditions.checkNotNull(blocksSlice, "blocksSlice is null");
        Preconditions.checkArgument(positionOffset >= 0, "positionOffset is negative");

        this.blockEncoding = blockEncoding;
        this.blocksSlice = blocksSlice;
        this.positionOffset = positionOffset;
    }

    @Override
    public Iterator<Block> iterator()
    {
        return new EncodedBlockIterator(blockEncoding, blocksSlice.getInput(), positionOffset);
    }

    private static class EncodedBlockIterator
            extends AbstractIterator<Block>
    {
        private final BlockEncoding blockEncoding;
        private final SliceInput sliceInput;
        private final long positionOffset;

        private EncodedBlockIterator(BlockEncoding blockEncoding, SliceInput sliceInput, long positionOffset)
        {
            this.blockEncoding = blockEncoding;
            this.sliceInput = sliceInput;
            this.positionOffset = positionOffset;
        }

        protected Block computeNext()
        {
            if (!sliceInput.isReadable()) {
                return endOfData();
            }

            Block block = blockEncoding.readBlock(sliceInput, positionOffset);
            return block;
        }
    }
}
