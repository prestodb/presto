/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

public class EncodedBlockIterable implements BlockIterable
{
    private final BlockEncoding blockEncoding;
    private final Slice blocksSlice;

    public EncodedBlockIterable(BlockEncoding blockEncoding, Slice blocksSlice)
    {
        Preconditions.checkNotNull(blockEncoding, "blockEncoding is null");
        Preconditions.checkNotNull(blocksSlice, "blocksSlice is null");

        this.blockEncoding = blockEncoding;
        this.blocksSlice = blocksSlice;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return blockEncoding.getTupleInfo();
    }

    @Override
    public Iterator<Block> iterator()
    {
        return new EncodedBlockIterator(blockEncoding, blocksSlice.getInput());
    }

    private static class EncodedBlockIterator
            extends AbstractIterator<Block>
    {
        private final BlockEncoding blockEncoding;
        private final SliceInput sliceInput;

        private EncodedBlockIterator(BlockEncoding blockEncoding, SliceInput sliceInput)
        {
            this.blockEncoding = blockEncoding;
            this.sliceInput = sliceInput;
        }

        protected Block computeNext()
        {
            if (!sliceInput.isReadable()) {
                return endOfData();
            }

            Block block = blockEncoding.readBlock(sliceInput);
            return block;
        }
    }
}
