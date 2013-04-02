/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.Iterator;

public class EncodedBlockIterable implements BlockIterable
{
    private final BlockEncoding blockEncoding;
    private final Slice blocksSlice;
    private final int positionCount;

    public EncodedBlockIterable(BlockEncoding blockEncoding, Slice blocksSlice, int positionCount)
    {
        this.positionCount = positionCount;
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
    public Optional<DataSize> getDataSize()
    {
        return Optional.of(new DataSize(blocksSlice.length(), Unit.BYTE));
    }

    @Override
    public Optional<Integer> getPositionCount()
    {
        return Optional.of(positionCount);
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
