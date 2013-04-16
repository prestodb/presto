/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.snappy.SnappyBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class SnappyBlockEncoding
        implements BlockEncoding
{
    private final TupleInfo tupleInfo;

    public SnappyBlockEncoding(SliceInput input)
    {
        Preconditions.checkNotNull(input, "input is null");
        this.tupleInfo = TupleInfoSerde.readTupleInfo(input);
    }

    public SnappyBlockEncoding(TupleInfo tupleInfo)
    {
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        this.tupleInfo = tupleInfo;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        SnappyBlock snappyBlock = (SnappyBlock) block;
        Preconditions.checkArgument(block.getTupleInfo().equals(tupleInfo), "Invalid tuple info");

        Slice slice = snappyBlock.getCompressedSlice();
        sliceOutput
                .appendInt(slice.length())
                .appendInt(snappyBlock.getPositionCount())
                .writeBytes(slice);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        int blockSize = sliceInput.readInt();
        int tupleCount = sliceInput.readInt();

        Slice block = sliceInput.readSlice(blockSize);
        return new SnappyBlock(tupleCount, tupleInfo, block);
    }

    public static void serialize(SliceOutput output, SnappyBlockEncoding encoding)
    {
        TupleInfoSerde.writeTupleInfo(output, encoding.getTupleInfo());
    }
}
