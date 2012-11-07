/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.Range;
import com.google.common.base.Preconditions;

public class UncompressedBlockEncoding
        implements BlockEncoding
{
    private final TupleInfo tupleInfo;

    public UncompressedBlockEncoding(TupleInfo tupleInfo)
    {
        Preconditions.checkNotNull(tupleInfo, "tupleInfo is null");
        this.tupleInfo = tupleInfo;
    }

    public UncompressedBlockEncoding(SliceInput input)
    {
        Preconditions.checkNotNull(input, "input is null");
        tupleInfo = TupleInfoSerde.readTupleInfo(input);
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        UncompressedBlock uncompressedBlock = (UncompressedBlock) block;
        Preconditions.checkArgument(block.getTupleInfo().equals(tupleInfo), "Invalid tuple info");
        writeUncompressedBlock(sliceOutput,
                uncompressedBlock.getRange().getStart(),
                (int) uncompressedBlock.getRange().length(),
                uncompressedBlock.getSlice());
    }

    @Override
    public Block readBlock(SliceInput sliceInput, long positionOffset)
    {
        int blockSize = sliceInput.readInt();
        int tupleCount = sliceInput.readInt();
        long startPosition = sliceInput.readLong() + positionOffset;

        Range range = Range.create(startPosition, startPosition + tupleCount - 1);

        Slice block = sliceInput.readSlice(blockSize);
        return new UncompressedBlock(range, tupleInfo, block);
    }

    private static void writeUncompressedBlock(SliceOutput destination, long startPosition, int tupleCount, Slice slice)
    {
        destination
                .appendInt(slice.length())
                .appendInt(tupleCount)
                .appendLong(startPosition)
                .writeBytes(slice);
    }

    public static void serialize(SliceOutput output, UncompressedBlockEncoding encoding)
    {
        TupleInfoSerde.writeTupleInfo(output, encoding.tupleInfo);
    }
}
