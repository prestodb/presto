/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.rle.RunLengthEncodedBlock;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.Range;
import com.google.common.base.Preconditions;

public class RunLengthBlockEncoding
        implements BlockEncoding
{
    private final TupleInfo tupleInfo;

    public RunLengthBlockEncoding(TupleInfo tupleInfo)
    {
        this.tupleInfo = tupleInfo;
    }

    public RunLengthBlockEncoding(SliceInput input)
    {
        Preconditions.checkNotNull(input, "input is null");
        tupleInfo = TupleInfoSerde.readTupleInfo(input);
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;
        Slice tupleSlice = rleBlock.getSingleValue().getTupleSlice();
        sliceOutput.appendInt(tupleSlice.length())
                .appendInt((int) rleBlock.getRange().length())
                .appendLong(rleBlock.getRange().getStart())
                .writeBytes(tupleSlice);
    }

    @Override
    public RunLengthEncodedBlock readBlock(SliceInput sliceInput, long positionOffset)
    {
        int tupleLength = sliceInput.readInt();
        int tupleCount = sliceInput.readInt();
        long startPosition = sliceInput.readLong() + positionOffset;

        Range range = Range.create(startPosition, startPosition + tupleCount - 1);

        Slice tupleSlice = sliceInput.readSlice(tupleLength);
        Tuple tuple = new Tuple(tupleSlice, tupleInfo);
        return new RunLengthEncodedBlock(tuple, range);
    }

    public static void serialize(SliceOutput output, RunLengthBlockEncoding encoding)
    {
        TupleInfoSerde.writeTupleInfo(output, encoding.tupleInfo);
    }
}
