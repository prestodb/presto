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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RunLengthEncodedBlockSerde
        implements BlockSerde
{
    public static final RunLengthEncodedBlockSerde RLE_BLOCK_SERDE = new RunLengthEncodedBlockSerde();

    @Override
    public BlocksWriter createBlocksWriter(SliceOutput sliceOutput)
    {
        return new RunLengthEncodedBlocksWriter(sliceOutput);
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;
        writeRunLengthEncodedBlock(sliceOutput,
                rleBlock.getRange().getStart(),
                (int) rleBlock.getRange().length(),
                rleBlock.getSingleValue());
    }

    @Override
    public RunLengthEncodedBlock readBlock(SliceInput sliceInput, TupleInfo tupleInfo, long positionOffset)
    {
        int tupleLength = sliceInput.readInt();
        int tupleCount = sliceInput.readInt();
        long startPosition = sliceInput.readLong() + positionOffset;

        Range range = Range.create(startPosition, startPosition + tupleCount - 1);

        Slice tupleSlice = sliceInput.readSlice(tupleLength);
        Tuple tuple = new Tuple(tupleSlice, tupleInfo);
        return new RunLengthEncodedBlock(tuple, range);
    }

    private static void writeRunLengthEncodedBlock(SliceOutput destination, long startPosition, int tupleCount, Tuple value)
    {
        Slice tupleSlice = value.getTupleSlice();
        destination
                .appendInt(tupleSlice.length())
                .appendInt(tupleCount)
                .appendLong(startPosition)
                .writeBytes(tupleSlice);
    }

    private static class RunLengthEncodedBlocksWriter
            implements BlocksWriter
    {
        private final SliceOutput sliceOutput;
        private boolean initialized;
        private boolean finished;

        private long startPosition = -1;
        private int tupleCount = -1;
        private Tuple lastTuple;

        private RunLengthEncodedBlocksWriter(SliceOutput sliceOutput)
        {
            this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
        }

        @Override
        public BlocksWriter append(Iterable<Tuple> tuples)
        {
            checkNotNull(tuples, "tuples is null");
            checkState(!finished, "already finished");

            if (!initialized) {
                initialized = true;
            }

            for (Tuple tuple : tuples) {
                if (lastTuple == null) {
                    startPosition = 0;
                    tupleCount = 1;
                    lastTuple = tuple;
                }
                else {
                    if (!tuple.equals(lastTuple)) {
                        writeRunLengthEncodedBlock(sliceOutput,
                                startPosition,
                                tupleCount,
                                lastTuple);

                        lastTuple = tuple;
                        startPosition += tupleCount;
                        tupleCount = 0;
                    }
                    tupleCount++;
                }
            }

            return this;
        }

        @Override
        public void finish()
        {
            checkState(initialized, "nothing appended");
            checkState(!finished, "already finished");
            finished = true;

            if (lastTuple != null) {
                // Flush out final block if there exists one (null if they were all empty blocks)
                writeRunLengthEncodedBlock(sliceOutput,
                        startPosition,
                        tupleCount,
                        lastTuple);
            }
        }
    }
}
