/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.Range;
import com.facebook.presto.Tuple;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class UncompressedBlockSerde
        implements BlockSerde
{
    private static final int MAX_BLOCK_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();
    public static final UncompressedBlockSerde UNCOMPRESSED_BLOCK_SERDE = new UncompressedBlockSerde();

    @Override
    public BlocksWriter createBlockWriter(SliceOutput sliceOutput)
    {
        return new UncompressedBlocksWriter(sliceOutput);
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        UncompressedBlock uncompressedBlock = (UncompressedBlock) block;
        writeUncompressedBlock(sliceOutput,
                uncompressedBlock.getRange().getStart(),
                (int) uncompressedBlock.getRange().length(),
                uncompressedBlock.getSlice());
    }

    @Override
    public Block readBlock(SliceInput sliceInput, TupleInfo tupleInfo, long positionOffset)
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

    private static class UncompressedBlocksWriter
            implements BlocksWriter
    {
        private final SliceOutput sliceOutput;
        private final DynamicSliceOutput buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);

        private boolean initialized;
        private boolean finished;
        private long blockStartPosition = 0;
        private int tupleCount;

        private UncompressedBlocksWriter(SliceOutput sliceOutput)
        {
            this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
        }

        @Override
        public BlocksWriter append(Tuple tuple)
        {
            Preconditions.checkNotNull(tuple, "tuple is null");
            checkState(!finished, "already finished");

            if (!initialized) {
                initialized = true;
            }

            tuple.writeTo(buffer);
            tupleCount++;

            if (buffer.size() >= MAX_BLOCK_SIZE) {
                writeBlock();
            }

            return this;
        }

        @Override
        public BlocksWriter append(Block block)
        {
            Preconditions.checkNotNull(block, "block is null");
            checkState(!finished, "already finished");

            if (!initialized) {
                initialized = true;
            }

            BlockCursor cursor = block.cursor();
            while (cursor.advanceNextPosition()) {
                cursor.getTuple().writeTo(buffer);
                tupleCount++;

                if (buffer.size() >= MAX_BLOCK_SIZE) {
                    writeBlock();
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

            if (buffer.size() > 0) {
                writeBlock();
            }
        }

        private void writeBlock()
        {
            writeUncompressedBlock(sliceOutput, blockStartPosition, tupleCount, buffer.slice());
            buffer.reset();
            blockStartPosition += tupleCount;
            tupleCount = 0;
        }
    }
}
