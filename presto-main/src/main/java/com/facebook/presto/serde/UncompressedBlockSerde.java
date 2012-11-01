/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.serde;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.nblock.uncompressed.UncompressedBlock;
import com.facebook.presto.slice.DynamicSliceOutput;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.SliceOutput;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.airlift.units.DataSize;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;

public class UncompressedBlockSerde
        implements BlockSerde
{
    private static final int MAX_BLOCK_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();
    public static UncompressedBlockSerde INSTANCE = new UncompressedBlockSerde();

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

        private boolean initialized;
        private boolean finished;
        private long currentStartPosition = -1;
        private DynamicSliceOutput buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);
        private int tupleCount;

        private UncompressedBlocksWriter(SliceOutput sliceOutput)
        {
            this.sliceOutput = checkNotNull(sliceOutput, "sliceOutput is null");
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
                if (currentStartPosition == -1) {
                    currentStartPosition = cursor.getPosition();
                }
                cursor.getTuple().writeTo(buffer);
                tupleCount++;

                if (buffer.size() >= MAX_BLOCK_SIZE) {
                    writeUncompressedBlock(sliceOutput, currentStartPosition, tupleCount, buffer.slice());
                    tupleCount = 0;
                    buffer = new DynamicSliceOutput(MAX_BLOCK_SIZE);
                    currentStartPosition = -1;
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
                checkState(currentStartPosition >= 0, "invariant");
                writeUncompressedBlock(sliceOutput, currentStartPosition, tupleCount, buffer.slice());
            }
            // todo this code did not open the stream so it shouldn't be closing it
            try {
                sliceOutput.close();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

}
