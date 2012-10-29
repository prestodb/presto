/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.demo;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.nblock.RangeBoundedBlock;
import com.facebook.presto.nblock.uncompressed.UncompressedBlock;
import com.facebook.presto.nblock.Blocks;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;

import java.util.BitSet;
import java.util.Iterator;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;

public class FilterAndProject implements Blocks
{
    private final Blocks partition;
    private final Blocks startTime;
    private final Blocks endTime;
    private final Blocks cpuMsecBlock;

    public FilterAndProject(Blocks partition,
            Blocks startTime,
            Blocks endTime,
            Blocks cpuMsecBlock)
    {
        this.partition = partition;
        this.startTime = startTime;
        this.endTime = endTime;
        this.cpuMsecBlock = cpuMsecBlock;
    }

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public Iterator<Block> iterator()
    {
        return new FilterAndProjectIterator(
                partition.iterator(),
                startTime.iterator(),
                endTime.iterator(),
                cpuMsecBlock.iterator());
    }

    public static class FilterAndProjectIterator extends AbstractIterator<Block>
    {
        private static final Slice constant1 = Slices.copiedBuffer("ds=2012-07-01/cluster_name=silver", Charsets.UTF_8);
        private static final Slice constant2 = Slices.copiedBuffer("ds=2012-10-11/cluster_name=silver", Charsets.UTF_8);

        private final Iterator<? extends Block> partitionBlocks;
        private final Iterator<? extends Block> startTimeBlocks;
        private final Iterator<? extends Block> endTimeBlocks;
        private final Iterator<? extends Block> cpuMsecBlocks;

        private final Iterator<? extends Block> blocks;

        private Block partitionBlock;
        private Block startTimeBlock;
        private Block endTimeBlock;
        private Block cpuMsecBlock;

        private Block block;

        private long startPosition;
        private long outputPosition;

        public FilterAndProjectIterator(
                Iterator<? extends Block> partitionBlocks,
                Iterator<? extends Block> startTimeBlocks,
                Iterator<? extends Block> endTimeBlocks,
                Iterator<? extends Block> cpuMsecBlocks)
        {
            this.partitionBlocks = partitionBlocks;
            this.startTimeBlocks = startTimeBlocks;
            this.endTimeBlocks = endTimeBlocks;
            this.cpuMsecBlocks = cpuMsecBlocks;


            partitionBlock = partitionBlocks.next();
            startTimeBlock = startTimeBlocks.next();
            endTimeBlock = endTimeBlocks.next();
            cpuMsecBlock = cpuMsecBlocks.next();

            this.blocks = this.cpuMsecBlocks;
            this.block = this.cpuMsecBlock;
        }

        protected Block computeNextX()
        {
            BlockBuilder blockBuilder = new BlockBuilder(outputPosition, TupleInfo.SINGLE_LONG);

            while (!blockBuilder.isFull()) {

                if (block.getRange().getEnd() < startPosition) {
                    if (!blocks.hasNext()) {
                        break;
                    }
                    block = blocks.next();
                }

                long rows = block.getRange().length();
                BlockCursor cpuMsecCursor = block.cursor();
                for (int position = 0; position < rows; position++) {
                    checkState(cpuMsecCursor.advanceNextPosition());
                    blockBuilder.append(eval(cpuMsecCursor.getLong(0), cpuMsecCursor.getLong(0), cpuMsecCursor.getLong(0)));
                }

                startPosition = block.getRange().getEnd() + 1;
           }

            if (blockBuilder.isEmpty()) {
                return endOfData();
            }
            UncompressedBlock block = blockBuilder.build();
            outputPosition += block.getCount();
            return block;
        }

        protected Block computeNext()
        {
            BlockBuilder blockBuilder = new BlockBuilder(outputPosition, TupleInfo.SINGLE_LONG);

            while (!blockBuilder.isFull()) {
                long endPosition;
                Range partitionBlockRange = partitionBlock.getRange();
                if (partitionBlockRange.getEnd() < startPosition) {
                    if (!partitionBlocks.hasNext()) {
                        checkState(!startTimeBlocks.hasNext());
                        checkState(!endTimeBlocks.hasNext());
                        checkState(!cpuMsecBlocks.hasNext());
                        break;
                    }
                    partitionBlock = partitionBlocks.next();
                    partitionBlockRange = partitionBlock.getRange();
                }
                endPosition = partitionBlockRange.getEnd();

                Range startTimeBlockRange = startTimeBlock.getRange();
                if (startTimeBlockRange.getEnd() < startPosition) {
                    startTimeBlock = startTimeBlocks.next();
                    startTimeBlockRange = startTimeBlock.getRange();
                }
                endPosition = Math.min(endPosition, startTimeBlockRange.getEnd());


                Range endTimeBlockRange = endTimeBlock.getRange();
                if (endTimeBlockRange.getEnd() < startPosition) {
                    endTimeBlock = endTimeBlocks.next();
                    endTimeBlockRange = endTimeBlock.getRange();
                }
                endPosition = Math.min(endPosition, endTimeBlockRange.getEnd());

                Range cpuMsecBlockRange = cpuMsecBlock.getRange();
                if (cpuMsecBlockRange.getEnd() < startPosition) {
                    cpuMsecBlock = cpuMsecBlocks.next();
                    cpuMsecBlockRange = cpuMsecBlock.getRange();
                }
                endPosition = Math.min(endPosition, cpuMsecBlockRange.getEnd());

                Range range = new Range(startPosition, endPosition);

                filterAndProjectRowOriented(
                        new RangeBoundedBlock(range, partitionBlock.cursor()),
                        new RangeBoundedBlock(range, startTimeBlock.cursor()),
                        new RangeBoundedBlock(range, endTimeBlock.cursor()),
                        new RangeBoundedBlock(range, cpuMsecBlock.cursor()),
                        blockBuilder);

//                BitSet bitSet = filterRowOriented(session,
//                        new RangeBoundedBlock(range, partitionBlock.cursor(session)),
//                        new RangeBoundedBlock(range, startTimeBlock.cursor(session)),
//                        new RangeBoundedBlock(range, endTimeBlock.cursor(session)));
//
//                projectRowOriented(session,
//                        new RangeBoundedBlock(range, startTimeBlock.cursor(session)),
//                        new RangeBoundedBlock(range, endTimeBlock.cursor(session)),
//                        new RangeBoundedBlock(range, cpuMsecBlock.cursor(session)),
//                        bitSet,
//                        blockBuilder);

//                BitSet bitSet = filterVectorized(session,
//                        new RangeBoundedBlock(range, partitionBlock.cursor(session)),
//                        new RangeBoundedBlock(range, startTimeBlock.cursor(session)),
//                        new RangeBoundedBlock(range, endTimeBlock.cursor(session)));
//
//                projectVectorized(session,
//                        new RangeBoundedBlock(range, startTimeBlock.cursor(session)),
//                        new RangeBoundedBlock(range, startTimeBlock.cursor(session)),
//                        new RangeBoundedBlock(range, endTimeBlock.cursor(session)),
//                        new RangeBoundedBlock(range, cpuMsecBlock.cursor(session)),
//                        bitSet,
//                        blockBuilder);

                startPosition = endPosition + 1;
            }

            if (blockBuilder.isEmpty()) {
                return endOfData();
            }
            UncompressedBlock block = blockBuilder.build();
            outputPosition += block.getCount();
            return block;
        }

        private void filterAndProjectRowOriented(Block partitionChunk, Block startTimeChunk, Block endTimeChunk, Block cpuMsecChunk, BlockBuilder blockBuilder)
        {
            int rows = (int) partitionChunk.getRange().length();

            BlockCursor partitionCursor = partitionChunk.cursor();
            BlockCursor startTimeCursor = startTimeChunk.cursor();
            BlockCursor endTimeCursor = endTimeChunk.cursor();
            BlockCursor cpuMsecCursor = cpuMsecChunk.cursor();

            for (int position = 0; position < rows; position++) {
                checkState(partitionCursor.advanceNextPosition());
                checkState(startTimeCursor.advanceNextPosition());
                checkState(endTimeCursor.advanceNextPosition());
                checkState(cpuMsecCursor.advanceNextPosition());

                if (filter(partitionCursor.getSlice(0), startTimeCursor.getLong(0), endTimeCursor.getLong(0))) {
                    blockBuilder.append(eval(cpuMsecCursor.getLong(0), startTimeCursor.getLong(0), endTimeCursor.getLong(0)));
                }
            }
            checkState(!partitionCursor.advanceNextPosition());
            checkState(!startTimeCursor.advanceNextPosition());
            checkState(!endTimeCursor.advanceNextPosition());
            checkState(!cpuMsecCursor.advanceNextPosition());
        }

        private BitSet filterRowOriented(Block partitionChunk, Block startTimeChunk, Block endTimeChunk)
        {
            int rows = (int) partitionChunk.getRange().length();
            BitSet bitSet = new BitSet(rows);

            BlockCursor partitionCursor = partitionChunk.cursor();
            BlockCursor startTimeCursor = startTimeChunk.cursor();
            BlockCursor endTimeCursor = endTimeChunk.cursor();

            // where partition >= 'ds=2012-07-01/cluster_name=silver'
            // and partition <= 'ds=2012-10-11/cluster_name=silver'
            // and start_time <= 1343350800
            // and end_time > 1343350800
            for (int position = 0; position < rows; position++) {
                checkState(partitionCursor.advanceNextPosition());
                checkState(startTimeCursor.advanceNextPosition());
                checkState(endTimeCursor.advanceNextPosition());

                bitSet.set(position, filter(partitionCursor.getSlice(0), startTimeCursor.getLong(0), endTimeCursor.getLong(0)));
            }
            checkState(!partitionCursor.advanceNextPosition());
            checkState(!startTimeCursor.advanceNextPosition());
            checkState(!endTimeCursor.advanceNextPosition());

            return bitSet;
        }

        private static boolean filter(Slice partition, long startTime, long endTime)
        {
            return startTime <= 1343350800 && endTime > 1343350800 && partition.compareTo(constant1) >= 0 && partition.compareTo(constant2) <= 0;
        }

        private BitSet filterVectorized(Block partitionChunk, Block startTimeChunk, Block endTimeChunk)
        {
            int rows = (int) partitionChunk.getRange().length();
            BitSet bitSet = new BitSet(rows);

            // where partition >= 'ds=2012-07-01/cluster_name=silver'
            // and partition <= 'ds=2012-10-11/cluster_name=silver'
            // and start_time <= 1343350800
            // and end_time > 1343350800

            BlockCursor partitionCursor = partitionChunk.cursor();
            for (int position = 0; position < rows; position++) {
                checkState(partitionCursor.advanceNextPosition());
                Slice partition = partitionCursor.getSlice(0);
                bitSet.set(position, partition.compareTo(constant1) >= 0 && partition.compareTo(constant2) <= 0);
            }
            checkState(!partitionCursor.advanceNextPosition());

            BlockCursor startTimeCursor = startTimeChunk.cursor();
            for (int position = 0; position < rows; position++) {
                checkState(startTimeCursor.advanceNextPosition());
                bitSet.set(position, bitSet.get(position) && startTimeCursor.getLong(0) <= 1343350800L);
            }
            checkState(!startTimeCursor.advanceNextPosition());

            BlockCursor endTimeCursor = endTimeChunk.cursor();
            for (int position = 0; position < rows; position++) {
                checkState(endTimeCursor.advanceNextPosition());
                bitSet.set(position, bitSet.get(position) && endTimeCursor.getLong(0) > 1343350800L);
            }
            checkState(!endTimeCursor.advanceNextPosition());

            return bitSet;
        }

        private void projectRowOriented(Block startTimeChunk,
                Block endTimeChunk,
                Block cpuMsecChunk,
                BitSet bitSet,
                BlockBuilder blockBuilder)
        {
            int rows = (int) cpuMsecChunk.getRange().length();

            BlockCursor cpuMsecCursor = cpuMsecChunk.cursor();
            BlockCursor startTimeCursor = startTimeChunk.cursor();
            BlockCursor endTimeCursor = endTimeChunk.cursor();

            // select sum(cpu_msec * (1343350800 - start_time) / (end_time - start_time)) / (1000*86400)
            for (int position = 0; position < rows; position++) {
                checkState(cpuMsecCursor.advanceNextPosition());
                checkState(startTimeCursor.advanceNextPosition());
                checkState(endTimeCursor.advanceNextPosition());

                if (bitSet.get(position)) {
                    blockBuilder.append(eval(cpuMsecCursor.getLong(0), startTimeCursor.getLong(0), endTimeCursor.getLong(0)));
                }
            }
            checkState(!cpuMsecCursor.advanceNextPosition());
            checkState(!startTimeCursor.advanceNextPosition());
            checkState(!endTimeCursor.advanceNextPosition());
        }

        private static long eval(long cpuMsec, long startTime, long endTime)
        {
            long elapsedTime = endTime - startTime;
            return (cpuMsec * (1343350800 - startTime) + elapsedTime) + (1000 * 86400);
        }

        private void projectVectorized(Block startTimeChunk,
                Block startTimeChunk2,
                Block endTimeChunk,
                Block cpuMsecChunk,
                BitSet bitSet,
                BlockBuilder blockBuilder)
        {
            int rows = (int) cpuMsecChunk.getRange().length();

            // select sum(cpu_msec * (1343350800 - start_time) / (end_time - start_time)) / (1000*86400)
            long[] results = new long[rows];
            BlockCursor cpuMsecCursor = cpuMsecChunk.cursor();
            for (int position = 0; position < rows; position++) {
                checkState(cpuMsecCursor.advanceNextPosition());
                results[position] = cpuMsecCursor.getLong(0);
            }
            checkState(!cpuMsecCursor.advanceNextPosition());

            BlockCursor startTimeCursor = startTimeChunk.cursor();
            for (int position = 0; position < rows; position++) {
                checkState(startTimeCursor.advanceNextPosition());
                results[position] *= 1343350800 - startTimeCursor.getLong(0);
            }
            checkState(!startTimeCursor.advanceNextPosition());

            long[] endTimeMinusStartTime = new long[rows];
            {
                BlockCursor endTimeCursor = endTimeChunk.cursor();
                for (int position = 0; position < rows; position++) {
                    checkState(endTimeCursor.advanceNextPosition());
                    endTimeMinusStartTime[position] = endTimeCursor.getLong(0);
                }
                checkState(!endTimeCursor.advanceNextPosition());

                startTimeCursor = startTimeChunk2.cursor();
                for (int position = 0; position < rows; position++) {
                    checkState(startTimeCursor.advanceNextPosition());
                    endTimeMinusStartTime[position] -= startTimeCursor.getLong(0);
                }
                checkState(!startTimeCursor.advanceNextPosition());
            }

            for (int position = 0; position < rows; position++) {
                if (bitSet.get(position)) {
                    blockBuilder.append(results[position] + endTimeMinusStartTime[position] + (1000 * 86400));
                }
            }
        }
    }
}
