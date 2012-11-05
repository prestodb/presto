/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.demo;

import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.uncompressed.UncompressedBlock;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;

import java.util.BitSet;
import java.util.Iterator;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;

public class Query3FilterAndProjectOperator implements Operator
{
    private final Operator operator;

    public Query3FilterAndProjectOperator(Operator operator)
    {
        this.operator = operator;
    }

    @Override
    public int getChannelCount()
    {
        return 1;
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new FilterAndProjectIterator(operator.iterator());
    }

    public static class FilterAndProjectIterator extends AbstractIterator<Page>
    {
        private static final Slice constant1 = Slices.copiedBuffer("ds=2012-07-01/cluster_name=silver", Charsets.UTF_8);
        private static final Slice constant2 = Slices.copiedBuffer("ds=2012-10-11/cluster_name=silver", Charsets.UTF_8);

        private final Iterator<Page> iterator;

        private long outputPosition;

        public FilterAndProjectIterator(Iterator<Page> iterator)
        {
            this.iterator = iterator;
        }

        protected Page computeNextX()
        {
            BlockBuilder blockBuilder = new BlockBuilder(outputPosition, TupleInfo.SINGLE_LONG);

            while (!blockBuilder.isFull() && iterator.hasNext()) {
                Page page = iterator.next();

                BlockCursor cpuMsecCursor = page.getBlock(0).cursor();
                while (cpuMsecCursor.advanceNextPosition()) {
                    blockBuilder.append(eval(cpuMsecCursor.getLong(0), cpuMsecCursor.getLong(0), cpuMsecCursor.getLong(0)));
                }
           }

            if (blockBuilder.isEmpty()) {
                return endOfData();
            }
            UncompressedBlock block = blockBuilder.build();
            outputPosition += block.getPositionCount();
            return new Page(block);
        }

        protected Page computeNext()
        {
            BlockBuilder blockBuilder = new BlockBuilder(outputPosition, TupleInfo.SINGLE_LONG);

            while (!blockBuilder.isFull() && iterator.hasNext()) {
                Page page = iterator.next();

                filterAndProjectRowOriented(
                        page.getBlock(0),
                        page.getBlock(1),
                        page.getBlock(2),
                        page.getBlock(3),
                        blockBuilder);

//                BitSet bitSet = filterRowOriented(
//                        page.getBlock(0),
//                        page.getBlock(1),
//                        page.getBlock(2));
//
//                projectRowOriented(
//                        page.getBlock(1),
//                        page.getBlock(2),
//                        page.getBlock(3),
//                        bitSet,
//                        blockBuilder);

//                BitSet bitSet = filterVectorized(
//                        page.getBlock(0),
//                        page.getBlock(1),
//                        page.getBlock(2));
//
//                projectVectorized(
//                        page.getBlock(1),
//                        page.getBlock(1),
//                        page.getBlock(2),
//                        page.getBlock(3),
//                        bitSet,
//                        blockBuilder);
            }

            if (blockBuilder.isEmpty()) {
                return endOfData();
            }
            UncompressedBlock block = blockBuilder.build();
            outputPosition += block.getPositionCount();
            return new Page(block);
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
