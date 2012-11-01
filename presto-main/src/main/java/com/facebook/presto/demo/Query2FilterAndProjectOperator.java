/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.demo;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockBuilder;
import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.nblock.uncompressed.UncompressedBlock;
import com.facebook.presto.noperator.Operator;
import com.facebook.presto.noperator.Page;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Charsets;
import com.google.common.collect.AbstractIterator;

import java.util.Iterator;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;

public class Query2FilterAndProjectOperator implements Operator
{
    private final Operator operator;

    public Query2FilterAndProjectOperator(Operator operator)
    {
        this.operator = operator;
    }

    @Override
    public int getChannelCount()
    {
        return 2;
    }

    @Override
    public Iterator<Page> iterator()
    {
        return new FilterAndProjectIterator(operator.iterator());
    }

    public static class FilterAndProjectIterator extends AbstractIterator<Page>
    {
        private static final Slice constant2 = Slices.copiedBuffer("ds=2012-10-11/cluster_name=silver", Charsets.UTF_8);

        private final Iterator<Page> pageIterator;

        private long outputPosition;

        public FilterAndProjectIterator(Iterator<Page> pageIterator)
        {
            this.pageIterator = pageIterator;
        }

        protected Page computeNext()
        {
            BlockBuilder poolNameOutputBlock = new BlockBuilder(outputPosition, TupleInfo.SINGLE_VARBINARY);
            BlockBuilder cpuMsecOutputBlock = new BlockBuilder(outputPosition, TupleInfo.SINGLE_LONG);

            while (!poolNameOutputBlock.isFull() && !cpuMsecOutputBlock.isFull() && pageIterator.hasNext()) {
                Page page = pageIterator.next();
                filterAndProjectRowOriented(page.getBlock(0),
                        page.getBlock(1),
                        page.getBlock(2),
                        poolNameOutputBlock,
                        cpuMsecOutputBlock);

            }

            if (poolNameOutputBlock.isEmpty()) {
                return endOfData();
            }

            UncompressedBlock poolOutput = poolNameOutputBlock.build();
            Page page = new Page(poolOutput, cpuMsecOutputBlock.build());
            outputPosition += poolOutput.getPositionCount();
            return page;
        }

        private void filterAndProjectRowOriented(Block partitionChunk, Block poolNameChunk, Block cpuMsecChunk, BlockBuilder poolNameOutputBlock, BlockBuilder cpuMsecOutputBlock)
        {
            int rows = (int) partitionChunk.getRange().length();

            BlockCursor partitionCursor = partitionChunk.cursor();
            BlockCursor poolName = poolNameChunk.cursor();
            BlockCursor cpuMsecCursor = cpuMsecChunk.cursor();

            for (int position = 0; position < rows; position++) {
                checkState(partitionCursor.advanceNextPosition());
                checkState(poolName.advanceNextPosition());
                checkState(cpuMsecCursor.advanceNextPosition());

                if (filter(partitionCursor.getSlice(0))) {
                    poolNameOutputBlock.append(poolName.getSlice(0));
                    cpuMsecOutputBlock.append(cpuMsecCursor.getLong(0));
                }
            }
            checkState(!partitionCursor.advanceNextPosition());
            checkState(!poolName.advanceNextPosition());
            checkState(!cpuMsecCursor.advanceNextPosition());
        }

        private static boolean filter(Slice partition)
        {
            return partition.equals(constant2);
        }
    }
}
