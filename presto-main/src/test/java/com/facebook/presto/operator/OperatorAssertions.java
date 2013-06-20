/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockIterables;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.block.BlockAssertions.assertBlocksEquals;
import static org.testng.Assert.assertEquals;

public final class OperatorAssertions
{
    private OperatorAssertions()
    {
    }

    public static Operator createOperator(Page... pages)
    {
        return createOperator(ImmutableList.copyOf(pages));
    }

    public static Operator createOperator(Iterable<? extends Page> pages)
    {
        return new OperatorAdapter(pages);
    }

    private static class OperatorAdapter
            implements Operator
    {
        private final List<Page> pages;
        private final int channelCount;
        private final List<TupleInfo> tupleInfos;

        public OperatorAdapter(Iterable<? extends Page> pages)
        {
            this.pages = ImmutableList.copyOf(pages);
            this.channelCount = this.pages.get(0).getChannelCount();

            ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
            for (Block block : this.pages.get(0).getBlocks()) {
                tupleInfos.add(block.getTupleInfo());
            }
            this.tupleInfos = tupleInfos.build();
        }

        @Override
        public int getChannelCount()
        {
            return channelCount;
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public PageIterator iterator(OperatorStats operatorStats)
        {
            return PageIterators.createPageIterator(pages, operatorStats);
        }
    }

    public static void assertOperatorEquals(Operator actual, Operator expected)
    {
        assertEquals(actual.getChannelCount(), expected.getChannelCount(), "Channel count");

        List<BlockIterable> actualColumns = loadColumns(actual);
        List<BlockIterable> expectedColumns = loadColumns(expected);
        for (int i = 0; i < actualColumns.size(); i++) {
            BlockIterable actualColumn = actualColumns.get(i);
            BlockIterable expectedColumn = expectedColumns.get(i);
            assertBlocksEquals(actualColumn, expectedColumn);
        }
    }

    public static List<BlockIterable> loadColumns(Operator operator)
    {
        List<ImmutableList.Builder<Block>> blockBuilders = new ArrayList<>();
        for (int i = 0; i < operator.getChannelCount(); i++) {
            blockBuilders.add(ImmutableList.<Block>builder());
        }
        PageIterator iterator = operator.iterator(new OperatorStats());
        while (iterator.hasNext()) {
            Page page = iterator.next();
            Block[] blocks = page.getBlocks();
            for (int i = 0; i < blocks.length; i++) {
                blockBuilders.get(i).add(blocks[i]);
            }
        }

        ImmutableList.Builder<BlockIterable> blockIterables = ImmutableList.builder();
        for (ImmutableList.Builder<Block> blockBuilder : blockBuilders) {
            blockIterables.add(BlockIterables.createBlockIterable(blockBuilder.build()));
        }
        return blockIterables.build();
    }
}
