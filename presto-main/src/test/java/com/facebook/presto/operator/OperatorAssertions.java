/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockIterables;
import com.facebook.presto.tuple.Tuple;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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

    private static class OperatorAdapter implements Operator
    {
        private final List<Page> pages;
        private final int channelCount;

        public OperatorAdapter(Iterable<? extends Page> pages)
        {
            this.pages = ImmutableList.copyOf(pages);
            this.channelCount = this.pages.get(0).getChannelCount();
        }

        @Override
        public int getChannelCount()
        {
            return channelCount;
        }

        @Override
        public Iterator<Page> iterator()
        {
            return pages.iterator();
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

    public static void assertOperatorEqualsUnordered(Operator actual, Operator expected)
    {
        assertEquals(actual.getChannelCount(), expected.getChannelCount(), "Channel count");

        Set<List<Tuple>> actualValues = new HashSet<>(unrollChannelTuples(actual));
        Set<List<Tuple>> expectedValues = new HashSet<>(unrollChannelTuples(expected));
        assertEquals(actualValues, expectedValues, "unmatched tuples");
    }

    /**
     * Converts each position in an operator to a List<Tuple> and returns the entire set of
     * List<Tuple> as an ordered list of values.
     */
    public static List<List<Tuple>> unrollChannelTuples(Operator operator)
    {
        ImmutableList.Builder<List<Tuple>> builder = ImmutableList.builder();
        for (Page page : operator) {
            Block[] blocks = page.getBlocks();
            BlockCursor[] cursors = new BlockCursor[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                cursors[i] = blocks[i].cursor();
            }
            for (int i = 0; i < page.getPositionCount(); i++) {
                ImmutableList.Builder<Tuple> tupleBuilder = ImmutableList.builder();
                for (BlockCursor cursor : cursors) {
                    Preconditions.checkState(cursor.advanceNextPosition());
                    tupleBuilder.add(cursor.getTuple());
                }
                builder.add(tupleBuilder.build());
            }
        }
        return builder.build();
    }

    public static List<BlockIterable> loadColumns(Operator operator)
    {
        List<ImmutableList.Builder<Block>> blockBuilders = new ArrayList<>();
        for (int i = 0; i < operator.getChannelCount(); i++) {
            blockBuilders.add(ImmutableList.<Block>builder());
        }
        for (Page page : operator) {
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
