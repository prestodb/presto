/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockIterables;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.units.DataSize;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class AlignmentOperator implements Operator
{
    private final BlockIterable[] channels;
    private final List<TupleInfo> tupleInfos;
    private Optional<DataSize> expectedDataSize;
    private Optional<Integer> expectedPositionCount;

    public AlignmentOperator(BlockIterable... channels)
    {
        this.channels = channels;
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (BlockIterable channel : channels) {
            tupleInfos.add(channel.getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();
        expectedDataSize = BlockIterables.getDataSize(channels);
        expectedPositionCount = BlockIterables.getPositionCount(channels);
    }

    public AlignmentOperator(Iterable<BlockIterable> channels)
    {
        this.channels = Iterables.toArray(channels, BlockIterable.class);
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (BlockIterable channel : channels) {
            tupleInfos.add(channel.getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();
        expectedDataSize = BlockIterables.getDataSize(channels);
        expectedPositionCount = BlockIterables.getPositionCount(channels);
    }

    @Override
    public int getChannelCount()
    {
        return channels.length;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    public Optional<DataSize> getExpectedDataSize()
    {
        return expectedDataSize;
    }

    public Optional<Integer> getExpectedPositionCount()
    {
        return expectedPositionCount;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        Iterator<? extends Block>[] iterators = new Iterator[channels.length];
        for (int i = 0; i < iterators.length; i++) {
            iterators[i] = channels[i].iterator();
        }

        // TODO: remove this hack after empty blocks are supported
        if (!iterators[0].hasNext()) {
            for (Iterator<? extends Block> iterator : iterators) {
                checkState(!iterator.hasNext(), "iterators are not aligned");
            }
            return PageIterators.emptyIterator(tupleInfos);
        }

        return new AlignmentIterator(tupleInfos, iterators, operatorStats);
    }

    public static class AlignmentIterator
            extends AbstractPageIterator
    {
        private final Iterator<? extends Block>[] iterators;
        private final OperatorStats operatorStats;
        private final BlockCursor[] cursors;

        public AlignmentIterator(List<TupleInfo> tupleInfos, Iterator<? extends Block>[] iterators, OperatorStats operatorStats)
        {
            super(tupleInfos);

            this.iterators = iterators;
            this.operatorStats = operatorStats;

            cursors = new BlockCursor[iterators.length];
            for (int i = 0; i < iterators.length; i++) {
                cursors[i] = iterators[i].next().cursor();
            }
        }

        protected Page computeNext()
        {
            if (operatorStats.isDone()) {
                return endOfData();
            }

            // all iterators should end together
            if (cursors[0].getRemainingPositions() <= 0 && !iterators[0].hasNext()) {
                for (Iterator<? extends Block> iterator : iterators) {
                    checkState(!iterator.hasNext());
                }
                return endOfData();
            }

            // determine maximum shared length
            int length = Integer.MAX_VALUE;
            for (int i = 0; i < iterators.length; i++) {
                Iterator<? extends Block> iterator = iterators[i];

                BlockCursor cursor = cursors[i];
                if (cursor.getRemainingPositions() <= 0) {
                    // load next block
                    cursor = iterator.next().cursor();
                    cursors[i] = cursor;
                }
                length = Math.min(length, cursor.getRemainingPositions());
            }

            // build page
            Block[] blocks = new Block[iterators.length];
            for (int i = 0; i < cursors.length; i++) {
                 blocks[i] = cursors[i].getRegionAndAdvance(length);
            }

            Page page = new Page(blocks);
            operatorStats.addCompletedDataSize(page.getDataSize().toBytes());
            operatorStats.addCompletedPositions(page.getPositionCount());
            return page;
        }

        @Override
        protected void doClose()
        {
        }
    }
}
