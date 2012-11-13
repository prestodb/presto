/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;

public class AlignmentOperator implements Operator
{
    private final BlockIterable[] channels;
    private final List<TupleInfo> tupleInfos;

    public AlignmentOperator(BlockIterable... channels)
    {
        this.channels = channels;
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (BlockIterable channel : channels) {
            tupleInfos.add(channel.getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();
    }

    public AlignmentOperator(Iterable<BlockIterable> channels)
    {
        this.channels = Iterables.toArray(channels, BlockIterable.class);
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (BlockIterable channel : channels) {
            tupleInfos.add(channel.getTupleInfo());
        }
        this.tupleInfos = tupleInfos.build();
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

    @Override
    public Iterator<Page> iterator()
    {
        Iterator<? extends Block>[] iterators = new Iterator[channels.length];
        for (int i = 0; i < iterators.length; i++) {
            iterators[i] = channels[i].iterator();
        }
        return new AlignmentIterator(iterators);
    }

    public static class AlignmentIterator extends AbstractIterator<Page>
    {
        private final Iterator<? extends Block>[] iterators;
        private final Block[] blocks;
        private final int[] rawPosition;

        public AlignmentIterator(Iterator<? extends Block>[] iterators)
        {
            this.iterators = iterators;
            blocks = new Block[iterators.length];
            for (int i = 0; i < iterators.length; i++) {
                blocks[i] = iterators[i].next();
            }
            rawPosition = new int[iterators.length];
        }

        protected Page computeNext()
        {
            // all iterators should end together
            if (rawPosition[0] >= blocks[0].getRawPositionCount() && !iterators[0].hasNext()) {
                for (Iterator<? extends Block> iterator : iterators) {
                    checkState(!iterator.hasNext());
                }
                return endOfData();
            }

            // determine maximum shared length
            int length = Integer.MAX_VALUE;
            for (int i = 0; i < iterators.length; i++) {
                Iterator<? extends Block> iterator = iterators[i];

                Block block = blocks[i];
                int rawPositionCount = block.getRawPositionCount();
                if (rawPosition[i] >= rawPositionCount) {
                    // load next block
                    block = iterator.next();
                    blocks[i] = block;
                    rawPosition[i] = 0;
                    rawPositionCount = block.getRawPositionCount();
                }
                length = Math.min(length, rawPositionCount - rawPosition[i]);
            }

            // build page
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = blocks[i].createViewPort(rawPosition[i], length);
            }

            // update raw position for next loop
            for (int i = 0; i < rawPosition.length; i++) {
                rawPosition[i] += length;
            }

            return new Page(blocks);
        }
    }
}
