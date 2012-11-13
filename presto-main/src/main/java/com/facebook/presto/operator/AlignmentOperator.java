/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
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
        private final BlockCursor[] cursors;

        public AlignmentIterator(Iterator<? extends Block>[] iterators)
        {
            this.iterators = iterators;
            cursors = new BlockCursor[iterators.length];
            for (int i = 0; i < iterators.length; i++) {
                cursors[i] = iterators[i].next().cursor();
            }
        }

        protected Page computeNext()
        {
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
                 blocks[i] = cursors[i].createBlockViewPort(length);
            }

            return new Page(blocks);
        }
    }
}
