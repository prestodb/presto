/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.noperator;

import com.facebook.presto.Range;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.Blocks;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterables;

import java.util.Iterator;

import static com.facebook.presto.hive.shaded.com.google.common.base.Preconditions.checkState;

public class AlignmentOperator implements Operator
{
    private final Blocks[] channels;

    public AlignmentOperator(Blocks... channels)
    {
        this.channels = channels;
    }

    public AlignmentOperator(Iterable<Blocks> channels)
    {
        this.channels = Iterables.toArray(channels, Blocks.class);
    }

    @Override
    public int getChannelCount()
    {
        return channels.length;
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
        private Block[] blocks;

        private long startPosition;

        public AlignmentIterator(Iterator<? extends Block>[] iterators)
        {
            this.iterators = iterators;
            blocks = new Block[iterators.length];
            for (int i = 0; i < iterators.length; i++) {
                blocks[i] = iterators[i].next();
            }
        }

        protected Page computeNext()
        {
            // all iterators should end together
            if (blocks[0].getRawRange().getEnd() < startPosition && !iterators[0].hasNext()) {
                for (Iterator<? extends Block> iterator : iterators) {
                    checkState(!iterator.hasNext());
                }
                return endOfData();
            }

            // determine shared range
            long endPosition = Long.MAX_VALUE;
            for (int i = 0; i < iterators.length; i++) {
                Iterator<? extends Block> iterator = iterators[i];

                Block block = blocks[i];
                long blockEndPosition = block.getRawRange().getEnd();
                if (blockEndPosition < startPosition) {
                    // load next block
                    block = iterator.next();
                    blocks[i] = block;
                    blockEndPosition = block.getRawRange().getEnd();
                }
                endPosition = Math.min(endPosition, blockEndPosition);
            }
            Range range = new Range(startPosition, endPosition);

            // update start position for next loop
            startPosition = endPosition + 1;

            // build page
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = blocks[i].createViewPort(range);
            }
            return new Page(blocks);
        }
    }
}
