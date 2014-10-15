/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptor.block;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class BlockPageSource
        implements ConnectorPageSource
{
    private final List<Iterator<Block>> iterators;
    private final List<BlockPosition> blockPositions;

    private boolean finished;
    private long sizeInBytes;

    public BlockPageSource(Iterable<Iterable<Block>> channels)
    {
        ImmutableList.Builder<Iterator<Block>> iterators = ImmutableList.builder();
        for (Iterable<Block> channel : channels) {
            iterators.add(channel.iterator());
        }
        this.iterators = iterators.build();

        blockPositions = new ArrayList<>(this.iterators.size());
        if (this.iterators.get(0).hasNext()) {
            for (Iterator<Block> iterator : this.iterators) {
                blockPositions.add(new BlockPosition(iterator.next()));
            }
        }
        else {
            // no data
            for (Iterator<Block> iterator : this.iterators) {
                checkState(!iterator.hasNext());
            }
            finished = true;
        }
    }

    @Override
    public long getTotalBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public void close()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }

        // all iterators should end together
        if (blockPositions.get(0).getRemainingPositions() <= 0 && !iterators.get(0).hasNext()) {
            for (Iterator<Block> iterator : iterators) {
                checkState(!iterator.hasNext());
            }
            finished = true;
            return null;
        }

        // determine maximum shared length
        int length = Integer.MAX_VALUE;
        for (int i = 0; i < iterators.size(); i++) {
            Iterator<? extends Block> iterator = iterators.get(i);

            BlockPosition blockPosition = blockPositions.get(i);
            if (blockPosition.getRemainingPositions() <= 0) {
                // load next block
                blockPosition = new BlockPosition(iterator.next());
                blockPositions.set(i, blockPosition);
            }
            length = Math.min(length, blockPosition.getRemainingPositions());
        }

        // build page
        Block[] blocks = new Block[iterators.size()];
        for (int i = 0; i < blockPositions.size(); i++) {
            blocks[i] = blockPositions.get(i).getRegionAndAdvance(length);
        }

        Page page = new Page(blocks);
        sizeInBytes += page.getSizeInBytes();
        return page;
    }

    private static final class BlockPosition
    {
        private final Block block;
        private int position;

        private BlockPosition(Block block)
        {
            this.block = block;
        }

        public Block getRegionAndAdvance(int length)
        {
            Block region = block.getRegion(position, length);
            position += length;
            return region;
        }

        public int getRemainingPositions()
        {
            return block.getPositionCount() - position;
        }
    }
}
