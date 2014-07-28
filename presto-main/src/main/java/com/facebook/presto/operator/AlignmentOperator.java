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
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class AlignmentOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> types;

    private final List<Iterator<Block>> iterators;
    private final List<BlockPosition> blockPositions;

    private boolean finished;

    public AlignmentOperator(OperatorContext operatorContext, List<Type> types, Iterable<Iterable<Block>> channels)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));

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
            for (Iterator<Block> iterator : this.iterators) {
                checkState(!iterator.hasNext());
            }
            finished = true;
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " cannot take input");
    }

    @Override
    public Page getOutput()
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
        operatorContext.recordGeneratedInput(page.getDataSize(), page.getPositionCount());
        return page;
    }

    private final class BlockPosition
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
