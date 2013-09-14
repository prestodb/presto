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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockIterables;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class AlignmentOperator
        implements Operator
{
    public static class AlignmentOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<BlockIterable> channels;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public AlignmentOperatorFactory(int operatorId, BlockIterable firstChannel, BlockIterable... otherChannels)
        {
            this(operatorId,
                    ImmutableList.<BlockIterable>builder()
                            .add(checkNotNull(firstChannel, "firstChannel is null"))
                            .add(checkNotNull(otherChannels, "otherChannels is null"))
                            .build());
        }

        public AlignmentOperatorFactory(int operatorId, Iterable<BlockIterable> channels)
        {
            this.operatorId = operatorId;
            this.channels = ImmutableList.copyOf(checkNotNull(channels, "channels is null"));
            this.tupleInfos = toTupleInfos(channels);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, AlignmentOperator.class.getSimpleName());
            return new AlignmentOperator(operatorContext, channels);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<TupleInfo> tupleInfos;
    private final Optional<DataSize> expectedDataSize;
    private final Optional<Integer> expectedPositionCount;

    private final List<Iterator<Block>> iterators;
    private final List<BlockCursor> cursors;

    private boolean finished;

    public AlignmentOperator(OperatorContext operatorContext, BlockIterable... channels)
    {
        this(operatorContext, ImmutableList.copyOf(channels));
    }

    public AlignmentOperator(OperatorContext operatorContext, Iterable<BlockIterable> channels)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.tupleInfos = toTupleInfos(checkNotNull(channels, "channels is null"));

        expectedDataSize = BlockIterables.getDataSize(channels);
        expectedPositionCount = BlockIterables.getPositionCount(channels);

        ImmutableList.Builder<Iterator<Block>> iterators = ImmutableList.builder();
        for (BlockIterable channel : channels) {
            iterators.add(channel.iterator());
        }
        this.iterators = iterators.build();

        // open the cursors
        cursors = new ArrayList<>(this.iterators.size());
        if (this.iterators.get(0).hasNext()) {
            for (Iterator<Block> iterator : this.iterators) {
                cursors.add(iterator.next().cursor());
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
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }

        // all iterators should end together
        if (cursors.get(0).getRemainingPositions() <= 0 && !iterators.get(0).hasNext()) {
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

            BlockCursor cursor = cursors.get(i);
            if (cursor.getRemainingPositions() <= 0) {
                // load next block
                cursor = iterator.next().cursor();
                cursors.set(i, cursor);
            }
            length = Math.min(length, cursor.getRemainingPositions());
        }

        // build page
        Block[] blocks = new Block[iterators.size()];
        for (int i = 0; i < cursors.size(); i++) {
            blocks[i] = cursors.get(i).getRegionAndAdvance(length);
        }

        Page page = new Page(blocks);
        operatorContext.recordGeneratedInput(page.getDataSize(), page.getPositionCount());
        return page;
    }

    private static List<TupleInfo> toTupleInfos(Iterable<BlockIterable> channels)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (BlockIterable channel : channels) {
            tupleInfos.add(channel.getTupleInfo());
        }
        return tupleInfos.build();
    }
}
