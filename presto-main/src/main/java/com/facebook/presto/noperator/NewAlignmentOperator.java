package com.facebook.presto.noperator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockIterables;
import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
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

public class NewAlignmentOperator
        implements NewOperator
{
    public static class NewAlignmentOperatorFactory
            implements NewOperatorFactory
    {
        private final List<BlockIterable> channels;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public NewAlignmentOperatorFactory(BlockIterable firstChannel, BlockIterable... otherChannels)
        {
            this(ImmutableList.<BlockIterable>builder()
                    .add(checkNotNull(firstChannel, "firstChannel is null"))
                    .add(checkNotNull(otherChannels, "otherChannels is null"))
                    .build());
        }

        public NewAlignmentOperatorFactory(Iterable<BlockIterable> channels)
        {
            this.channels = ImmutableList.copyOf(checkNotNull(channels, "channels is null"));
            this.tupleInfos = toTupleInfos(channels);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public NewOperator createOperator(OperatorStats operatorStats, TaskMemoryManager taskMemoryManager)
        {
            checkState(!closed, "Factory is already closed");
            return new NewAlignmentOperator(channels);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final List<TupleInfo> tupleInfos;
    private final Optional<DataSize> expectedDataSize;
    private final Optional<Integer> expectedPositionCount;

    private final List<Iterator<Block>> iterators;
    private final List<BlockCursor> cursors;

    private boolean finished;

    public NewAlignmentOperator(BlockIterable... channels)
    {
        this(ImmutableList.copyOf(channels));
    }

    public NewAlignmentOperator(Iterable<BlockIterable> channels)
    {
        this.tupleInfos = toTupleInfos(channels);

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

        return new Page(blocks);
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
