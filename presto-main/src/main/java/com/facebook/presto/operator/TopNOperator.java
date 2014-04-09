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

import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Returns the top N rows from the source sorted according to the specified ordering in the keyChannelIndex channel.
 */
public class TopNOperator
        implements Operator
{
    public static class TopNOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> sourceTypes;
        private final int n;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private final Optional<Integer> sampleWeight;
        private final boolean partial;
        private boolean closed;

        public TopNOperatorFactory(
                int operatorId,
                List<? extends Type> types,
                int n,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                Optional<Integer> sampleWeight,
                boolean partial)
        {
            this.operatorId = operatorId;
            this.sourceTypes = ImmutableList.copyOf(checkNotNull(types, "types is null"));
            this.n = n;
            this.sortChannels = ImmutableList.copyOf(checkNotNull(sortChannels, "sortChannels is null"));
            this.sortOrders = ImmutableList.copyOf(checkNotNull(sortOrders, "sortOrders is null"));
            this.partial = partial;
            this.sampleWeight = sampleWeight;
        }

        @Override
        public List<Type> getTypes()
        {
            return sourceTypes;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, TopNOperator.class.getSimpleName());
            return new TopNOperator(
                    operatorContext,
                    sourceTypes,
                    n,
                    sortChannels,
                    sortOrders,
                    sampleWeight,
                    partial);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private static final int MAX_INITIAL_PRIORITY_QUEUE_SIZE = 10000;
    private static final DataSize OVERHEAD_PER_VALUE = new DataSize(100, DataSize.Unit.BYTE); // for estimating in-memory size. This is a completely arbitrary number

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final int n;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final TopNMemoryManager memoryManager;
    private final boolean partial;
    private final Optional<Integer> sampleWeight;

    private final PageBuilder pageBuilder;

    private TopNBuilder topNBuilder;
    private boolean finishing;

    private Iterator<RandomAccessBlock[]> outputIterator;

    public TopNOperator(
            OperatorContext operatorContext,
            List<Type> types,
            int n,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            Optional<Integer> sampleWeight,
            boolean partial)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.types = checkNotNull(types, "types is null");

        checkArgument(n > 0, "n must be greater than zero");
        this.n = n;

        this.sortChannels = checkNotNull(sortChannels, "sortChannels is null");
        this.sortOrders = checkNotNull(sortOrders, "sortOrders is null");

        this.partial = partial;

        this.memoryManager = new TopNMemoryManager(checkNotNull(operatorContext, "operatorContext is null"));

        this.pageBuilder = new PageBuilder(getTypes());

        this.sampleWeight = sampleWeight;
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
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && topNBuilder == null && (outputIterator == null || !outputIterator.hasNext());
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && outputIterator == null && (topNBuilder == null || !topNBuilder.isFull());
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkNotNull(page, "page is null");
        if (topNBuilder == null) {
            topNBuilder = new TopNBuilder(
                    n,
                    sortChannels,
                    sortOrders,
                    sampleWeight,
                    memoryManager);
        }

        checkState(!topNBuilder.isFull(), "Aggregation buffer is full");
        topNBuilder.processPage(page);
    }

    @Override
    public Page getOutput()
    {
        if (outputIterator == null || !outputIterator.hasNext()) {
            // no data
            if (topNBuilder == null) {
                return null;
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && !topNBuilder.isFull()) {
                return null;
            }

            // Only partial aggregation can flush early. Also, check that we are not flushing tiny bits at a time
            checkState(finishing || partial, "Task exceeded max memory size of %s", memoryManager.getMaxMemorySize());

            outputIterator = topNBuilder.build();
            topNBuilder = null;
        }

        pageBuilder.reset();
        while (!pageBuilder.isFull() && outputIterator.hasNext()) {
            RandomAccessBlock[] next = outputIterator.next();
            for (int i = 0; i < next.length; i++) {
                next[i].appendTo(0, pageBuilder.getBlockBuilder(i));
            }
        }

        Page page = pageBuilder.build();
        return page;
    }

    private static class TopNBuilder
    {
        private final int n;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private final TopNMemoryManager memoryManager;
        private final PriorityQueue<RandomAccessBlock[]> globalCandidates;
        private final Optional<Integer> sampleWeightChannel;

        private long memorySize;

        private TopNBuilder(int n, List<Integer> sortChannels, List<SortOrder> sortOrders, Optional<Integer> sampleWeightChannel, TopNMemoryManager memoryManager)
        {
            this.n = n;

            this.sortChannels = sortChannels;
            this.sortOrders = sortOrders;

            this.memoryManager = memoryManager;
            this.sampleWeightChannel = sampleWeightChannel;

            Ordering<RandomAccessBlock[]> comparator = Ordering.from(new RowComparator(sortChannels, sortOrders)).reverse();
            this.globalCandidates = new PriorityQueue<>(Math.min(n, MAX_INITIAL_PRIORITY_QUEUE_SIZE), comparator);
        }

        public void processPage(Page page)
        {
            long sizeDelta = mergeWithGlobalCandidates(page);
            memorySize += sizeDelta;
        }

        private long mergeWithGlobalCandidates(Page page)
        {
            long sizeDelta = 0;

            BlockCursor[] cursors = new BlockCursor[page.getChannelCount()];
            for (int i = 0; i < page.getChannelCount(); i++) {
                cursors[i] = page.getBlock(i).cursor();
            }

            for (int i = 0; i < page.getPositionCount(); i++) {
                for (BlockCursor cursor : cursors) {
                    checkState(cursor.advanceNextPosition());
                }

                if (globalCandidates.size() < n) {
                    sizeDelta += addRow(cursors);
                }
                else if (compare(cursors, globalCandidates.peek()) < 0) {
                    sizeDelta += addRow(cursors);
                }
            }

            return sizeDelta;
        }

        private int compare(BlockCursor[] cursors, RandomAccessBlock[] currentMax)
        {
            for (int i = 0; i < sortChannels.size(); i++) {
                int sortChannel = sortChannels.get(i);
                SortOrder sortOrder = sortOrders.get(i);

                BlockCursor cursor = cursors[sortChannel];
                RandomAccessBlock currentMaxValue = currentMax[sortChannel];

                // compare the right value to the left cursor but negate the result since we are evaluating in the opposite order
                int compare = -currentMaxValue.compareTo(sortOrder, 0, cursor);
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        }

        private long addRow(BlockCursor[] cursors)
        {
            long sizeDelta = 0;
            RandomAccessBlock[] row = getValues(cursors);
            long sampleWeight = 1;
            if (sampleWeightChannel.isPresent()) {
                sampleWeight = row[sampleWeightChannel.get()].getLong(0);
                // Set the weight to one, since we're going to insert it multiple times in the priority queue
                row[sampleWeightChannel.get()] = createBigintBlock(1);
            }

            // Count the column sizes only once, because we insert the same object reference multiple times for sampled rows
            sizeDelta += sizeOfRow(row);
            globalCandidates.add(row);
            sizeDelta += (sampleWeight - 1) * OVERHEAD_PER_VALUE.toBytes();
            for (int i = 1; i < sampleWeight; i++) {
                globalCandidates.add(row);
            }

            while (globalCandidates.size() > n) {
                RandomAccessBlock[] previous = globalCandidates.remove();
                // We insert sampled rows multiple times, so use reference equality when checking if this row is still in the queue
                if (previous != globalCandidates.peek()) {
                    sizeDelta -= sizeOfRow(previous);
                }
                else {
                    sizeDelta -= OVERHEAD_PER_VALUE.toBytes();
                }
            }
            return sizeDelta;
        }

        private long sizeOfRow(RandomAccessBlock[] row)
        {
            long size = OVERHEAD_PER_VALUE.toBytes();
            for (RandomAccessBlock value : row) {
                size += value.getSizeInBytes();
            }
            return size;
        }

        private RandomAccessBlock[] getValues(BlockCursor[] cursors)
        {
            RandomAccessBlock[] row = new RandomAccessBlock[cursors.length];
            for (int i = 0; i < cursors.length; i++) {
                row[i] = cursors[i].getSingleValueBlock();
            }
            return row;
        }

        private boolean isFull()
        {
            return memoryManager.canUse(memorySize);
        }

        public Iterator<RandomAccessBlock[]> build()
        {
            ImmutableList.Builder<RandomAccessBlock[]> minSortedGlobalCandidates = ImmutableList.builder();
            long sampleWeight = 1;
            while (!globalCandidates.isEmpty()) {
                RandomAccessBlock[] row = globalCandidates.remove();
                if (sampleWeightChannel.isPresent()) {
                    // sampled rows are inserted multiple times (we can use identity comparison here)
                    // we could also test for equality to "pack" results further, but that would require another equality function
                    if (globalCandidates.peek() != null && row == globalCandidates.peek()) {
                        sampleWeight++;
                    }
                    else {
                        row[sampleWeightChannel.get()] = createBigintBlock(sampleWeight);
                        minSortedGlobalCandidates.add(row);
                        sampleWeight = 1;
                    }
                }
                else {
                    minSortedGlobalCandidates.add(row);
                }
            }
            return minSortedGlobalCandidates.build().reverse().iterator();
        }

        private static RandomAccessBlock createBigintBlock(long value)
        {
            return BIGINT.createBlockBuilder(new BlockBuilderStatus())
                    .append(value)
                    .build()
                    .toRandomAccessBlock();
        }
    }

    public static class TopNMemoryManager
    {
        private final OperatorContext operatorContext;
        private long currentMemoryReservation;

        public TopNMemoryManager(OperatorContext operatorContext)
        {
            this.operatorContext = operatorContext;
        }

        public boolean canUse(long memorySize)
        {
            // remove the pre-allocated memory from this size
            memorySize -= operatorContext.getOperatorPreAllocatedMemory().toBytes();

            long delta = memorySize - currentMemoryReservation;
            if (delta <= 0) {
                return false;
            }

            if (!operatorContext.reserveMemory(delta)) {
                return true;
            }

            // reservation worked, record the reservation
            currentMemoryReservation = Math.max(currentMemoryReservation, memorySize);
            return false;
        }

        public DataSize getMaxMemorySize()
        {
            return operatorContext.getMaxMemorySize();
        }
    }

    private static class RowComparator
            implements Comparator<RandomAccessBlock[]>
    {
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;

        public RowComparator(List<Integer> sortChannels, List<SortOrder> sortOrders)
        {
            checkNotNull(sortChannels, "sortChannels is null");
            checkNotNull(sortOrders, "sortOrders is null");
            checkArgument(sortChannels.size() == sortOrders.size(), "sortFields size (%s) doesn't match sortOrders size (%s)", sortChannels.size(), sortOrders.size());

            this.sortChannels = ImmutableList.copyOf(sortChannels);
            this.sortOrders = ImmutableList.copyOf(sortOrders);
        }

        @Override
        public int compare(RandomAccessBlock[] leftRow, RandomAccessBlock[] rightRow)
        {
            for (int index = 0; index < sortChannels.size(); index++) {
                int channel = sortChannels.get(index);
                SortOrder sortOrder = sortOrders.get(index);

                RandomAccessBlock left = leftRow[channel];
                RandomAccessBlock right = rightRow[channel];

                int comparison = left.compareTo(sortOrder, 0, right, 0);
                if (comparison != 0) {
                    return comparison;
                }
            }
            return 0;
        }
    }
}
