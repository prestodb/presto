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

import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import io.airlift.units.DataSize;

import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

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
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final int n;
        private final List<Type> sortTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private final boolean partial;
        private final DataSize maxPartialMemory;
        private boolean closed;

        public TopNOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types,
                int n,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                boolean partial,
                DataSize maxPartialMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.n = n;
            this.maxPartialMemory = maxPartialMemory;
            ImmutableList.Builder<Type> sortTypes = ImmutableList.builder();
            for (int channel : sortChannels) {
                sortTypes.add(types.get(channel));
            }
            this.sortTypes = sortTypes.build();
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrders = ImmutableList.copyOf(requireNonNull(sortOrders, "sortOrders is null"));
            this.partial = partial;
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TopNOperator.class.getSimpleName());
            return new TopNOperator(
                    operatorContext,
                    sourceTypes,
                    n,
                    sortTypes,
                    sortChannels,
                    sortOrders,
                    partial,
                    maxPartialMemory);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TopNOperatorFactory(operatorId, planNodeId, sourceTypes, n, sortChannels, sortOrders, partial, maxPartialMemory);
        }
    }

    private static final int MAX_INITIAL_PRIORITY_QUEUE_SIZE = 10000;
    private static final DataSize OVERHEAD_PER_VALUE = new DataSize(100, DataSize.Unit.BYTE); // for estimating in-memory size. This is a completely arbitrary number

    private final OperatorContext operatorContext;
    private final List<Type> types;
    private final int n;
    private final List<Type> sortTypes;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final boolean partial;
    private final DataSize maxPartialMemory;

    private final PageBuilder pageBuilder;

    private TopNBuilder topNBuilder;
    private boolean finishing;

    private Iterator<Block[]> outputIterator;

    public TopNOperator(
            OperatorContext operatorContext,
            List<Type> types,
            int n,
            List<Type> sortTypes,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            boolean partial,
            DataSize maxPartialMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = requireNonNull(types, "types is null");

        checkArgument(n >= 0, "n must be positive");
        this.n = n;

        this.sortTypes = requireNonNull(sortTypes, "sortTypes is null");
        this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
        this.sortOrders = requireNonNull(sortOrders, "sortOrders is null");

        this.partial = partial;
        this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");

        this.pageBuilder = new PageBuilder(types);

        if (n == 0) {
            finishing = true;
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
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && topNBuilder == null && (outputIterator == null || !outputIterator.hasNext());
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && (outputIterator == null || !outputIterator.hasNext()) && (topNBuilder == null || !topNBuilder.isFull());
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        if (topNBuilder == null) {
            topNBuilder = new TopNBuilder(
                    n,
                    partial,
                    sortTypes,
                    sortChannels,
                    sortOrders,
                    operatorContext,
                    maxPartialMemory);
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
            if (finishing || partial) {
                outputIterator = topNBuilder.build();
                topNBuilder = null;
            }
        }

        pageBuilder.reset();
        while (!pageBuilder.isFull() && outputIterator.hasNext()) {
            Block[] next = outputIterator.next();
            pageBuilder.declarePosition();
            for (int i = 0; i < next.length; i++) {
                Type type = types.get(i);
                type.appendTo(next[i], 0, pageBuilder.getBlockBuilder(i));
            }
        }

        return pageBuilder.build();
    }

    private static class TopNBuilder
    {
        private final int n;
        private final boolean partial;
        private final List<Type> sortTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private final OperatorContext operatorContext;
        private final LocalMemoryContext systemMemoryContext;
        private final long maxPartialMemory;
        private final PriorityQueue<Block[]> globalCandidates;

        private long memorySize;

        private TopNBuilder(int n,
                boolean partial,
                List<Type> sortTypes,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                OperatorContext operatorContext,
                DataSize maxPartialMemory)
        {
            this.n = n;
            this.partial = partial;

            this.sortTypes = sortTypes;
            this.sortChannels = sortChannels;
            this.sortOrders = sortOrders;

            this.operatorContext = operatorContext;
            this.systemMemoryContext = operatorContext.getSystemMemoryContext().newLocalMemoryContext();
            this.maxPartialMemory = maxPartialMemory.toBytes();

            Ordering<Block[]> comparator = Ordering.from(new RowComparator(sortTypes, sortChannels, sortOrders)).reverse();
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

            Block[] blocks = page.getBlocks();
            for (int position = 0; position < page.getPositionCount(); position++) {
                if (globalCandidates.size() < n || compare(position, blocks, globalCandidates.peek()) < 0) {
                    sizeDelta += addRow(position, blocks);
                }
            }

            return sizeDelta;
        }

        private int compare(int position, Block[] blocks, Block[] currentMax)
        {
            for (int i = 0; i < sortChannels.size(); i++) {
                Type type = sortTypes.get(i);
                int sortChannel = sortChannels.get(i);
                SortOrder sortOrder = sortOrders.get(i);

                Block block = blocks[sortChannel];
                Block currentMaxValue = currentMax[sortChannel];

                // compare the right value to the left block but negate the result since we are evaluating in the opposite order
                int compare = -sortOrder.compareBlockValue(type, currentMaxValue, 0, block, position);
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        }

        private long addRow(int position, Block[] blocks)
        {
            long sizeDelta = 0;
            Block[] row = getValues(position, blocks);

            sizeDelta += sizeOfRow(row);
            globalCandidates.add(row);

            while (globalCandidates.size() > n) {
                Block[] previous = globalCandidates.remove();
                sizeDelta -= sizeOfRow(previous);
            }
            return sizeDelta;
        }

        private static long sizeOfRow(Block[] row)
        {
            long size = OVERHEAD_PER_VALUE.toBytes();
            for (Block value : row) {
                size += value.getRetainedSizeInBytes();
            }
            return size;
        }

        private static Block[] getValues(int position, Block[] blocks)
        {
            Block[] row = new Block[blocks.length];
            for (int i = 0; i < blocks.length; i++) {
                row[i] = blocks[i].getSingleValueBlock(position);
            }
            return row;
        }

        private boolean isFull()
        {
            long memorySize = this.memorySize;
            if (partial) {
                systemMemoryContext.setBytes(memorySize);
                return (memorySize > maxPartialMemory);
            }
            else {
                operatorContext.setMemoryReservation(memorySize);
                return false;
            }
        }

        public Iterator<Block[]> build()
        {
            ImmutableList.Builder<Block[]> minSortedGlobalCandidates = ImmutableList.builder();
            while (!globalCandidates.isEmpty()) {
                Block[] row = globalCandidates.remove();
                minSortedGlobalCandidates.add(row);
            }
            return minSortedGlobalCandidates.build().reverse().iterator();
        }
    }
}
