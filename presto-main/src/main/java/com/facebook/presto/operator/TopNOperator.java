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

import com.facebook.presto.operator.MultiChannelTopNAccumulator.BlocksSnapShot;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.List;

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
        private final List<Type> sourceTypes;
        private final int n;
        private final List<Type> sortTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private final boolean partial;
        private boolean closed;

        public TopNOperatorFactory(
                int operatorId,
                List<? extends Type> sourceTypes,
                int n,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                boolean partial)
        {
            this.operatorId = operatorId;
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.n = n;
            ImmutableList.Builder<Type> sortTypes = ImmutableList.builder();
            for (int channel : sortChannels) {
                sortTypes.add(sourceTypes.get(channel));
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
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, TopNOperator.class.getSimpleName());
            return new TopNOperator(
                    operatorContext,
                    sourceTypes,
                    n,
                    sortTypes,
                    sortChannels,
                    sortOrders,
                    partial);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TopNOperatorFactory(operatorId, sourceTypes, n, sortChannels, sortOrders, partial);
        }
    }

    private static final int MAX_INITIAL_PRIORITY_QUEUE_SIZE = 10000;

    private final OperatorContext operatorContext;
    private final List<Type> sourceTypes;
    private final int n;
    private final List<Type> sortTypes;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrders;
    private final boolean partial;

    private final PageBuilder pageBuilder;

    private TopNBuilder topNBuilder;
    private boolean finishing;

    private BlocksSnapShot outputIterator;

    public TopNOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            int n,
            List<Type> sortTypes,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            boolean partial)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");

        checkArgument(n >= 0, "n must be positive");
        this.n = n;

        this.sortTypes = requireNonNull(sortTypes, "sortTypes is null");
        this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
        this.sortOrders = requireNonNull(sortOrders, "sortOrders is null");

        this.partial = partial;

        this.pageBuilder = new PageBuilder(sourceTypes);

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
        return sourceTypes;
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
                    sourceTypes,
                    sortTypes,
                    sortChannels,
                    sortOrders,
                    operatorContext);
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
            pageBuilder.declarePosition();

            outputIterator.popRow(pageBuilder.getBlockBuilders());
        }

        return pageBuilder.build();
    }

    private static class TopNBuilder
    {
        private final int n;
        private final boolean partial;
        private final List<Type> sourceTypes;
        private final List<Type> sortTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private final OperatorContext operatorContext;
        private MultiChannelTopNAccumulator globalCandidates;
        private Ordering<Block[]> comparator;

        private long memorySize;

        private TopNBuilder(int n,
                boolean partial,
                List<Type> sourceTypes,
                List<Type> sortTypes,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                OperatorContext operatorContext)
        {
            this.n = n;
            this.partial = partial;

            this.sourceTypes = sourceTypes;
            this.sortTypes = sortTypes;
            this.sortChannels = sortChannels;
            this.sortOrders = sortOrders;

            this.operatorContext = operatorContext;

            comparator = Ordering.from(new RowComparator(sortTypes, sortChannels, sortOrders)).reverse();
        }

        public void processPage(Page page)
        {
            long sizeDelta = mergeWithGlobalCandidates(page);
            memorySize += sizeDelta;
        }

        private long mergeWithGlobalCandidates(Page page)
        {
            long sizeDelta = 0;

            Block[] blocksInCurrentPage = page.getBlocks();
            // here we use a heap that only references the original blocks by pointers, to avoid excessive copy
            // once we scraped through all the rows in the blocks, and found the topN positions,
            // we copy out those rows into the global topN buffer and update that buffer.
            if (globalCandidates == null) {
                globalCandidates = new MultiChannelTopNAccumulator(sourceTypes, n, sortChannels, sortTypes, sortOrders);
            }

            for (int position = 0; position < page.getPositionCount(); position++) {
                sizeDelta += globalCandidates.add(blocksInCurrentPage, position);
            }

            return sizeDelta;
        }

        private boolean isFull()
        {
            long memorySize = this.memorySize - operatorContext.getOperatorPreAllocatedMemory().toBytes();
            if (memorySize < 0) {
                memorySize = 0;
            }
            if (partial) {
                return !operatorContext.trySetMemoryReservation(memorySize);
            }
            else {
                operatorContext.setMemoryReservation(memorySize);
                return false;
            }
        }

        public BlocksSnapShot build()
        {
            return globalCandidates.flushContent();
        }
    }
}
