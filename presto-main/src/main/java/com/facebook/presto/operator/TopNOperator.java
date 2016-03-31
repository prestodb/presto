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

import com.facebook.presto.operator.aggregation.TypedMultiChannelHeap;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

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
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final int n;
        private final List<Type> sortTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrders;
        private final boolean partial;
        private boolean closed;

        public TopNOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> types,
                int n,
                List<Integer> sortChannels,
                List<SortOrder> sortOrders,
                boolean partial)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.n = n;
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
            return new TopNOperatorFactory(operatorId, planNodeId, sourceTypes, n, sortChannels, sortOrders, partial);
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

    private final PageBuilder pageBuilder;

    //private TopNBuilder topNBuilder;
    private TypedMultiChannelHeap channelHeap;
    private boolean finishing;

    //private Iterator<Integer> outputIterator;
    private TypedMultiChannelHeap.TypedMultiChannelHeapPageFiller pageFiller;

    public TopNOperator(
            OperatorContext operatorContext,
            List<Type> types,
            int n,
            List<Type> sortTypes,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            boolean partial)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.types = requireNonNull(types, "types is null");

        checkArgument(n >= 0, "n must be positive");
        this.n = n;

        this.sortTypes = requireNonNull(sortTypes, "sortTypes is null");
        this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
        this.sortOrders = requireNonNull(sortOrders, "sortOrders is null");

        this.partial = partial;

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
        return finishing && channelHeap == null && (pageFiller == null || pageFiller.isEmpty());
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && (pageFiller == null || pageFiller.isEmpty()) && (channelHeap == null || !isHeapFull());
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        if (channelHeap == null) {
            channelHeap = new TypedMultiChannelHeap(
                    this.types,
                    this.sortChannels,
                    this.sortOrders,
                    n);
        }

        checkState(!isHeapFull(), "Aggregation buffer is full");
        channelHeap.addPage(page);
    }

    @Override
    public Page getOutput()
    {
        if (pageFiller == null || pageFiller.isEmpty()) {
            // no data
            if (channelHeap == null) {
                return null;
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && !isHeapFull()) {
                return null;
            }

            // Only partial aggregation can flush early. Also, check that we are not flushing tiny bits at a time
            if (finishing || partial) {
                pageFiller = channelHeap.build();
                channelHeap = null;
            }
        }

        pageBuilder.reset();
        pageFiller.fillPage(pageBuilder);
        return pageBuilder.build();
    }

    public boolean isHeapFull()
    {
        long memorySize = channelHeap.getEstimatedSize() - operatorContext.getOperatorPreAllocatedMemory().toBytes();
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
}
