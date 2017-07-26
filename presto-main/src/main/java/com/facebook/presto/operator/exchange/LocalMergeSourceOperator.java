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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.MergeSortProcessor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PageComparator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class LocalMergeSourceOperator
        implements Operator
{
    public static class LocalMergeSourceOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final LocalMergeExchange exchange;
        private final List<Type> types;
        private final OrderingCompiler orderingCompiler;
        private final List<Integer> sortChannels;
        private final List<SortOrder> orderings;

        private boolean created;
        private boolean closed;

        public LocalMergeSourceOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                LocalMergeExchange exchange,
                List<Type> types,
                OrderingCompiler orderingCompiler,
                List<Integer> sortChannels,
                List<SortOrder> orderings)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.exchange = requireNonNull(exchange, "exchange is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.orderings = ImmutableList.copyOf(requireNonNull(orderings, "orderings is null"));
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            checkState(!created, "Single instance is expected to be created");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LocalMergeSourceOperator.class.getSimpleName());
            PageComparator comparator = orderingCompiler.compilePageComparator(types, sortChannels, orderings);
            LocalMergeSourceOperator localMergeSourceOperator = new LocalMergeSourceOperator(operatorContext, exchange, types, comparator);
            created = true;
            return localMergeSourceOperator;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories can not be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMergeExchange exchange;
    private final List<Type> types;
    private final MergeSortProcessor processor;
    private final PageBuilder pageBuilder;
    private final LocalMemoryContext pageBuilderLocalMemoryContext;

    public LocalMergeSourceOperator(OperatorContext operatorContext, LocalMergeExchange exchange, List<Type> types, PageComparator comparator)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.exchange = requireNonNull(exchange, "exchange is null");
        this.types = requireNonNull(types, "types is null");
        ImmutableList<LocalExchangePageSupplier> pageSuppliers = exchange.getBuffers().stream()
                .map(LocalExchangePageSupplier::new)
                .collect(toImmutableList());
        processor = new MergeSortProcessor(requireNonNull(comparator, "comparator is null"), pageSuppliers, operatorContext.getSystemMemoryContext().newAggregatedMemoryContext());
        pageBuilder = new PageBuilder(types);
        this.pageBuilderLocalMemoryContext = operatorContext.getSystemMemoryContext().newLocalMemoryContext();
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
        exchange.finishRead();
    }

    @Override
    public boolean isFinished()
    {
        return processor.isFinished() && pageBuilder.isEmpty();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return processor.isBlocked();
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        while (!pageBuilder.isFull()) {
            MergeSortProcessor.PageWithPosition pageWithPosition = processor.poll();
            if (pageWithPosition == null) {
                break;
            }

            Page page = pageWithPosition.getPage();

            // append the row
            List<Integer> outputChannels = IntStream.range(0, page.getChannelCount()).boxed().collect(toImmutableList());
            pageWithPosition.appendTo(pageBuilder, outputChannels, getTypes());
            pageBuilderLocalMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
        }

        if (pageBuilder.isEmpty()) {
            return null;
        }

        // As in LookupJoinOperator, only flush full pages unless we are done
        if (pageBuilder.isFull() || processor.isFinished()) {
            Page page = pageBuilder.build();
            operatorContext.recordGeneratedInput(page.getSizeInBytes(), page.getPositionCount());
            pageBuilder.reset();
            return page;
        }

        return null;
    }

    @Override
    public void close()
            throws IOException
    {
        exchange.close();
        processor.close();
    }
}
