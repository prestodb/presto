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

import com.facebook.presto.execution.SystemMemoryUsageListener;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.MergeSortProcessor.PageWithPosition;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class MergeOperator
        implements SourceOperator, Closeable
{
    public static class MergeOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final ExchangeClientSupplier exchangeClientSupplier;
        private final PagesSerdeFactory serdeFactory;
        private final List<Type> types;
        private final List<Integer> outputChannels;
        private final List<Type> outputTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final OrderingCompiler orderingCompiler;
        private boolean closed;

        public MergeOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                ExchangeClientSupplier exchangeClientSupplier,
                PagesSerdeFactory serdeFactory,
                OrderingCompiler orderingCompiler,
                List<Type> types,
                List<Integer> outputChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
            this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
            this.types = requireNonNull(types, "types is null");
            this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
            this.outputTypes = outputTypes(types, outputChannels);
            this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
            this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
            this.orderingCompiler = requireNonNull(orderingCompiler, "mergeSortComparatorFactory is null");
        }

        private static List<Type> outputTypes(List<? extends Type> sourceTypes, List<Integer> outputChannels)
        {
            return outputChannels.stream()
                    .map(sourceTypes::get)
                    .collect(toImmutableList());
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public List<Type> getTypes()
        {
            return outputTypes;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, MergeOperator.class.getSimpleName());

            return new MergeOperator(
                    operatorContext,
                    sourceId,
                    () -> exchangeClientSupplier.get(new UpdateSystemMemory(driverContext.getPipelineContext())),
                    serdeFactory.createPagesSerde(),
                    orderingCompiler.compilePageComparator(types, sortChannels, sortOrder),
                    outputChannels,
                    outputTypes);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    @NotThreadSafe
    private static final class UpdateSystemMemory
            implements SystemMemoryUsageListener
    {
        private final PipelineContext pipelineContext;

        public UpdateSystemMemory(PipelineContext pipelineContext)
        {
            this.pipelineContext = requireNonNull(pipelineContext, "pipelineContext is null");
        }

        @Override
        public void updateSystemMemoryUsage(long deltaMemoryInBytes)
        {
            if (deltaMemoryInBytes > 0) {
                pipelineContext.reserveSystemMemory(deltaMemoryInBytes);
            }
            else {
                pipelineContext.freeSystemMemory(-deltaMemoryInBytes);
            }
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final Supplier<ExchangeClient> exchangeClientSupplier;
    private final PagesSerde pagesSerde;
    private final PageComparator comparator;
    private final List<Integer> outputChannels;
    private final List<Type> outputTypes;
    private final PageBuilder pageBuilder;
    private final LocalMemoryContext pageBuilderLocalMemoryContext;

    private final SettableFuture<Void> blockedOnSplits = SettableFuture.create();

    private final List<PageSupplier> pageSuppliers = new ArrayList<>();
    private final Closer closer = Closer.create();

    private boolean closed;

    private MergeSortProcessor mergeSortProcessor;

    public MergeOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            Supplier<ExchangeClient> exchangeClientSupplier,
            PagesSerde pagesSerde,
            PageComparator comparator,
            List<Integer> outputChannels,
            List<Type> outputTypes)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.comparator = requireNonNull(comparator, "comparator is null");
        this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
        this.outputTypes = requireNonNull(outputTypes, "outputTypes is null");
        this.pageBuilder = new PageBuilder(outputTypes);
        this.pageBuilderLocalMemoryContext = operatorContext.getSystemMemoryContext().newLocalMemoryContext();
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split.getConnectorSplit() instanceof RemoteSplit, "split is not a remote split");
        checkState(!blockedOnSplits.isDone(), "noMoreSplits has been called already");

        URI location = ((RemoteSplit) split.getConnectorSplit()).getLocation();
        ExchangeClient exchangeClient = closer.register(exchangeClientSupplier.get());
        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();
        pageSuppliers.add(new RemoteExchangePageSupplier(exchangeClient, pagesSerde));

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        mergeSortProcessor = new MergeSortProcessor(comparator, ImmutableList.copyOf(pageSuppliers), operatorContext.getSystemMemoryContext().newAggregatedMemoryContext());
        closer.register(mergeSortProcessor);
        blockedOnSplits.set(null);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return outputTypes;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        return closed || (mergeSortProcessor != null && mergeSortProcessor.isFinished() && pageBuilder.isEmpty());
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!blockedOnSplits.isDone()) {
            return blockedOnSplits;
        }
        return mergeSortProcessor.isBlocked();
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
        checkState(mergeSortProcessor != null, "mergeSources must be non-null when getting output");

        if (closed) {
            return null;
        }

        while (!pageBuilder.isFull()) {
            PageWithPosition pageWithPosition = mergeSortProcessor.poll();
            if (pageWithPosition == null) {
                break;
            }

            pageWithPosition.appendTo(pageBuilder, outputChannels, outputTypes);
            pageBuilderLocalMemoryContext.setBytes(pageBuilder.getRetainedSizeInBytes());
        }

        if (pageBuilder.isEmpty()) {
            return null;
        }

        // As in LookupJoinOperator, only flush full pages unless we are done
        if (pageBuilder.isFull() || mergeSortProcessor.isFinished()) {
            Page page = pageBuilder.build();
            operatorContext.recordGeneratedInput(page.getSizeInBytes(), page.getPositionCount());
            pageBuilder.reset();
            return page;
        }

        return null;
    }

    @Override
    public void close()
    {
        try {
            closed = true;
            closer.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
