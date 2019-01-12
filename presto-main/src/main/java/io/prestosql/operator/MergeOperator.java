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
package io.prestosql.operator;

import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.execution.buffer.PagesSerde;
import io.prestosql.execution.buffer.PagesSerdeFactory;
import io.prestosql.metadata.Split;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.type.Type;
import io.prestosql.split.RemoteSplit;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.util.MergeSortedPages.mergeSortedPages;
import static io.prestosql.util.MoreLists.mappedCopy;
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
            this.outputTypes = mappedCopy(outputChannels, types::get);
            this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
            this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
            this.orderingCompiler = requireNonNull(orderingCompiler, "mergeSortComparatorFactory is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, MergeOperator.class.getSimpleName());

            return new MergeOperator(
                    operatorContext,
                    sourceId,
                    exchangeClientSupplier,
                    serdeFactory.createPagesSerde(),
                    orderingCompiler.compilePageWithPositionComparator(types, sortChannels, sortOrder),
                    outputChannels,
                    outputTypes);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final PagesSerde pagesSerde;
    private final PageWithPositionComparator comparator;
    private final List<Integer> outputChannels;
    private final List<Type> outputTypes;

    private final SettableFuture<Void> blockedOnSplits = SettableFuture.create();

    private final List<WorkProcessor<Page>> pageProducers = new ArrayList<>();
    private final Closer closer = Closer.create();

    private WorkProcessor<Page> mergedPages;
    private boolean closed;

    public MergeOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            ExchangeClientSupplier exchangeClientSupplier,
            PagesSerde pagesSerde,
            PageWithPositionComparator comparator,
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
        ExchangeClient exchangeClient = closer.register(exchangeClientSupplier.get(operatorContext.localSystemMemoryContext()));
        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();
        pageProducers.add(exchangeClient.pages()
                .map(serializedPage -> {
                    operatorContext.recordRawInput(serializedPage.getSizeInBytes());
                    return pagesSerde.deserialize(serializedPage);
                }));

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        mergedPages = mergeSortedPages(
                pageProducers,
                comparator,
                outputChannels,
                outputTypes,
                (pageBuilder, pageWithPosition) -> pageBuilder.isFull(),
                false,
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.getDriverContext().getYieldSignal());
        blockedOnSplits.set(null);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        return closed || (mergedPages != null && mergedPages.isFinished());
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!blockedOnSplits.isDone()) {
            return blockedOnSplits;
        }

        if (mergedPages.isBlocked()) {
            return mergedPages.getBlockedFuture();
        }

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
        if (closed || mergedPages == null || !mergedPages.process() || mergedPages.isFinished()) {
            return null;
        }

        Page page = mergedPages.getResult();
        operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        return page;
    }

    @Override
    public void close()
    {
        try {
            closer.close();
            closed = true;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
