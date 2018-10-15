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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.Spiller;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.util.MergeSortedPages.mergeSortedPages;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class OrderByOperator
        implements Operator
{
    public static class OrderByOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final int expectedPositions;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private boolean closed;
        private final PagesIndex.Factory pagesIndexFactory;
        private final boolean spillEnabled;
        private final Optional<SpillerFactory> spillerFactory;

        public OrderByOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                int expectedPositions,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                Optional<SpillerFactory> spillerFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
            this.expectedPositions = expectedPositions;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));

            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;

            checkState(!spillEnabled || spillerFactory.isPresent(), "Spiller Factory is not present when spill is enabled");

            this.spillerFactory = spillerFactory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OrderByOperator.class.getSimpleName());
            return new OrderByOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    expectedPositions,
                    sortChannels,
                    sortOrder,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new OrderByOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, expectedPositions, sortChannels, sortOrder, pagesIndexFactory, spillEnabled, spillerFactory);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final int[] outputChannels;
    private final LocalMemoryContext pagesIndexRevocableMemoryContext;
    private final LocalMemoryContext pagesIndexUserMemoryContext;

    private final PagesIndex pageIndex;

    private final List<Type> sourceTypes;

    private final boolean spillEnabled;
    private final Optional<SpillerFactory> spillerFactory;
    private Optional<Spiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = immediateFuture(null);

    private Iterator<Page> sortedPages;

    private State state = State.NEEDS_INPUT;

    public OrderByOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            int expectedPositions,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            Optional<SpillerFactory> spillerFactory)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(requireNonNull(outputChannels, "outputChannels is null"));
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        this.sourceTypes = sourceTypes;
        this.pagesIndexUserMemoryContext = operatorContext.localUserMemoryContext();
        this.pagesIndexRevocableMemoryContext = operatorContext.localRevocableMemoryContext();

        this.spillerFactory = spillerFactory;
        this.pageIndex = pagesIndexFactory.newPagesIndex(sourceTypes, expectedPositions);

        this.spillEnabled = spillEnabled;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;

            WorkProcessor<Page> resultPages = getSortedPages(
                    new SimplePageWithPositionComparator(sourceTypes, sortChannels, sortOrder));

            sortedPages = resultPages.iterator();
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        requireNonNull(page, "page is null");

        pageIndex.addPage(page);
        updateMemoryUsage(spillEnabled);
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        if (!sortedPages.hasNext()) {
            state = State.FINISHED;
            return null;
        }

        Page resultPage = sortedPages.next();
        Block[] blocksArray = new Block[outputChannels.length];

        for (int i = 0; i < outputChannels.length; i++) {
            blocksArray[i] = resultPage.getBlock(outputChannels[i]);
        }

        return new Page(resultPage.getPositionCount(), blocksArray);
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        return spillToDisk();
    }

    public ListenableFuture<?> spillToDisk()
    {
        checkState(hasPreviousSpillCompletedSuccessfully(), "Previous spill hasn't yet finished");

        if (pagesIndexRevocableMemoryContext.getBytes() == 0 ||
                pageIndex.getPositionCount() == 0 ||
                state == State.HAS_OUTPUT) {
            return Futures.immediateFuture(null);
        }

        if (!spiller.isPresent()) {
            this.spiller = Optional.of(spillerFactory.get().create(
                    sourceTypes,
                    operatorContext.getSpillContext(),
                    operatorContext.newAggregateSystemMemoryContext()));
        }

        if (pageIndex.getPositionCount() == 0) {
            return spillInProgress;
        }

        pageIndex.sort(sortChannels, sortOrder);
        spillInProgress = spiller.get().spill(pageIndex.getSortedPages());

        return spillInProgress;
    }

    private boolean hasPreviousSpillCompletedSuccessfully()
    {
        if (spillInProgress.isDone()) {
            // check for exception from previous spill for early failure
            getFutureValue(spillInProgress);
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public void finishMemoryRevoke()
    {
        if (pagesIndexRevocableMemoryContext.getBytes() == 0) {
            return;
        }

        pageIndex.clear();

        pagesIndexRevocableMemoryContext.setBytes(0L);
        pagesIndexUserMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
    }

    @Override
    public void close()
    {
        pageIndex.clear();
    }

    public WorkProcessor<Page> getSortedPages(PageWithPositionComparator pageWithPositionComparator)
    {
        List<WorkProcessor<Page>> spilledPages = getSpilledPages();

        if (!spillEnabled || spilledPages.isEmpty()) {
            pageIndex.sort(sortChannels, sortOrder);

            return WorkProcessor.fromIterator(pageIndex.getSortedPages());
        }

        return mergeSpilledPagesAndCurrentPagesIndex(pageIndex, spilledPages, pageWithPositionComparator);
    }

    public List<WorkProcessor<Page>> getSpilledPages()
    {
        if (!spiller.isPresent()) {
            return ImmutableList.of();
        }

        return spiller.get().getSpills().stream()
                .map(WorkProcessor::fromIterator)
                .collect(Collectors.toList());
    }

    public WorkProcessor<Page> mergeSpilledPagesAndCurrentPagesIndex(
            PagesIndex currentPagesIndex,
            List<WorkProcessor<Page>> spilledPages,
            PageWithPositionComparator pageWithPositionComparator)
    {
        WorkProcessor<Page> pageIndexPages = WorkProcessor.fromIterator(currentPagesIndex.getSortedPages());

        spilledPages.add(pageIndexPages);

        return mergeSortedPages(
                spilledPages,
                requireNonNull(pageWithPositionComparator, "comparator is null"),
                sourceTypes,
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.getDriverContext().getYieldSignal());
    }

    private void updateMemoryUsage(boolean useRevocableMemory)
    {
        if (useRevocableMemory) {
            if (!spillEnabled) {
                throw new IllegalStateException("Revocable memory requested when spill to disk is not enabled");
            }

            pagesIndexRevocableMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
        }
        else {
            if (!pagesIndexUserMemoryContext.trySetBytes(pageIndex.getEstimatedSize().toBytes())) {
                pageIndex.compact();
                pagesIndexUserMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
            }
        }
    }
}
