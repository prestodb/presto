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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
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

        public OrderByOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                int expectedPositions,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                PagesIndex.Factory pagesIndexFactory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
            this.expectedPositions = expectedPositions;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));

            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
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
                    pagesIndexFactory);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new OrderByOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, expectedPositions, sortChannels, sortOrder, pagesIndexFactory);
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
    private final LocalMemoryContext localUserMemoryContext;

    private final PagesIndex pageIndex;

    private Iterator<Page> sortedPages;

    private State state = State.NEEDS_INPUT;

    public OrderByOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            int expectedPositions,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            PagesIndex.Factory pagesIndexFactory)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(requireNonNull(outputChannels, "outputChannels is null"));
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();

        this.pageIndex = pagesIndexFactory.newPagesIndex(sourceTypes, expectedPositions);
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

            pageIndex.sort(sortChannels, sortOrder);
            WorkProcessor<Page> resultPages = WorkProcessor.fromIterator(pageIndex.getSortedPages());
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
        updateMemoryUsage();
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        verify(sortedPages != null, "sortedPages is null");
        if (!sortedPages.hasNext()) {
            state = State.FINISHED;
            return null;
        }

        Page nextPage = sortedPages.next();
        Block[] blocks = new Block[outputChannels.length];
        for (int i = 0; i < outputChannels.length; i++) {
            blocks[i] = nextPage.getBlock(outputChannels[i]);
        }
        return new Page(nextPage.getPositionCount(), blocks);
    }

    private void updateMemoryUsage()
    {
        if (!localUserMemoryContext.trySetBytes(pageIndex.getEstimatedSize().toBytes())) {
            pageIndex.compact();
            localUserMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
        }
    }

    @Override
    public void close()
    {
        pageIndex.clear();
        sortedPages = null;
    }
}
