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
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class StreamingAggregationOperator
        implements Operator
{
    public static class StreamingAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final JoinCompiler joinCompiler;
        private boolean closed;

        public StreamingAggregationOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes, List<Type> groupByTypes, List<Integer> groupByChannels, Step step, List<AccumulatorFactory> accumulatorFactories, JoinCompiler joinCompiler)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.groupByTypes = ImmutableList.copyOf(requireNonNull(groupByTypes, "groupByTypes is null"));
            this.groupByChannels = ImmutableList.copyOf(requireNonNull(groupByChannels, "groupByChannels is null"));
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(requireNonNull(accumulatorFactories, "accumulatorFactories is null"));
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, StreamingAggregationOperator.class.getSimpleName());
            return new StreamingAggregationOperator(operatorContext, sourceTypes, groupByTypes, groupByChannels, step, accumulatorFactories, joinCompiler);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new StreamingAggregationOperatorFactory(operatorId, planNodeId, sourceTypes, groupByTypes, groupByChannels, step, accumulatorFactories, joinCompiler);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final LocalMemoryContext userMemoryContext;
    private final List<Type> groupByTypes;
    private final int[] groupByChannels;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Step step;
    private final PagesHashStrategy pagesHashStrategy;

    private List<Aggregator> aggregates;
    private final PageBuilder pageBuilder;
    private final Deque<Page> outputPages = new LinkedList<>();
    private Page currentGroup;
    private boolean finishing;

    public StreamingAggregationOperator(OperatorContext operatorContext, List<Type> sourceTypes, List<Type> groupByTypes, List<Integer> groupByChannels, Step step, List<AccumulatorFactory> accumulatorFactories, JoinCompiler joinCompiler)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(StreamingAggregationOperator.class.getSimpleName());
        this.userMemoryContext = operatorContext.localUserMemoryContext();
        this.groupByTypes = ImmutableList.copyOf(requireNonNull(groupByTypes, "groupByTypes is null"));
        this.groupByChannels = Ints.toArray(requireNonNull(groupByChannels, "groupByChannels is null"));
        this.accumulatorFactories = requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        this.step = requireNonNull(step, "step is null");

        this.aggregates = setupAggregates(step, accumulatorFactories);
        this.pageBuilder = new PageBuilder(toTypes(groupByTypes, aggregates));
        requireNonNull(joinCompiler, "joinCompiler is null");

        requireNonNull(sourceTypes, "sourceTypes is null");
        pagesHashStrategy = joinCompiler.compilePagesHashStrategyFactory(sourceTypes, groupByChannels, Optional.empty())
                .createPagesHashStrategy(
                        sourceTypes.stream()
                                .map(type -> ImmutableList.<Block>of())
                                .collect(toImmutableList()), OptionalInt.empty());
    }

    private List<Aggregator> setupAggregates(Step step, List<AccumulatorFactory> accumulatorFactories)
    {
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        for (AccumulatorFactory factory : accumulatorFactories) {
            builder.add(new Aggregator(factory, step, this::updateMemoryUsage));
        }
        return builder.build();
    }

    private static List<Type> toTypes(List<Type> groupByTypes, List<Aggregator> aggregates)
    {
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        builder.addAll(groupByTypes);
        aggregates.stream()
                .map(Aggregator::getType)
                .forEach(builder::add);
        return builder.build();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && outputPages.isEmpty();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");

        processInput(page);
        updateMemoryUsage();
    }

    private boolean updateMemoryUsage()
    {
        long memorySize = pageBuilder.getRetainedSizeInBytes();
        for (Page output : outputPages) {
            memorySize += output.getRetainedSizeInBytes();
        }
        for (Aggregator aggregator : aggregates) {
            memorySize += aggregator.getEstimatedSize();
        }

        if (currentGroup != null) {
            memorySize += currentGroup.getRetainedSizeInBytes();
        }

        if (step.isOutputPartial()) {
            systemMemoryContext.setBytes(memorySize);
        }
        else {
            userMemoryContext.setBytes(memorySize);
        }
        // If memory is not available, inform the caller that we cannot proceed for allocation.
        return operatorContext.isWaitingForMemory().isDone();
    }

    private void processInput(Page page)
    {
        requireNonNull(page, "page is null");

        Page groupByPage = page.extractChannels(groupByChannels);
        if (currentGroup != null) {
            if (!pagesHashStrategy.rowEqualsRow(0, currentGroup.extractChannels(groupByChannels), 0, groupByPage)) {
                // page starts with new group, so flush it
                evaluateAndFlushGroup(currentGroup, 0);
            }
            currentGroup = null;
        }

        int startPosition = 0;
        while (true) {
            // may be equal to page.getPositionCount() if the end is not found in this page
            int nextGroupStart = findNextGroupStart(startPosition, groupByPage);
            addRowsToAggregates(page, startPosition, nextGroupStart - 1);

            if (nextGroupStart < page.getPositionCount()) {
                // current group stops somewhere in the middle of the page, so flush it
                evaluateAndFlushGroup(page, startPosition);
                startPosition = nextGroupStart;
            }
            else {
                currentGroup = page.getRegion(page.getPositionCount() - 1, 1);
                return;
            }
        }
    }

    private void addRowsToAggregates(Page page, int startPosition, int endPosition)
    {
        Page region = page.getRegion(startPosition, endPosition - startPosition + 1);
        for (Aggregator aggregator : aggregates) {
            aggregator.processPage(region);
        }
    }

    private void evaluateAndFlushGroup(Page page, int position)
    {
        pageBuilder.declarePosition();
        for (int i = 0; i < groupByTypes.size(); i++) {
            Block block = page.getBlock(groupByChannels[i]);
            Type type = groupByTypes.get(i);
            type.appendTo(block, position, pageBuilder.getBlockBuilder(i));
        }
        int offset = groupByTypes.size();
        for (int i = 0; i < aggregates.size(); i++) {
            aggregates.get(i).evaluate(pageBuilder.getBlockBuilder(offset + i));
        }

        if (pageBuilder.isFull()) {
            outputPages.add(pageBuilder.build());
            pageBuilder.reset();
        }

        aggregates = setupAggregates(step, accumulatorFactories);
    }

    private int findNextGroupStart(int startPosition, Page page)
    {
        for (int i = startPosition + 1; i < page.getPositionCount(); i++) {
            if (!pagesHashStrategy.rowEqualsRow(startPosition, page, i, page)) {
                return i;
            }
        }

        return page.getPositionCount();
    }

    @Override
    public Page getOutput()
    {
        if (!outputPages.isEmpty()) {
            return outputPages.removeFirst();
        }

        return null;
    }

    @Override
    public void finish()
    {
        finishing = true;

        if (currentGroup != null) {
            evaluateAndFlushGroup(currentGroup, 0);
            currentGroup = null;
        }

        if (!pageBuilder.isEmpty()) {
            outputPages.add(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    @Override
    public boolean isFinished()
    {
        return finishing && outputPages.isEmpty() && currentGroup == null && pageBuilder.isEmpty();
    }
}
