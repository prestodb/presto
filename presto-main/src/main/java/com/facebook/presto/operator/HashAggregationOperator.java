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

import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HashAggregationOperator
        implements Operator
{
    public static class HashAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final Optional<Integer> hashChannel;

        private final int expectedGroups;
        private final List<Type> types;
        private boolean closed;
        private final long maxPartialMemory;

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                int expectedGroups,
                DataSize maxPartialMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.groupByTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = ImmutableList.copyOf(groupByChannels);
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.expectedGroups = expectedGroups;
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null").toBytes();

            this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
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

            OperatorContext operatorContext;
            if (step.isOutputPartial()) {
                operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName(), maxPartialMemory);
            }
            else {
                operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName());
            }
            HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
                    operatorContext,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups);
            return hashAggregationOperator;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashAggregationOperatorFactory(
                    operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    new DataSize(maxPartialMemory, Unit.BYTE));
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final Step step;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Optional<Integer> hashChannel;
    private final int expectedGroups;

    private final List<Type> types;

    private GroupByHashAggregationBuilder aggregationBuilder;
    private Iterator<Page> outputIterator;
    private boolean finishing;

    public HashAggregationOperator(
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Step step,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> hashChannel,
            int expectedGroups)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(step, "step is null");
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        requireNonNull(operatorContext, "operatorContext is null");

        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
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
        return finishing && aggregationBuilder == null && (outputIterator == null || !outputIterator.hasNext());
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && outputIterator == null && (aggregationBuilder == null || !aggregationBuilder.isFull());
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        if (aggregationBuilder == null) {
            aggregationBuilder = new GroupByHashAggregationBuilder(
                    accumulatorFactories,
                    step,
                    expectedGroups,
                    groupByTypes,
                    groupByChannels,
                    hashChannel,
                    operatorContext);

            // assume initial aggregationBuilder is not full
        }
        else {
            checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
        }
        aggregationBuilder.processPage(page);
    }

    @Override
    public Page getOutput()
    {
        if (outputIterator == null || !outputIterator.hasNext()) {
            // current output iterator is done
            outputIterator = null;

            // no data
            if (aggregationBuilder == null) {
                return null;
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && !aggregationBuilder.isFull()) {
                return null;
            }

            outputIterator = aggregationBuilder.build();
            aggregationBuilder = null;

            if (!outputIterator.hasNext()) {
                // current output iterator is done
                outputIterator = null;
                return null;
            }
        }

        return outputIterator.next();
    }

    private static List<Type> toTypes(List<? extends Type> groupByType, Step step, List<AccumulatorFactory> factories, Optional<Integer> hashChannel)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        types.addAll(groupByType);
        if (hashChannel.isPresent()) {
            types.add(BIGINT);
        }
        for (AccumulatorFactory factory : factories) {
            types.add(new Aggregator(factory, step).getType());
        }
        return types.build();
    }

    private static class GroupByHashAggregationBuilder
    {
        private final GroupByHash groupByHash;
        private final List<Aggregator> aggregators;
        private final OperatorContext operatorContext;
        private final boolean partial;

        private GroupByHashAggregationBuilder(
                List<AccumulatorFactory> accumulatorFactories,
                Step step,
                int expectedGroups,
                List<Type> groupByTypes,
                List<Integer> groupByChannels,
                Optional<Integer> hashChannel,
                OperatorContext operatorContext)
        {
            this.groupByHash = createGroupByHash(operatorContext.getSession(), groupByTypes, Ints.toArray(groupByChannels), hashChannel, expectedGroups);
            this.operatorContext = operatorContext;
            this.partial = step.isOutputPartial();

            // wrapper each function with an aggregator
            ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
            requireNonNull(accumulatorFactories, "accumulatorFactories is null");
            for (int i = 0; i < accumulatorFactories.size(); i++) {
                AccumulatorFactory accumulatorFactory = accumulatorFactories.get(i);
                builder.add(new Aggregator(accumulatorFactory, step));
            }
            aggregators = builder.build();
        }

        private void processPage(Page page)
        {
            if (aggregators.isEmpty()) {
                groupByHash.addPage(page);
                return;
            }

            GroupByIdBlock groupIds = groupByHash.getGroupIds(page);

            for (Aggregator aggregator : aggregators) {
                aggregator.processPage(groupIds, page);
            }
        }

        public boolean isFull()
        {
            long memorySize = groupByHash.getEstimatedSize();
            for (Aggregator aggregator : aggregators) {
                memorySize += aggregator.getEstimatedSize();
            }
            memorySize -= operatorContext.getOperatorPreAllocatedMemory().toBytes();
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

        public Iterator<Page> build()
        {
            List<Type> types = new ArrayList<>(groupByHash.getTypes());
            for (Aggregator aggregator : aggregators) {
                types.add(aggregator.getType());
            }

            final PageBuilder pageBuilder = new PageBuilder(types);
            return new AbstractIterator<Page>()
            {
                private final int groupCount = groupByHash.getGroupCount();
                private int groupId;

                @Override
                protected Page computeNext()
                {
                    if (groupId >= groupCount) {
                        return endOfData();
                    }

                    pageBuilder.reset();

                    List<Type> types = groupByHash.getTypes();
                    while (!pageBuilder.isFull() && groupId < groupCount) {
                        groupByHash.appendValuesTo(groupId, pageBuilder, 0);

                        pageBuilder.declarePosition();
                        for (int i = 0; i < aggregators.size(); i++) {
                            Aggregator aggregator = aggregators.get(i);
                            BlockBuilder output = pageBuilder.getBlockBuilder(types.size() + i);
                            aggregator.evaluate(groupId, output);
                        }

                        groupId++;
                    }

                    return pageBuilder.build();
                }
            };
        }
    }

    private static class Aggregator
    {
        private final GroupedAccumulator aggregation;
        private final Step step;
        private final int intermediateChannel;

        private Aggregator(AccumulatorFactory accumulatorFactory, Step step)
        {
            if (step.isInputRaw()) {
                intermediateChannel = -1;
                aggregation = accumulatorFactory.createGroupedAccumulator();
            }
            else {
                checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
                intermediateChannel = accumulatorFactory.getInputChannels().get(0);
                aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
            }
            this.step = step;
        }

        public long getEstimatedSize()
        {
            return aggregation.getEstimatedSize();
        }

        public Type getType()
        {
            if (step.isOutputPartial()) {
                return aggregation.getIntermediateType();
            }
            else {
                return aggregation.getFinalType();
            }
        }

        public void processPage(GroupByIdBlock groupIds, Page page)
        {
            if (step.isInputRaw()) {
                aggregation.addInput(groupIds, page);
            }
            else {
                aggregation.addIntermediate(groupIds, page.getBlock(intermediateChannel));
            }
        }

        public void evaluate(int groupId, BlockBuilder output)
        {
            if (step.isOutputPartial()) {
                aggregation.evaluateIntermediate(groupId, output);
            }
            else {
                aggregation.evaluateFinal(groupId, output);
            }
        }
    }
}
