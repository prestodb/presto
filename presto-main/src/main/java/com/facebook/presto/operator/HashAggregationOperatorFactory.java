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
import com.facebook.presto.operator.simple.ClusterBoundaryCallback;
import com.facebook.presto.operator.simple.OperatorBuilder;
import com.facebook.presto.operator.simple.ProcessorBase;
import com.facebook.presto.operator.simple.ProcessorInput;
import com.facebook.presto.operator.simple.ProcessorPageBuilderOutput;
import com.facebook.presto.operator.simple.SimpleOperator.ProcessorState;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.PageCompiler;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.operator.simple.SimpleOperator.ProcessorState.HAS_OUTPUT;
import static com.facebook.presto.operator.simple.SimpleOperator.ProcessorState.NEEDS_INPUT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class HashAggregationOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final Optional<Integer> maskChannel;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final List<Integer> preGroupedChannels;
    private final Step step;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Optional<Integer> hashChannel;

    private final int expectedGroups;
    private final List<Type> types;
    private boolean closed;
    private final long maxPartialMemory;
    private final DataSize minClusterCallbackSize;

    public HashAggregationOperatorFactory(
            int operatorId,
            List<? extends Type> groupByTypes,
            List<Integer> groupByChannels,
            List<Integer> preGroupedChannels,
            Step step,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> maskChannel,
            Optional<Integer> hashChannel,
            int expectedGroups,
            DataSize maxPartialMemory)
    {
        this(operatorId,
                groupByTypes,
                groupByChannels,
                preGroupedChannels,
                step,
                accumulatorFactories,
                maskChannel,
                hashChannel,
                expectedGroups,
                maxPartialMemory,
                new DataSize(PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES, DataSize.Unit.BYTE));
    }

    public HashAggregationOperatorFactory(
            int operatorId,
            List<? extends Type> groupByTypes,
            List<Integer> groupByChannels,
            List<Integer> preGroupedChannels,
            Step step,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> maskChannel,
            Optional<Integer> hashChannel,
            int expectedGroups,
            DataSize maxPartialMemory,
            DataSize minClusterCallbackSize)
    {
        this.operatorId = operatorId;
        this.maskChannel = checkNotNull(maskChannel, "maskChannel is null");
        this.hashChannel = checkNotNull(hashChannel, "hashChannel is null");
        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.preGroupedChannels = ImmutableList.copyOf(checkNotNull(preGroupedChannels, "preGroupedChannels is null"));
        this.step = step;
        this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        this.expectedGroups = expectedGroups;
        this.maxPartialMemory = checkNotNull(maxPartialMemory, "maxPartialMemory is null").toBytes();
        this.minClusterCallbackSize = checkNotNull(minClusterCallbackSize, "minClusterCallbackSize is null");

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
        if (step == Step.PARTIAL) {
            operatorContext = driverContext.addOperatorContext(operatorId, HashAggregationProcessor.class.getSimpleName(), maxPartialMemory);
        }
        else {
            operatorContext = driverContext.addOperatorContext(operatorId, HashAggregationProcessor.class.getSimpleName());
        }
        HashAggregationProcessor processor = new HashAggregationProcessor(
                operatorContext,
                groupByTypes,
                groupByChannels,
                step,
                accumulatorFactories,
                maskChannel,
                hashChannel,
                expectedGroups);

        OperatorBuilder operatorBuilder = OperatorBuilder.newOperatorBuilder(processor)
                .bindInput(processor)
                .bindOutput(processor);
        if (step == Step.PARTIAL) {
            return operatorBuilder.build();
        }
        else {
            PagePositionEqualitor equalitor = PageCompiler.INSTANCE.compilePagePositionEqualitor(Lists.transform(preGroupedChannels, types::get), preGroupedChannels);
            return operatorBuilder
                    .withClusterBoundaryCallback(equalitor, minClusterCallbackSize, processor)
                    .build();
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private static List<Type> toTypes(List<? extends Type> groupByType, Step step, List<AccumulatorFactory> factories, Optional<Integer> hashChannel)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        types.addAll(groupByType);
        if (hashChannel.isPresent()) {
            types.add(BIGINT);
        }
        for (AccumulatorFactory factory : factories) {
            types.add(new HashAggregationProcessor.Aggregator(factory, step).getType());
        }
        return types.build();
    }

    private static class HashAggregationProcessor
            implements ProcessorBase, ProcessorInput<Page>, ProcessorPageBuilderOutput, ClusterBoundaryCallback
    {
        private final OperatorContext operatorContext;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final Optional<Integer> maskChannel;
        private final Optional<Integer> hashChannel;
        private final int expectedGroups;

        private final List<Type> types;

        private GroupByHashAggregationBuilder aggregationBuilder;
        private GroupByHashAggregationBuilder.Drainer drainer;

        public HashAggregationProcessor(
                OperatorContext operatorContext,
                List<Type> groupByTypes,
                List<Integer> groupByChannels,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> maskChannel,
                Optional<Integer> hashChannel,
                int expectedGroups)
        {
            this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
            checkNotNull(step, "step is null");
            checkNotNull(accumulatorFactories, "accumulatorFactories is null");
            checkNotNull(operatorContext, "operatorContext is null");

            this.groupByTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = ImmutableList.copyOf(groupByChannels);
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.maskChannel = checkNotNull(maskChannel, "maskChannel is null");
            this.hashChannel = checkNotNull(hashChannel, "hashChannel is null");
            this.step = step;
            this.expectedGroups = expectedGroups;
            this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
        }

        @Override
        public String getName()
        {
            return HashAggregationProcessor.class.getSimpleName();
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
        public ProcessorState addInput(@Nullable Page page)
        {
            if (page == null) {
                if (aggregationBuilder == null) {
                    return NEEDS_INPUT;
                }
                else {
                    prepareForOutput();
                    return HAS_OUTPUT;
                }
            }
            if (aggregationBuilder == null) {
                aggregationBuilder = new GroupByHashAggregationBuilder(
                        accumulatorFactories,
                        step,
                        expectedGroups,
                        groupByTypes,
                        groupByChannels,
                        maskChannel,
                        hashChannel,
                        operatorContext);

                // assume initial aggregationBuilder is not full
            }
            else {
                checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
            }
            aggregationBuilder.processPage(page);
            if (aggregationBuilder.isFull()) {
                prepareForOutput();
                return HAS_OUTPUT;
            }
            else {
                return NEEDS_INPUT;
            }
        }

        private void prepareForOutput()
        {
            drainer = aggregationBuilder.build();
            aggregationBuilder = null;
        }

        @Override
        public ProcessorState appendOutputTo(PageBuilder pageBuilder)
        {
            checkState(drainer != null);
            return drainer.drainTo(pageBuilder);
        }

        @Override
        public ProcessorState clusterBoundary()
        {
            if (aggregationBuilder == null) {
                return NEEDS_INPUT;
            }
            prepareForOutput();
            return HAS_OUTPUT;
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
                    Optional<Integer> maskChannel,
                    Optional<Integer> hashChannel,
                    OperatorContext operatorContext)
            {
                this.groupByHash = createGroupByHash(groupByTypes, Ints.toArray(groupByChannels), maskChannel, hashChannel, expectedGroups);
                this.operatorContext = operatorContext;
                this.partial = (step == Step.PARTIAL);

                // wrapper each function with an aggregator
                ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
                checkNotNull(accumulatorFactories, "accumulatorFactories is null");
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

            public Drainer build()
            {
                return new Drainer();
            }

            public class Drainer
            {
                private final int groupCount = groupByHash.getGroupCount();
                private int groupId;

                public ProcessorState drainTo(PageBuilder pageBuilder)
                {
                    if (groupId >= groupCount) {
                        return NEEDS_INPUT;
                    }

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

                    return HAS_OUTPUT;
                }
            }
        }

        private static class Aggregator
        {
            private final GroupedAccumulator aggregation;
            private final Step step;
            private final int intermediateChannel;

            private Aggregator(AccumulatorFactory accumulatorFactory, Step step)
            {
                if (step == Step.FINAL) {
                    checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
                    intermediateChannel = accumulatorFactory.getInputChannels().get(0);
                    aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
                }
                else {
                    intermediateChannel = -1;
                    aggregation = accumulatorFactory.createGroupedAccumulator();
                }
                this.step = step;
            }

            public long getEstimatedSize()
            {
                return aggregation.getEstimatedSize();
            }

            public Type getType()
            {
                if (step == Step.PARTIAL) {
                    return aggregation.getIntermediateType();
                }
                else {
                    return aggregation.getFinalType();
                }
            }

            public void processPage(GroupByIdBlock groupIds, Page page)
            {
                if (step == Step.FINAL) {
                    aggregation.addIntermediate(groupIds, page.getBlock(intermediateChannel));
                }
                else {
                    aggregation.addInput(groupIds, page);
                }
            }

            public void evaluate(int groupId, BlockBuilder output)
            {
                if (step == Step.PARTIAL) {
                    aggregation.evaluateIntermediate(groupId, output);
                }
                else {
                    aggregation.evaluateFinal(groupId, output);
                }
            }
        }
    }
}
