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

import com.facebook.presto.operator.aggregation.Accumulator;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.builder.HashAggregationBuilder;
import com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder.toTypes;
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
        private final List<Integer> globalAggregationGroupIds;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final Optional<Integer> hashChannel;
        private final Optional<Integer> groupIdChannel;

        private final int expectedGroups;
        private final List<Type> types;
        private final DataSize maxPartialMemory;

        private boolean closed;

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                DataSize maxPartialMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
            this.groupByTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = ImmutableList.copyOf(groupByChannels);
            this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.expectedGroups = expectedGroups;
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");

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

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName());
            HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
                    operatorContext,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory);
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
                    globalAggregationGroupIds,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final List<Integer> globalAggregationGroupIds;
    private final Step step;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Optional<Integer> hashChannel;
    private final Optional<Integer> groupIdChannel;
    private final int expectedGroups;
    private final DataSize maxPartialMemory;

    private final List<Type> types;

    private HashAggregationBuilder aggregationBuilder;
    private Iterator<Page> outputIterator;
    private boolean inputProcessed;
    private boolean finishing;
    private boolean finished;

    public HashAggregationOperator(
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            List<Integer> globalAggregationGroupIds,
            Step step,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> hashChannel,
            Optional<Integer> groupIdChannel,
            int expectedGroups,
            DataSize maxPartialMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(step, "step is null");
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        requireNonNull(operatorContext, "operatorContext is null");

        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
        this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
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
        return finished;
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
        inputProcessed = true;

        if (aggregationBuilder == null) {
            aggregationBuilder = new InMemoryHashAggregationBuilder(
                    accumulatorFactories,
                    step,
                    expectedGroups,
                    groupByTypes,
                    groupByChannels,
                    hashChannel,
                    operatorContext,
                    maxPartialMemory);

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
        if (finished) {
            return null;
        }

        if (outputIterator == null) {
            // current output iterator is done
            outputIterator = null;

            if (finishing) {
                if (!inputProcessed && !step.isOutputPartial()) {
                    // global aggregations always generate an output row with the default aggregation output (e.g. 0 for COUNT, NULL for SUM)
                    finished = true;
                    return getGlobalAggregationOutput();
                }

                if (aggregationBuilder == null) {
                    finished = true;
                    return null;
                }
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && (aggregationBuilder == null || !aggregationBuilder.isFull())) {
                return null;
            }

            outputIterator = aggregationBuilder.buildResult();

            if (!outputIterator.hasNext()) {
                // current output iterator is done
                closeAggregationBuilder();
                return null;
            }
        }

        Page output = outputIterator.next();
        if (!outputIterator.hasNext()) {
            closeAggregationBuilder();
        }
        return output;
    }

    @Override
    public void close()
    {
        closeAggregationBuilder();
    }

    private void closeAggregationBuilder()
    {
        outputIterator = null;
        if (aggregationBuilder != null) {
            aggregationBuilder.close();
            aggregationBuilder = null;
        }
    }

    private Page getGlobalAggregationOutput()
    {
        List<Accumulator> accumulators = accumulatorFactories.stream()
                .map(AccumulatorFactory::createAccumulator)
                .collect(Collectors.toList());

        PageBuilder output = new PageBuilder(types);

        for (int groupId : globalAggregationGroupIds) {
            output.declarePosition();
            int channel = 0;

            for (; channel < groupByTypes.size(); channel++) {
                if (channel == groupIdChannel.get()) {
                    output.getBlockBuilder(channel).writeLong(groupId);
                }
                else {
                    output.getBlockBuilder(channel).appendNull();
                }
            }

            if (hashChannel.isPresent()) {
                output.getBlockBuilder(channel++).writeLong(TypeUtils.NULL_HASH_CODE);
            }

            for (int j = 0; j < accumulators.size(); channel++, j++) {
                accumulators.get(j).evaluateFinal(output.getBlockBuilder(channel));
            }
        }

        if (output.isEmpty()) {
            return null;
        }
        return output.build();
    }
}
