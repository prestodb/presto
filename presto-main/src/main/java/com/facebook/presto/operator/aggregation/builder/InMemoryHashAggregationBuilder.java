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
package com.facebook.presto.operator.aggregation.builder;

import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.operator.GroupByIdBlock;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class InMemoryHashAggregationBuilder
        implements HashAggregationBuilder
{
    private final GroupByHash groupByHash;
    private final List<Aggregator> aggregators;
    private final OperatorContext operatorContext;
    private final boolean partial;

    private boolean full;

    public InMemoryHashAggregationBuilder(
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

    @Override
    public void close()
    {
    }

    @Override
    public void processPage(Page page)
    {
        if (aggregators.isEmpty()) {
            groupByHash.addPage(page);
            return;
        }

        GroupByIdBlock groupIds = groupByHash.getGroupIds(page);

        for (Aggregator aggregator : aggregators) {
            aggregator.processPage(groupIds, page);
        }
        updateMemory();
    }

    private void updateMemory()
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
            full = !operatorContext.trySetMemoryReservation(memorySize);
        }
        else {
            operatorContext.setMemoryReservation(memorySize);
        }
    }

    @Override
    public boolean isFull()
    {
        return full;
    }

    @Override
    public Iterator<Page> buildResult()
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

    private static class Aggregator
    {
        private final GroupedAccumulator aggregation;
        private final Step step;
        private final int intermediateChannel;

        private Aggregator(AccumulatorFactory accumulatorFactory, Step step)
        {
            if (step.isInputRaw()) {
                this.intermediateChannel = -1;
                this.aggregation = accumulatorFactory.createGroupedAccumulator();
            }
            else {
                checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
                this.intermediateChannel = accumulatorFactory.getInputChannels().get(0);
                this.aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
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

    public static List<Type> toTypes(List<? extends Type> groupByType, Step step, List<AccumulatorFactory> factories, Optional<Integer> hashChannel)
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
}
