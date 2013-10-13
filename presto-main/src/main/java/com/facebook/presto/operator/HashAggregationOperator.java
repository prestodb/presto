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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.type.Type;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.longs.Long2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class HashAggregationOperator
        implements Operator
{
    public static class HashAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final Step step;
        private final List<AggregationFunctionDefinition> functionDefinitions;
        private final int expectedGroups;
        private final List<Type> types;
        private boolean closed;

        public HashAggregationOperatorFactory(
                int operatorId,
                List<Type> groupByTypes,
                List<Integer> groupByChannels,
                Step step,
                List<AggregationFunctionDefinition> functionDefinitions,
                int expectedGroups)
        {
            this.operatorId = operatorId;
            this.groupByTypes = groupByTypes;
            this.groupByChannels = groupByChannels;
            this.step = step;
            this.functionDefinitions = functionDefinitions;
            this.expectedGroups = expectedGroups;

            this.types = toTypes(groupByTypes, step, functionDefinitions);
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

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, HashAggregationOperator.class.getSimpleName());
            return new HashAggregationOperator(
                    operatorContext,
                    groupByTypes,
                    groupByChannels,
                    step,
                    functionDefinitions,
                    expectedGroups
            );
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final Step step;
    private final List<AggregationFunctionDefinition> functionDefinitions;
    private final int expectedGroups;

    private final List<Type> types;
    private final HashMemoryManager memoryManager;

    private GroupByHashAggregationBuilder aggregationBuilder;
    private Iterator<Page> outputIterator;
    private boolean finishing;

    public HashAggregationOperator(
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Step step,
            List<AggregationFunctionDefinition> functionDefinitions,
            int expectedGroups)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        checkNotNull(step, "step is null");
        checkNotNull(functionDefinitions, "functionDefinitions is null");
        checkNotNull(operatorContext, "operatorContext is null");

        this.groupByTypes = groupByTypes;
        this.groupByChannels = groupByChannels;
        this.functionDefinitions = ImmutableList.copyOf(functionDefinitions);
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.memoryManager = new HashMemoryManager(operatorContext);

        this.types = toTypes(groupByTypes, step, functionDefinitions);
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
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
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
        checkNotNull(page, "page is null");
        if (aggregationBuilder == null) {
            aggregationBuilder = new GroupByHashAggregationBuilder(
                    functionDefinitions,
                    step,
                    expectedGroups,
                    groupByTypes,
                    groupByChannels,
                    memoryManager);

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
            // no data
            if (aggregationBuilder == null) {
                return null;
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && !aggregationBuilder.isFull()) {
                return null;
            }

            // Only partial aggregation can flush early. Also, check that we are not flushing tiny bits at a time
            if (!finishing && step != Step.PARTIAL) {
                throw new ExceededMemoryLimitException(memoryManager.getMaxMemorySize());
            }

            outputIterator = aggregationBuilder.build();
            aggregationBuilder = null;

            if (!outputIterator.hasNext()) {
                return null;
            }
        }

        return outputIterator.next();
    }

    private static List<Type> toTypes(List<Type> groupByType, Step step, List<AggregationFunctionDefinition> functionDefinitions)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        types.addAll(groupByType);
        for (AggregationFunctionDefinition functionDefinition : functionDefinitions) {
            if (step != Step.PARTIAL) {
                types.add(functionDefinition.getFunction().getFinalType());
            }
            else {
                types.add(functionDefinition.getFunction().getIntermediateType());
            }
        }
        return types.build();
    }

    private static class GroupByHashAggregationBuilder
    {
        private final GroupByHash groupByHash;
        private final List<Aggregator> aggregators;
        private final HashMemoryManager memoryManager;

        private GroupByHashAggregationBuilder(
                List<AggregationFunctionDefinition> functionDefinitions,
                Step step,
                int expectedGroups,
                List<Type> groupByTypes,
                List<Integer> groupByChannels,
                HashMemoryManager memoryManager)
        {
            this.groupByHash = new GroupByHash(groupByTypes, Ints.toArray(groupByChannels), expectedGroups);
            this.memoryManager = memoryManager;

            // wrapper each function with an aggregator
            ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
            for (AggregationFunctionDefinition functionDefinition : checkNotNull(functionDefinitions, "functionDefinitions is null")) {
                builder.add(new Aggregator(functionDefinition, step));
            }
            aggregators = builder.build();
        }

        private void processPage(Page page)
        {
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
            return !memoryManager.canUse(memorySize);
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
                private final ObjectIterator<Entry> pagePositionToGroup = groupByHash.getPagePositionToGroupId().long2IntEntrySet().fastIterator();

                @Override
                protected Page computeNext()
                {
                    if (!pagePositionToGroup.hasNext()) {
                        return endOfData();
                    }

                    pageBuilder.reset();

                    List<Type> types = groupByHash.getTypes();
                    BlockBuilder[] groupByBlockBuilders = new BlockBuilder[types.size()];
                    for (int i = 0; i < types.size(); i++) {
                        groupByBlockBuilders[i] = pageBuilder.getBlockBuilder(i);
                    }

                    while (!pageBuilder.isFull() && pagePositionToGroup.hasNext()) {
                        Entry next = pagePositionToGroup.next();
                        long pagePosition = next.getLongKey();
                        int groupId = next.getIntValue();

                        groupByHash.appendValuesTo(pagePosition, groupByBlockBuilders);

                        for (int i = 0; i < aggregators.size(); i++) {
                            Aggregator aggregator = aggregators.get(i);
                            BlockBuilder output = pageBuilder.getBlockBuilder(types.size() + i);
                            aggregator.evaluate(groupId, output);
                        }
                    }

                    Page page = pageBuilder.build();
                    return page;
                }
            };
        }
    }

    public static class HashMemoryManager
    {
        private final OperatorContext operatorContext;
        private long currentMemoryReservation;

        public HashMemoryManager(OperatorContext operatorContext)
        {
            this.operatorContext = operatorContext;
        }

        public boolean canUse(long memorySize)
        {
            // remove the pre-allocated memory from this size
            memorySize -= operatorContext.getOperatorPreAllocatedMemory().toBytes();

            long delta = memorySize - currentMemoryReservation;
            if (delta <= 0) {
                return true;
            }

            if (!operatorContext.reserveMemory(delta)) {
                return false;
            }

            // reservation worked, record the reservation
            currentMemoryReservation = Math.max(currentMemoryReservation, memorySize);
            return true;
        }

        public DataSize getMaxMemorySize()
        {
            return operatorContext.getMaxMemorySize();
        }
    }

    private static class Aggregator
    {
        private final GroupedAccumulator aggregation;
        private final Step step;

        private final int intermediateChannel;

        private Aggregator(AggregationFunctionDefinition functionDefinition, Step step)
        {
            AggregationFunction function = functionDefinition.getFunction();

            if (step == Step.FINAL) {
                checkArgument(functionDefinition.getInputs().size() == 1, "Expected a single input for an intermediate aggregation");
                intermediateChannel = functionDefinition.getInputs().get(0).getChannel();
                aggregation = function.createGroupedIntermediateAggregation(functionDefinition.getConfidence());
            }
            else {
                int[] argumentChannels = new int[functionDefinition.getInputs().size()];
                for (int i = 0; i < argumentChannels.length; i++) {
                    argumentChannels[i] = functionDefinition.getInputs().get(i).getChannel();
                }
                intermediateChannel = -1;
                aggregation = function.createGroupedAggregation(
                        functionDefinition.getMask().transform(Input.channelGetter()),
                        functionDefinition.getSampleWeight().transform(Input.channelGetter()),
                        functionDefinition.getConfidence(),
                        argumentChannels);
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
