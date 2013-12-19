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

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.longs.Long2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;

public class HashAggregationOperator
        implements Operator
{
    public static class HashAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final List<TupleInfo> groupByTupleInfos;
        private final List<Integer> groupByChannels;
        private final Step step;
        private final List<AggregationFunctionDefinition> functionDefinitions;
        private final int expectedGroups;
        private final List<TupleInfo> tupleInfos;
        private boolean closed;

        public HashAggregationOperatorFactory(
                int operatorId,
                List<TupleInfo> groupByTupleInfos,
                List<Integer> groupByChannels,
                Step step,
                List<AggregationFunctionDefinition> functionDefinitions,
                int expectedGroups)
        {
            this.operatorId = operatorId;
            this.groupByTupleInfos = groupByTupleInfos;
            this.groupByChannels = groupByChannels;
            this.step = step;
            this.functionDefinitions = functionDefinitions;
            this.expectedGroups = expectedGroups;

            this.tupleInfos = toTupleInfos(groupByTupleInfos, step, functionDefinitions);
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return tupleInfos;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, HashAggregationOperator.class.getSimpleName());
            return new HashAggregationOperator(
                    operatorContext,
                    groupByTupleInfos,
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
    private final List<TupleInfo> groupByTupleInfos;
    private final List<Integer> groupByChannels;
    private final Step step;
    private final List<AggregationFunctionDefinition> functionDefinitions;
    private final int expectedGroups;

    private final List<TupleInfo> tupleInfos;
    private final HashMemoryManager memoryManager;

    private GroupByHashAggregationBuilder aggregationBuilder;
    private Iterator<Page> outputIterator;
    private boolean finishing;

    public HashAggregationOperator(
            OperatorContext operatorContext,
            List<TupleInfo> groupByTupleInfos,
            List<Integer> groupByChannels,
            Step step,
            List<AggregationFunctionDefinition> functionDefinitions,
            int expectedGroups)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        Preconditions.checkNotNull(step, "step is null");
        Preconditions.checkNotNull(functionDefinitions, "functionDefinitions is null");
        Preconditions.checkNotNull(operatorContext, "operatorContext is null");

        this.groupByTupleInfos = groupByTupleInfos;
        this.groupByChannels = groupByChannels;
        this.functionDefinitions = ImmutableList.copyOf(functionDefinitions);
        this.step = step;
        this.expectedGroups = expectedGroups;
        this.memoryManager = new HashMemoryManager(operatorContext);

        this.tupleInfos = toTupleInfos(groupByTupleInfos, step, functionDefinitions);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
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
                    groupByTupleInfos,
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
            checkState(finishing || step == Step.PARTIAL, "Task exceeded max memory size of %s", memoryManager.getMaxMemorySize());

            outputIterator = aggregationBuilder.build();
            aggregationBuilder = null;

            if (!outputIterator.hasNext()) {
                return null;
            }
        }

        return outputIterator.next();
    }

    private static List<TupleInfo> toTupleInfos(List<TupleInfo> groupByTupleInfo, Step step, List<AggregationFunctionDefinition> functionDefinitions)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        tupleInfos.addAll(groupByTupleInfo);
        for (AggregationFunctionDefinition functionDefinition : functionDefinitions) {
            if (step != Step.PARTIAL) {
                tupleInfos.add(functionDefinition.getFunction().getFinalTupleInfo());
            }
            else {
                tupleInfos.add(functionDefinition.getFunction().getIntermediateTupleInfo());
            }
        }
        return tupleInfos.build();
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
                List<TupleInfo> groupByTupleInfos,
                List<Integer> groupByChannels,
                HashMemoryManager memoryManager)
        {
            List<Type> groupByTypes = ImmutableList.copyOf(transform(groupByTupleInfos, new Function<TupleInfo, Type>()
            {
                public Type apply(TupleInfo tupleInfo)
                {
                    return tupleInfo.getType();
                }
            }));

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
            return memoryManager.canUse(memorySize);
        }

        public Iterator<Page> build()
        {
            List<Type> types = groupByHash.getTypes();
            ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
            for (Type type : types) {
                tupleInfos.add(new TupleInfo(type));
            }
            for (Aggregator aggregator : aggregators) {
                tupleInfos.add(aggregator.getTupleInfo());
            }

            final PageBuilder pageBuilder = new PageBuilder(tupleInfos.build());
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
                return false;
            }

            if (!operatorContext.reserveMemory(delta)) {
                return true;
            }

            // reservation worked, record the reservation
            currentMemoryReservation = Math.max(currentMemoryReservation, memorySize);
            return false;
        }

        public Object getMaxMemorySize()
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
                aggregation = function.createGroupedIntermediateAggregation();
            }
            else {
                int[] argumentChannels = new int[functionDefinition.getInputs().size()];
                for (int i = 0; i < argumentChannels.length; i++) {
                    argumentChannels[i] = functionDefinition.getInputs().get(i).getChannel();
                }
                intermediateChannel = -1;
                Optional<Integer> maskChannel = Optional.absent();
                if (functionDefinition.getMask().isPresent()) {
                    maskChannel = Optional.of(functionDefinition.getMask().get().getChannel());
                }
                aggregation = function.createGroupedAggregation(maskChannel, argumentChannels);
            }
            this.step = step;
        }

        public long getEstimatedSize()
        {
            return aggregation.getEstimatedSize();
        }

        public TupleInfo getTupleInfo()
        {
            if (step == Step.PARTIAL) {
                return aggregation.getIntermediateTupleInfo();
            }
            else {
                return aggregation.getFinalTupleInfo();
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
