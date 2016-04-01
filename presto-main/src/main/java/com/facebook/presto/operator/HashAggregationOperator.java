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

import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.GroupedAccumulator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.spiller.Spiller;
import com.facebook.presto.spi.spiller.SpillerFactory;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
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
        private final long maxPartialMemory;
        private final long maxEntriesBeforeSpill;
        private final DataSize memoryLimitBeforeSpill;
        private final Optional<SpillerFactory> spillerFactory;

        private boolean closed;

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
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    step,
                    accumulatorFactories,
                    hashChannel,
                    expectedGroups,
                    maxPartialMemory,
                    0,
                    new DataSize(0, MEGABYTE),
                    Optional.empty());
        }

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                int expectedGroups,
                DataSize maxPartialMemory,
                long maxEntriesBeforeSpill,
                DataSize memoryLimitBeforeSpill,
                Optional<SpillerFactory> spillerFactory)
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
            this.maxEntriesBeforeSpill = maxEntriesBeforeSpill;
            this.memoryLimitBeforeSpill = requireNonNull(memoryLimitBeforeSpill, "memoryLimitBeforeSpill is null");
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");

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
                    expectedGroups,
                    maxEntriesBeforeSpill,
                    memoryLimitBeforeSpill,
                    spillerFactory);
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
                    new DataSize(maxPartialMemory, Unit.BYTE),
                    maxEntriesBeforeSpill,
                    memoryLimitBeforeSpill,
                    spillerFactory);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final Step step;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Optional<Integer> hashChannel;
    private final int expectedGroups;
    private final Optional<SpillerFactory> spillerFactory;
    private final long maxEntriesBeforeSpill;
    private final DataSize memoryLimitBeforeSpill;
    private final Closer closer = Closer.create();

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
            int expectedGroups,
            long maxEntriesBeforeSpill,
            DataSize memoryLimitBeforeSpill,
            Optional<SpillerFactory> spillerFactory)
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
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.maxEntriesBeforeSpill = maxEntriesBeforeSpill;
        this.memoryLimitBeforeSpill = requireNonNull(memoryLimitBeforeSpill, "memoryLimitBeforeSpill is null");
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
        return !finishing && outputIterator == null && (aggregationBuilder == null || !aggregationBuilder.checkFullAndUpdateMemory());
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
                    operatorContext,
                    maxEntriesBeforeSpill,
                    memoryLimitBeforeSpill,
                    spillerFactory);

            closer.register(aggregationBuilder);
            // assume initial aggregationBuilder is not full
        }
        else {
            checkState(!aggregationBuilder.checkFullAndUpdateMemory(), "Aggregation buffer is full");
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

            if (!finishing && (aggregationBuilder.isSpillInProgress() || !aggregationBuilder.checkFullAndUpdateMemory())) {
                return null;
            }

            outputIterator = aggregationBuilder.buildResult();
            aggregationBuilder = null;

            if (!outputIterator.hasNext()) {
                // current output iterator is done
                outputIterator = null;
                return null;
            }
        }

        return outputIterator.next();
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
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

    private static class GroupByHashAggregationBuilder implements Closeable
    {
        private static final Logger log = Logger.get(GroupByHashAggregationBuilder.class);
        private static final int MAX_GROUPS_COUNT_DURING_MERGE = 1_000_000;

        private GroupByHash groupByHash;
        private List<Aggregator> aggregators;
        private final int expectedGroups;
        private final List<Type> groupByTypes;
        private List<Integer> groupByChannels;
        private Optional<Integer> hashChannel;
        private final OperatorContext operatorContext;
        private final LocalMemoryContext systemMemoryContext;
        private final boolean partial;
        private final List<AccumulatorFactory> accumulatorFactories;
        private Step step;
        private final Optional<SpillerFactory> spillerFactory;
        private final long maxEntriesBeforeSpill;
        private final long memorySizeBeforeSpill;
        private Optional<Spiller> spiller = Optional.empty();
        private CompletableFuture<?> spillInProgress = CompletableFuture.completedFuture(null);

        private GroupByHashAggregationBuilder(
                List<AccumulatorFactory> accumulatorFactories,
                Step step,
                int expectedGroups,
                List<Type> groupByTypes,
                List<Integer> groupByChannels,
                Optional<Integer> hashChannel,
                OperatorContext operatorContext,
                long maxEntriesBeforeSpill,
                DataSize memoryLimitBeforeSpill,
                Optional<SpillerFactory> spillerFactory)
        {
            this.accumulatorFactories = accumulatorFactories;
            this.step = step;
            this.expectedGroups = expectedGroups;
            this.groupByTypes = groupByTypes;
            this.groupByChannels = groupByChannels;
            this.hashChannel = hashChannel;
            this.operatorContext = operatorContext;
            this.partial = step.isOutputPartial();
            this.maxEntriesBeforeSpill = maxEntriesBeforeSpill;
            this.memorySizeBeforeSpill = memoryLimitBeforeSpill.toBytes();
            this.spillerFactory = spillerFactory;
            this.systemMemoryContext = operatorContext.getSystemMemoryContext().newLocalMemoryContext();

            rebuildAggregators();
        }

        public void processPage(Page page)
        {
            checkState(!isSpillInProgress(), "Previous spill hasn't yet finished");

            if (aggregators.isEmpty()) {
                groupByHash.addPage(page);
                return;
            }

            GroupByIdBlock groupIds = groupByHash.getGroupIds(page);

            for (Aggregator aggregator : aggregators) {
                aggregator.processPage(groupIds, page);
            }
        }

        public boolean checkFullAndUpdateMemory()
        {
            if (isSpillInProgress()) {
                return true;
            }

            long memorySize = getSizeInMemory();
            if (partial) {
                return !operatorContext.trySetMemoryReservation(memorySize);
            }
            else {
                if (isSpillingEnabled()) {
                    systemMemoryContext.setBytes(memorySize);

                    if (shouldSpill(memorySize)) {
                        spillToDisk();
                    }
                }
                else {
                    operatorContext.setMemoryReservation(memorySize);
                }
                return false;
            }
        }

        private boolean shouldSpill(long memorySize)
        {
            return (maxEntriesBeforeSpill > 0 && groupByHash.getGroupCount() > maxEntriesBeforeSpill)
                    || (memorySizeBeforeSpill > 0 && memorySize > memorySizeBeforeSpill);
        }

        private boolean isSpillingEnabled()
        {
            return maxEntriesBeforeSpill > 0 || memorySizeBeforeSpill > 0;
        }

        public Iterator<Page> buildResult()
        {
            checkState(!isSpillInProgress(), "Previous spill hasn't yet finished");

            if (!spiller.isPresent()) {
                return buildResultFromMemory();
            }

            try {
                // TODO: don't spill here to disk, instead merge disk content with memory content
                spillToDisk().get();
                return mergeFromDisk();
            }
            catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close()
                throws IOException
        {
            if (spiller.isPresent()) {
                spiller.get().close();
                spiller = Optional.empty();
            }
        }

        private CompletableFuture<?> spillToDisk()
        {
            for (Aggregator aggregator : aggregators) {
                aggregator.setOutputPartial();
            }

            if (!spiller.isPresent()) {
                spiller = Optional.of(spillerFactory.get().create(buildTypes()));
            }

            // start spilling process with current content of the aggregators and groupByHash...
            spillInProgress = spiller.get().spill(buildResultFromMemory(sortedGroupIds(), buildTypes(), groupByHash, aggregators));
            // ... and immediately create new aggregators and groupByHash so effectively memory ownership
            // over groupByHash and aggregators is transferred from this thread to spilling thread
            rebuildAggregators();

            return spillInProgress;
        }

        private Iterator<Page> mergeFromDisk()
        {
            checkState(spiller.isPresent());

            convertToMerge();
            rebuildAggregatorsForMerge();

            MergeSort mergeSort = new MergeSort(groupByTypes, buildIntermediateTypes());
            Iterator<Page> mergedSpilledPages = mergeSort.merge(spiller.get().getSpills());

            return buildResultFromMerge(mergedSpilledPages);
        }

        private Iterator<Page> buildResultFromMerge(Iterator<Page> mergedPages)
        {
            return new Iterator<Page>() {
                private Iterator<Page> resultPages = Collections.emptyIterator();

                @Override
                public boolean hasNext()
                {
                    return mergedPages.hasNext() || resultPages.hasNext();
                }

                @Override
                public Page next()
                {
                    if (!resultPages.hasNext()) {
                        rebuildAggregatorsForMerge();
                        long memorySize = 0; // ensure that at least one merged page will be processed

                        while (mergedPages.hasNext() && !shouldSpill(memorySize)) {
                            processPage(mergedPages.next());
                            memorySize = getSizeInMemory();
                            systemMemoryContext.setBytes(memorySize);
                        }
                        resultPages = buildResultFromMemory();
                    }

                    return resultPages.next();
                }
            };
        }

        private Iterator<Page> buildResultFromMemory()
        {
            return buildResultFromMemory(
                    consecutiveGroupIds(),
                    buildTypes(),
                    groupByHash,
                    aggregators);
        }

        private static Iterator<Page> buildResultFromMemory(
                Iterator<Integer> groupIds,
                List<Type> types,
                GroupByHash groupByHash,
                List<Aggregator> aggregators)
        {
            final PageBuilder pageBuilder = new PageBuilder(types);
            return new AbstractIterator<Page>()
            {
                @Override
                protected Page computeNext()
                {
                    if (!groupIds.hasNext()) {
                        return endOfData();
                    }

                    pageBuilder.reset();

                    List<Type> types = groupByHash.getTypes();
                    while (!pageBuilder.isFull() && groupIds.hasNext()) {
                        int groupId = groupIds.next();

                        groupByHash.appendValuesTo(groupId, pageBuilder, 0);

                        pageBuilder.declarePosition();
                        for (int i = 0; i < aggregators.size(); i++) {
                            Aggregator aggregator = aggregators.get(i);
                            BlockBuilder output = pageBuilder.getBlockBuilder(types.size() + i);
                            aggregator.evaluate(groupId, output);
                        }
                    }

                    return pageBuilder.build();
                }
            };
        }

        private void rebuildAggregators()
        {
            groupByHash = createGroupByHash();
            aggregators = createAggregators();
        }

        private void rebuildAggregatorsForMerge()
        {
            groupByHash = createGroupByHash();
            aggregators = createAggregatorsForMerge(groupByHash.getTypes().size());
        }

        private GroupByHash createGroupByHash()
        {
            return GroupByHash.createGroupByHash(operatorContext.getSession(), groupByTypes, Ints.toArray(groupByChannels), hashChannel, expectedGroups);
        }

        private List<Aggregator> createAggregators()
        {
            // wrapper each function with an aggregator
            ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();

            requireNonNull(accumulatorFactories, "accumulatorFactories is null");
            for (int i = 0; i < accumulatorFactories.size(); i++) {
                AccumulatorFactory accumulatorFactory = accumulatorFactories.get(i);
                builder.add(new Aggregator(accumulatorFactory, step));
            }
            return builder.build();
        }

        private List<Aggregator> createAggregatorsForMerge(int numberOfColumnsInKey)
        {
            // wrapper each function with an aggregator
            ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();

            requireNonNull(accumulatorFactories, "accumulatorFactories is null");
            for (int i = 0; i < accumulatorFactories.size(); i++) {
                AccumulatorFactory accumulatorFactory = accumulatorFactories.get(i);
                builder.add(new Aggregator(accumulatorFactory, step, numberOfColumnsInKey + i));
            }
            return builder.build();
        }

        private void convertToMerge()
        {
            step = Step.partialInput(step);
            ImmutableList.Builder<Integer> groupByPartialChannels = ImmutableList.builder();
            for (int i = 0; i < groupByTypes.size(); i++) {
                groupByPartialChannels.add(i);
            }
            if (hashChannel.isPresent()) {
                hashChannel = Optional.of(groupByTypes.size());
            }

            groupByChannels = groupByPartialChannels.build();
        }

        private List<Type> buildIntermediateTypes()
        {
            ArrayList<Type> types = new ArrayList<>(groupByHash.getTypes());
            for (Aggregator aggregator : aggregators) {
                types.add(aggregator.getIntermediateType());
            }
            return types;
        }

        private List<Type> buildTypes()
        {
            ArrayList<Type> types = new ArrayList<>(groupByHash.getTypes());
            for (Aggregator aggregator : aggregators) {
                types.add(aggregator.getType());
            }
            return types;
        }

        private long getSizeInMemory()
        {
            long sizeInMemory = groupByHash.getEstimatedSize();
            for (Aggregator aggregator : aggregators) {
                sizeInMemory += aggregator.getEstimatedSize();
            }
            sizeInMemory -= operatorContext.getOperatorPreAllocatedMemory().toBytes();
            if (sizeInMemory < 0) {
                sizeInMemory = 0;
            }
            return sizeInMemory;
        }

        private Iterator<Integer> consecutiveGroupIds()
        {
            return IntStream.range(0, groupByHash.getGroupCount()).iterator();
        }

        private Iterator<Integer> sortedGroupIds()
        {
            List<Integer> groupIds = Lists.newArrayList(consecutiveGroupIds());
            groupIds.sort(groupByHash::compare);
            return groupIds.iterator();
        }

        public boolean isSpillInProgress()
        {
            return !spillInProgress.isDone();
        }
    }

    private static class Aggregator
    {
        private final GroupedAccumulator aggregation;
        private Step step;
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

        public Aggregator(AccumulatorFactory accumulatorFactory, Step step, int intermediateChannel)
        {
            if (step.isInputRaw()) {
                this.intermediateChannel = -1;
                aggregation = accumulatorFactory.createGroupedAccumulator();
            }
            else {
                //TODO: re-enable this check somehow?
                //checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
                this.intermediateChannel = intermediateChannel;
                aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
            }
            this.step = step;
        }

        public long getEstimatedSize()
        {
            return aggregation.getEstimatedSize();
        }

        public Type getIntermediateType()
        {
            return aggregation.getIntermediateType();
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

        public void setOutputPartial()
        {
            step = Step.partialOutput(step);
        }
    }
}
