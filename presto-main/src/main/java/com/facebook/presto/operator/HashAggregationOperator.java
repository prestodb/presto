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
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.builder.HashAggregationBuilder;
import com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import com.facebook.presto.operator.aggregation.builder.SpillableHashAggregationBuilder;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.spi.function.aggregation.Accumulator;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder.toTypes;
import static com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer.INITIAL_HASH_VALUE;
import static com.facebook.presto.type.TypeUtils.NULL_HASH_CODE;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class HashAggregationOperator
        implements Operator
{
    private static final double MERGE_WITH_MEMORY_RATIO = 0.9;

    public static class HashAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        // A subset of groupByChannels, containing channels that are already sorted.
        private final List<Integer> preGroupedChannels;
        private final List<Integer> globalAggregationGroupIds;
        private final Step step;
        private final boolean produceDefaultOutput;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final Optional<Integer> hashChannel;
        private final Optional<Integer> groupIdChannel;

        private final int expectedGroups;
        private final Optional<DataSize> maxPartialMemory;
        private final boolean spillEnabled;
        private final DataSize memoryLimitForMerge;
        private final DataSize memoryLimitForMergeWithMemory;
        private final SpillerFactory spillerFactory;
        private final JoinCompiler joinCompiler;
        private final boolean useSystemMemory;

        private boolean closed;

        @VisibleForTesting
        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> preGroupedChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    preGroupedChannels,
                    globalAggregationGroupIds,
                    step,
                    false,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    false,
                    new DataSize(0, MEGABYTE),
                    new DataSize(0, MEGABYTE),
                    (types, spillContext, memoryContext) -> {
                        throw new UnsupportedOperationException();
                    },
                    joinCompiler,
                    useSystemMemory);
        }

        public HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> preGroupedChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                boolean produceDefaultOutput,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                boolean spillEnabled,
                DataSize unspillMemoryLimit,
                SpillerFactory spillerFactory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    preGroupedChannels,
                    globalAggregationGroupIds,
                    step,
                    produceDefaultOutput,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    spillEnabled,
                    unspillMemoryLimit,
                    DataSize.succinctBytes((long) (unspillMemoryLimit.toBytes() * MERGE_WITH_MEMORY_RATIO)),
                    spillerFactory,
                    joinCompiler,
                    useSystemMemory);
        }

        @VisibleForTesting
        HashAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> preGroupedChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                boolean produceDefaultOutput,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                boolean spillEnabled,
                DataSize memoryLimitForMerge,
                DataSize memoryLimitForMergeWithMemory,
                SpillerFactory spillerFactory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
            this.groupByTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = ImmutableList.copyOf(groupByChannels);
            this.preGroupedChannels = ImmutableList.copyOf(preGroupedChannels);
            this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
            this.step = step;
            this.produceDefaultOutput = produceDefaultOutput;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.expectedGroups = expectedGroups;
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
            this.spillEnabled = spillEnabled;
            this.memoryLimitForMerge = requireNonNull(memoryLimitForMerge, "memoryLimitForMerge is null");
            this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.useSystemMemory = useSystemMemory;
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
                    preGroupedChannels,
                    globalAggregationGroupIds,
                    step,
                    produceDefaultOutput,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    spillEnabled,
                    memoryLimitForMerge,
                    memoryLimitForMergeWithMemory,
                    spillerFactory,
                    joinCompiler,
                    useSystemMemory);
            return hashAggregationOperator;
        }

        @Override
        public void noMoreOperators()
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
                    preGroupedChannels,
                    globalAggregationGroupIds,
                    step,
                    produceDefaultOutput,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    spillEnabled,
                    memoryLimitForMerge,
                    memoryLimitForMergeWithMemory,
                    spillerFactory,
                    joinCompiler,
                    useSystemMemory);
        }
    }

    private final OperatorContext operatorContext;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final int[] preGroupedChannels;
    private final List<Integer> globalAggregationGroupIds;
    private final Step step;
    private final boolean produceDefaultOutput;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Optional<Integer> hashChannel;
    private final Optional<Integer> groupIdChannel;
    private final int expectedGroups;
    private final Optional<DataSize> maxPartialMemory;
    private final boolean spillEnabled;
    private final DataSize memoryLimitForMerge;
    private final DataSize memoryLimitForMergeWithMemory;
    private final SpillerFactory spillerFactory;
    private final JoinCompiler joinCompiler;
    private final boolean useSystemMemory;
    private final Optional<PagesHashStrategy> preGroupedHashStrategy;

    private final List<Type> types;
    private final HashCollisionsCounter hashCollisionsCounter;

    private HashAggregationBuilder aggregationBuilder;
    private WorkProcessor<Page> outputPages;
    private boolean inputProcessed;
    private boolean finishing;
    private boolean finished;
    private Page firstUnfinishedSegment;
    private Page remainingPageForSegmentedAggregation;

    // for yield when memory is not available
    private Work<?> unfinishedWork;

    public HashAggregationOperator(
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            List<Integer> preGroupedChannels,
            List<Integer> globalAggregationGroupIds,
            Step step,
            boolean produceDefaultOutput,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> hashChannel,
            Optional<Integer> groupIdChannel,
            int expectedGroups,
            Optional<DataSize> maxPartialMemory,
            boolean spillEnabled,
            DataSize memoryLimitForMerge,
            DataSize memoryLimitForMergeWithMemory,
            SpillerFactory spillerFactory,
            JoinCompiler joinCompiler,
            boolean useSystemMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(step, "step is null");
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        requireNonNull(operatorContext, "operatorContext is null");

        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.preGroupedChannels = Ints.toArray(requireNonNull(preGroupedChannels, "preGroupedChannels is null"));
        this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
        this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
        this.step = step;
        this.produceDefaultOutput = produceDefaultOutput;
        this.expectedGroups = expectedGroups;
        this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
        this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
        this.spillEnabled = spillEnabled;
        this.memoryLimitForMerge = requireNonNull(memoryLimitForMerge, "memoryLimitForMerge is null");
        this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
        this.useSystemMemory = useSystemMemory;

        checkState(ImmutableSet.copyOf(groupByChannels).containsAll(preGroupedChannels), "groupByChannels must include all channels in preGroupedChannels");
        this.preGroupedHashStrategy = preGroupedChannels.isEmpty()
                ? Optional.empty()
                : Optional.of(joinCompiler.compilePagesHashStrategyFactory(
                        preGroupedChannels.stream().map(groupByTypes::get).collect(toImmutableList()), preGroupedChannels, Optional.empty())
                .createPagesHashStrategy(groupByTypes.stream().map(type -> ImmutableList.<Block>of()).collect(toImmutableList()), OptionalInt.empty()));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
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

    // This operator needs input from the upstream operator only when all of following conditions are true:
    // - 1. It has not received finish() signal (more input to come).
    // - 2. Current page has been processed.
    // - 3. Aggregation builder has not been triggered or has finished processing.
    // - 4. If this is partial aggregation then it must have not reached the memory limit.
    // - 5. If running in segmented aggregation mode, there must be no remaining page to process.
    @Override
    public boolean needsInput()
    {
        return !finishing
                && unfinishedWork == null
                && outputPages == null
                && !partialAggregationReachedMemoryLimit()
                && remainingPageForSegmentedAggregation == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(unfinishedWork == null, "Operator has unfinished work");
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        inputProcessed = true;

        initializeAggregationBuilderIfNeeded();
        processInputPage(page);

        // process the current page; save the unfinished work if we are waiting for memory
        if (unfinishedWork != null && unfinishedWork.process()) {
            unfinishedWork = null;
        }
        aggregationBuilder.updateMemory();
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        if (aggregationBuilder != null) {
            return aggregationBuilder.startMemoryRevoke();
        }
        return NOT_BLOCKED;
    }

    @Override
    public void finishMemoryRevoke()
    {
        if (aggregationBuilder != null) {
            aggregationBuilder.finishMemoryRevoke();
        }
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }

        // process unfinished work if one exists
        if (unfinishedWork != null) {
            boolean workDone = unfinishedWork.process();
            aggregationBuilder.updateMemory();
            if (!workDone) {
                return null;
            }
            unfinishedWork = null;
        }

        if (outputPages == null) {
            if (finishing) {
                if (!inputProcessed && produceDefaultOutput) {
                    // global aggregations always generate an output row with the default aggregation output (e.g. 0 for COUNT, NULL for SUM)
                    finished = true;
                    return getGlobalAggregationOutput();
                }

                if (aggregationBuilder == null) {
                    finished = true;
                    return null;
                }
            }

            if (!shouldFlush()) {
                return null;
            }

            outputPages = aggregationBuilder.buildResult();
        }

        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            closeAggregationBuilder();
            processRemainingPageForSegmentedAggregation();
            return null;
        }

        return outputPages.getResult();
    }

    @Override
    public void close()
    {
        closeAggregationBuilder();
    }

    @VisibleForTesting
    public HashAggregationBuilder getAggregationBuilder()
    {
        return aggregationBuilder;
    }

    private void processInputPage(Page page)
    {
        // 1. normal aggregation
        if (!preGroupedHashStrategy.isPresent()) {
            unfinishedWork = aggregationBuilder.processPage(page);
            return;
        }

        // 2. segmented aggregation
        if (firstUnfinishedSegment == null) {
            // If this is the first page, treat the first segment in this page as the current segment.
            firstUnfinishedSegment = page.getRegion(0, 1);
        }

        Page pageOnPreGroupedChannels = page.extractChannels(preGroupedChannels);
        int lastRowInPage = page.getPositionCount() - 1;
        int lastSegmentStart = findLastSegmentStart(preGroupedHashStrategy.get(), pageOnPreGroupedChannels);
        if (lastSegmentStart == 0) {
            // The whole page is in one segment.
            if (preGroupedHashStrategy.get().rowEqualsRow(0, firstUnfinishedSegment.extractChannels(preGroupedChannels), 0, pageOnPreGroupedChannels)) {
                // All rows in this page belong to the previous unfinished segment, process the whole page.
                unfinishedWork = aggregationBuilder.processPage(page);
            }
            else {
                // If the current page starts with a new segment, flush before processing it.
                remainingPageForSegmentedAggregation = page;
            }
        }
        else {
            // If the current segment ends in the current page, flush it with all the segments (if exist) except the last segment of the current page.
            unfinishedWork = aggregationBuilder.processPage(page.getRegion(0, lastSegmentStart));
            remainingPageForSegmentedAggregation = page.getRegion(lastSegmentStart, lastRowInPage - lastSegmentStart + 1);
        }
        // Record the last segment.
        firstUnfinishedSegment = page.getRegion(lastRowInPage, 1);
    }

    private int findLastSegmentStart(PagesHashStrategy pagesHashStrategy, Page page)
    {
        for (int i = page.getPositionCount() - 1; i > 0; i--) {
            if (!pagesHashStrategy.rowEqualsRow(i - 1, page, i, page)) {
                return i;
            }
        }
        return 0;
    }

    private void closeAggregationBuilder()
    {
        outputPages = null;
        if (aggregationBuilder != null) {
            aggregationBuilder.recordHashCollisions(hashCollisionsCounter);
            aggregationBuilder.close();
            // aggregationBuilder.close() will release all memory reserved in memory accounting.
            // The reference must be set to null afterwards to avoid unaccounted memory.
            aggregationBuilder = null;
        }
        operatorContext.localUserMemoryContext().setBytes(0);
        operatorContext.localRevocableMemoryContext().setBytes(0);
    }

    private void processRemainingPageForSegmentedAggregation()
    {
        // Running in segmented aggregation mode, reopen the aggregation builder and process the remaining page.
        if (remainingPageForSegmentedAggregation != null) {
            initializeAggregationBuilderIfNeeded();
            unfinishedWork = aggregationBuilder.processPage(remainingPageForSegmentedAggregation);
            remainingPageForSegmentedAggregation = null;
        }
    }

    private void initializeAggregationBuilderIfNeeded()
    {
        if (aggregationBuilder != null) {
            checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
            return;
        }

        if (step.isOutputPartial() || !spillEnabled) {
            aggregationBuilder = new InMemoryHashAggregationBuilder(
                    accumulatorFactories,
                    step,
                    expectedGroups,
                    groupByTypes,
                    groupByChannels,
                    hashChannel,
                    operatorContext,
                    maxPartialMemory,
                    joinCompiler,
                    true,
                    useSystemMemory);
        }
        else {
            verify(!useSystemMemory, "using system memory in spillable aggregations is not supported");
            aggregationBuilder = new SpillableHashAggregationBuilder(
                    accumulatorFactories,
                    step,
                    expectedGroups,
                    groupByTypes,
                    groupByChannels,
                    hashChannel,
                    operatorContext,
                    memoryLimitForMerge,
                    memoryLimitForMergeWithMemory,
                    spillerFactory,
                    joinCompiler);
        }
    }

    // Flush if one of the following is true:
    // - received finish() signal (no more input to come).
    // - it is a partial aggregation and has reached memory limit
    // - running in segmented aggregation mode and at least one segment has been fully processed
    private boolean shouldFlush()
    {
        return finishing || partialAggregationReachedMemoryLimit() || remainingPageForSegmentedAggregation != null;
    }

    private boolean partialAggregationReachedMemoryLimit()
    {
        return aggregationBuilder != null && aggregationBuilder.isFull();
    }

    private Page getGlobalAggregationOutput()
    {
        List<Accumulator> accumulators = accumulatorFactories.stream()
                // No input will be added to the accumulators, it is ok not to specify the memory callback
                .map(accumulatorFactory -> accumulatorFactory.createAccumulator(UpdateMemory.NOOP))
                .collect(Collectors.toList());

        // global aggregation output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder output = new PageBuilder(globalAggregationGroupIds.size(), types);

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
                long hashValue = calculateDefaultOutputHash(groupByTypes, groupIdChannel.get(), groupId);
                output.getBlockBuilder(channel++).writeLong(hashValue);
            }

            for (int j = 0; j < accumulators.size(); channel++, j++) {
                if (step.isOutputPartial()) {
                    accumulators.get(j).evaluateIntermediate(output.getBlockBuilder(channel));
                }
                else {
                    accumulators.get(j).evaluateFinal(output.getBlockBuilder(channel));
                }
            }
        }

        if (output.isEmpty()) {
            return null;
        }
        return output.build();
    }

    private static long calculateDefaultOutputHash(List<Type> groupByChannels, int groupIdChannel, int groupId)
    {
        // Default output has NULLs on all columns except of groupIdChannel
        long result = INITIAL_HASH_VALUE;
        for (int channel = 0; channel < groupByChannels.size(); channel++) {
            if (channel != groupIdChannel) {
                result = CombineHashFunction.getHash(result, NULL_HASH_CODE);
            }
            else {
                result = CombineHashFunction.getHash(result, BigintType.hash(groupId));
            }
        }
        return result;
    }
}
