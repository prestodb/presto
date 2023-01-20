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
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.isDictionaryAggregationEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TopNRowNumberOperator
        implements Operator
{
    public static class TopNRowNumberOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;

        private final List<Type> sourceTypes;

        private final List<Integer> outputChannels;
        private final List<Integer> partitionChannels;
        private final List<Type> partitionTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final int maxRowCountPerPartition;
        private final boolean partial;
        private final Optional<Integer> hashChannel;
        private final int expectedPositions;

        private final boolean generateRowNumber;
        private boolean closed;
        private final long unspillMemoryLimit;
        private final JoinCompiler joinCompiler;
        private final SpillerFactory spillerFactory;
        private final boolean spillEnabled;

        public TopNRowNumberOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> partitionChannels,
                List<? extends Type> partitionTypes,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int maxRowCountPerPartition,
                boolean partial,
                Optional<Integer> hashChannel,
                int expectedPositions,
                long unspillMemoryLimit,
                JoinCompiler joinCompiler,
                SpillerFactory spillerFactory,
                boolean spillEnabled)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
            this.partitionTypes = ImmutableList.copyOf(requireNonNull(partitionTypes, "partitionTypes is null"));
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder));
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.partial = partial;
            checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");
            this.maxRowCountPerPartition = maxRowCountPerPartition;
            checkArgument(expectedPositions > 0, "expectedPositions must be > 0");
            this.generateRowNumber = !partial;
            this.expectedPositions = expectedPositions;
            this.unspillMemoryLimit = unspillMemoryLimit;
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.spillerFactory = spillerFactory;
            this.spillEnabled = spillEnabled;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TopNRowNumberOperator.class.getSimpleName());
            return new TopNRowNumberOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    sortOrder,
                    maxRowCountPerPartition,
                    generateRowNumber,
                    hashChannel,
                    expectedPositions,
                    unspillMemoryLimit,
                    joinCompiler,
                    spillerFactory,
                    spillEnabled);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TopNRowNumberOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, partitionChannels, partitionTypes, sortChannels, sortOrder, maxRowCountPerPartition, partial, hashChannel, expectedPositions, unspillMemoryLimit, joinCompiler, spillerFactory, spillEnabled);
        }
    }

    private final OperatorContext operatorContext;

    private final int[] outputChannels;

    private GroupedTopNBuilder groupedTopNBuilder;

    private boolean finishing;
    private boolean finished;
    private Work<?> unfinishedWork;
    private WorkProcessor<Page> outputPages;

    public TopNRowNumberOperator(
            OperatorContext operatorContext,
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<Integer> partitionChannels,
            List<Type> partitionTypes,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            int maxRowCountPerPartition,
            boolean generateRowNumber,
            Optional<Integer> hashChannel,
            int expectedPositions,
            long unspillMemoryLimit,
            JoinCompiler joinCompiler,
            SpillerFactory spillerFactory,
            boolean spillEnabled)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        ImmutableList.Builder<Integer> outputChannelsBuilder = ImmutableList.builder();
        for (int channel : requireNonNull(outputChannels, "outputChannels is null")) {
            outputChannelsBuilder.add(channel);
        }
        if (generateRowNumber) {
            outputChannelsBuilder.add(outputChannels.size());
        }
        this.outputChannels = Ints.toArray(outputChannelsBuilder.build());

        checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");

        List<Type> types = toTypes(sourceTypes, outputChannels, generateRowNumber);
        Supplier<GroupByHash> groupByHashSupplier = () -> createGroupByHash(
                partitionTypes,
                partitionChannels,
                hashChannel,
                expectedPositions,
                joinCompiler,
                isDictionaryAggregationEnabled(operatorContext.getSession()),
                this::updateMemoryReservation);

        if (spillEnabled) {
            this.groupedTopNBuilder = new SpillableGroupedTopNBuilder(
                    ImmutableList.copyOf(sourceTypes),
                    partitionTypes,
                    partitionChannels,
                    () -> new InMemoryGroupedTopNBuilder(
                            ImmutableList.copyOf(sourceTypes),
                            new SimplePageWithPositionComparator(types, sortChannels, sortOrders),
                            maxRowCountPerPartition,
                            generateRowNumber,
                            operatorContext.localRevocableMemoryContext(),
                            groupByHashSupplier.get()),
                    () -> new InMemoryGroupedTopNBuilder(
                            ImmutableList.copyOf(sourceTypes),
                            new SimplePageWithPositionComparator(types, sortChannels, sortOrders),
                            maxRowCountPerPartition,
                            generateRowNumber,
                            operatorContext.localUserMemoryContext(),
                            groupByHashSupplier.get()),
                    operatorContext::isWaitingForMemory,
                    unspillMemoryLimit,
                    operatorContext.localUserMemoryContext(),
                    operatorContext.localRevocableMemoryContext(),
                    operatorContext.aggregateSystemMemoryContext(),
                    operatorContext.aggregateSystemMemoryContext(),
                    operatorContext.getSpillContext(),
                    operatorContext.getDriverContext().getYieldSignal(),
                    spillerFactory);
        }
        else {
            this.groupedTopNBuilder = new InMemoryGroupedTopNBuilder(
                    ImmutableList.copyOf(sourceTypes),
                    new SimplePageWithPositionComparator(types, sortChannels, sortOrders),
                    maxRowCountPerPartition,
                    generateRowNumber,
                    operatorContext.localUserMemoryContext(),
                    groupByHashSupplier.get());
        }
    }

    private GroupByHash createGroupByHash(
            List<? extends Type> partitionTypes,
            List<Integer> partitionChannels,
            Optional<Integer> inputHashChannel,
            int expectedPositions,
            JoinCompiler joinCompiler,
            boolean isDictionaryAggregationEnabled,
            UpdateMemory updateMemory)
    {
        if (!partitionChannels.isEmpty()) {
            checkArgument(expectedPositions > 0, "expectedPositions must be > 0");
            return GroupByHash.createGroupByHash(
                    partitionTypes,
                    Ints.toArray(partitionChannels),
                    inputHashChannel,
                    expectedPositions,
                    isDictionaryAggregationEnabled,
                    joinCompiler,
                    updateMemory);
        }
        else {
            return new NoChannelGroupByHash();
        }
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
        // has no more input, has finished flushing, and has no unfinished work
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        // still has more input, has not started flushing yet, and has no unfinished work
        return !finishing && outputPages == null && unfinishedWork == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkState(unfinishedWork == null, "Cannot add input with the operator when unfinished work is not empty");
        checkState(outputPages == null, "Cannot add input with the operator when flushing");
        requireNonNull(page, "page is null");
        unfinishedWork = groupedTopNBuilder.processPage(page);
        if (unfinishedWork.process()) {
            unfinishedWork = null;
        }
        updateMemoryReservation();
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        if (finishing) {
            return NOT_BLOCKED;
        }
        return groupedTopNBuilder.startMemoryRevoke();
    }

    @Override
    public void finishMemoryRevoke()
    {
        groupedTopNBuilder.finishMemoryRevoke();
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }

        if (unfinishedWork != null) {
            boolean finished = unfinishedWork.process();
            updateMemoryReservation();
            if (!finished) {
                return null;
            }
            unfinishedWork = null;
        }

        if (!finishing) {
            return null;
        }

        if (outputPages == null) {
            if (groupedTopNBuilder == null) {
                finished = true;
                return null;
            }
            // start flushing
            outputPages = groupedTopNBuilder.buildResult();
        }

        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            if (groupedTopNBuilder != null) {
                groupedTopNBuilder.close();
                groupedTopNBuilder = null;
            }
            finished = true;
            return null;
        }

        Page outputPage = outputPages.getResult()
                .extractChannels(outputChannels);

        updateMemoryReservation();
        return outputPage;
    }

    @VisibleForTesting
    public int getCapacity()
    {
        GroupByHash groupByHash = groupedTopNBuilder.getGroupByHash();
        checkState(groupByHash != null);
        return groupByHash.getCapacity();
    }

    private boolean updateMemoryReservation()
    {
        groupedTopNBuilder.updateMemoryReservations();
        return operatorContext.isWaitingForMemory().isDone();
    }

    private static List<Type> toTypes(List<? extends Type> sourceTypes, List<Integer> outputChannels, boolean generateRowNumber)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int channel : outputChannels) {
            types.add(sourceTypes.get(channel));
        }
        if (generateRowNumber) {
            types.add(BIGINT);
        }
        return types.build();
    }
}
