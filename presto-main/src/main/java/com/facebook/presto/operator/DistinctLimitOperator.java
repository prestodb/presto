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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isDictionaryAggregationEnabled;
import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DistinctLimitOperator
        implements Operator
{
    public static class DistinctLimitOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Integer> distinctChannels;
        private final List<Type> sourceTypes;
        private final long limit;
        private final Optional<Integer> hashChannel;
        private boolean closed;
        private final JoinCompiler joinCompiler;

        public DistinctLimitOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> distinctChannels,
                long limit,
                Optional<Integer> hashChannel,
                JoinCompiler joinCompiler)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.distinctChannels = requireNonNull(distinctChannels, "distinctChannels is null");

            checkArgument(limit >= 0, "limit must be at least zero");
            this.limit = limit;
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, DistinctLimitOperator.class.getSimpleName());
            List<Type> distinctTypes = distinctChannels.stream()
                    .map(sourceTypes::get)
                    .collect(toImmutableList());
            return new DistinctLimitOperator(operatorContext, distinctChannels, distinctTypes, limit, hashChannel, joinCompiler);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new DistinctLimitOperatorFactory(operatorId, planNodeId, sourceTypes, distinctChannels, limit, hashChannel, joinCompiler);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;

    private Page inputPage;
    private long remainingLimit;

    private boolean finishing;

    private final int[] outputChannels;
    private final GroupByHash groupByHash;
    private long nextDistinctId;

    // for yield when memory is not available
    private GroupByIdBlock groupByIds;
    private Work<GroupByIdBlock> unfinishedWork;

    public DistinctLimitOperator(OperatorContext operatorContext, List<Integer> distinctChannels, List<Type> distinctTypes, long limit, Optional<Integer> hashChannel, JoinCompiler joinCompiler)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        checkArgument(limit >= 0, "limit must be at least zero");
        requireNonNull(hashChannel, "hashChannel is null");

        int[] distinctChannelInts = Ints.toArray(requireNonNull(distinctChannels, "distinctChannels is null"));
        if (hashChannel.isPresent()) {
            outputChannels = Arrays.copyOf(distinctChannelInts, distinctChannelInts.length + 1);
            outputChannels[distinctChannelInts.length] = hashChannel.get();
        }
        else {
            outputChannels = distinctChannelInts.clone(); // defensive copy since this is passed into createGroupByHash
        }

        this.groupByHash = createGroupByHash(
                distinctTypes,
                distinctChannelInts,
                hashChannel,
                min((int) limit, 10_000),
                isDictionaryAggregationEnabled(operatorContext.getSession()),
                joinCompiler,
                this::updateMemoryReservation);
        remainingLimit = limit;
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
        return !hasUnfinishedInput() && (finishing || remainingLimit == 0);
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && remainingLimit > 0 && !hasUnfinishedInput();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(needsInput());

        inputPage = page;
        unfinishedWork = groupByHash.getGroupIds(page);
        processUnfinishedWork();
        updateMemoryReservation();
    }

    @Override
    public Page getOutput()
    {
        if (unfinishedWork != null && !processUnfinishedWork()) {
            return null;
        }

        if (groupByIds == null) {
            return null;
        }

        verify(inputPage != null);

        long resultingPositions = min(groupByIds.getGroupCount() - nextDistinctId, remainingLimit);
        Page result = null;
        if (resultingPositions > 0) {
            int[] distinctPositions = new int[toIntExact(resultingPositions)];
            int distinctCount = 0;
            for (int position = 0; position < groupByIds.getPositionCount() && distinctCount < distinctPositions.length; position++) {
                if (groupByIds.getGroupId(position) == nextDistinctId) {
                    distinctPositions[distinctCount++] = position;
                    nextDistinctId++;
                }
            }
            verify(distinctCount == distinctPositions.length);
            remainingLimit -= distinctCount;
            result = inputPage.extractChannels(outputChannels).getPositions(distinctPositions, 0, distinctPositions.length);
        }

        groupByIds = null;
        inputPage = null;

        updateMemoryReservation();
        return result;
    }

    private boolean processUnfinishedWork()
    {
        verify(unfinishedWork != null);
        if (!unfinishedWork.process()) {
            return false;
        }
        groupByIds = unfinishedWork.getResult();
        unfinishedWork = null;
        return true;
    }

    private boolean hasUnfinishedInput()
    {
        return inputPage != null || unfinishedWork != null;
    }

    /**
     * Update memory usage.
     *
     * @return true if the reservation is within the limit
     */
    // TODO: update in the interface after the new memory tracking framework is landed (#9049)
    // Essentially we would love to have clean interfaces to support both pushing and pulling memory usage
    // The following implementation is a hybrid model, where the push model is going to call the pull model causing reentrancy
    private boolean updateMemoryReservation()
    {
        // Operator/driver will be blocked on memory after we call localUserMemoryContext.setBytes().
        // If memory is not available, once we return, this operator will be blocked until memory is available.
        localUserMemoryContext.setBytes(groupByHash.getEstimatedSize());
        // If memory is not available, inform the caller that we cannot proceed for allocation.
        return operatorContext.isWaitingForMemory().isDone();
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return groupByHash.getCapacity();
    }
}
