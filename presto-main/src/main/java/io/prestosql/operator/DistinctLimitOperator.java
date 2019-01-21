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
package io.prestosql.operator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.prestosql.operator.GroupByHash.createGroupByHash;
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

    private final List<Integer> outputChannels;
    private final GroupByHash groupByHash;
    private long nextDistinctId;

    // for yield when memory is not available
    private GroupByIdBlock groupByIds;
    private Work<GroupByIdBlock> unfinishedWork;

    public DistinctLimitOperator(OperatorContext operatorContext, List<Integer> distinctChannels, List<Type> distinctTypes, long limit, Optional<Integer> hashChannel, JoinCompiler joinCompiler)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        requireNonNull(distinctChannels, "distinctChannels is null");
        checkArgument(limit >= 0, "limit must be at least zero");
        requireNonNull(hashChannel, "hashChannel is null");

        outputChannels = ImmutableList.<Integer>builder()
                .addAll(distinctChannels)
                .addAll(hashChannel.map(ImmutableList::of).orElse(ImmutableList.of()))
                .build();

        this.groupByHash = createGroupByHash(
                distinctTypes,
                Ints.toArray(distinctChannels),
                hashChannel,
                Math.min((int) limit, 10_000),
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
        int distinctCount = 0;
        int[] distinctPositions = new int[inputPage.getPositionCount()];
        for (int position = 0; position < groupByIds.getPositionCount(); position++) {
            if (groupByIds.getGroupId(position) == nextDistinctId) {
                distinctPositions[distinctCount] = position;
                distinctCount++;

                remainingLimit--;
                nextDistinctId++;
                if (remainingLimit == 0) {
                    break;
                }
            }
        }
        Page result = maskToDistinctOutputPositions(distinctCount, distinctPositions);

        groupByIds = null;
        inputPage = null;

        updateMemoryReservation();
        return result;
    }

    private Page maskToDistinctOutputPositions(int distinctCount, int[] distinctPositions)
    {
        Page result = null;
        if (distinctCount > 0) {
            Block[] blocks = outputChannels.stream()
                    .map(inputPage::getBlock)
                    .map(block -> block.getPositions(distinctPositions, 0, distinctCount))
                    .toArray(Block[]::new);
            result = new Page(distinctCount, blocks);
        }
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
    // TODO: update in the interface now that the new memory tracking framework is landed
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
