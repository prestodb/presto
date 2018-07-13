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

import com.facebook.presto.operator.ChannelSet.ChannelSetBuilder;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SetBuilderOperator
        implements Operator
{
    public static class SetBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Optional<Integer> hashChannel;
        private final JoinBridgeDataManager<SetBridge> setBridgeManager;
        private final int setChannel;
        private final int expectedPositions;
        private boolean closed;
        private final JoinCompiler joinCompiler;

        public SetBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeDataManager<SetBridge> setBridgeManager,
                int setChannel,
                Optional<Integer> hashChannel,
                int expectedPositions,
                JoinCompiler joinCompiler)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            Preconditions.checkArgument(setChannel >= 0, "setChannel is negative");
            this.setBridgeManager = requireNonNull(setBridgeManager, "setBridgeManager is null");
            this.setChannel = setChannel;
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.expectedPositions = expectedPositions;
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, SetBuilderOperator.class.getSimpleName());
            return new SetBuilderOperator(operatorContext, setBridgeManager.forLifespan(driverContext.getLifespan()), setChannel, hashChannel, expectedPositions, joinCompiler);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new SetBuilderOperatorFactory(operatorId, planNodeId, setBridgeManager, setChannel, hashChannel, expectedPositions, joinCompiler);
        }
    }

    private final OperatorContext operatorContext;
    private final SetBridge setBridge;
    private final int setChannel;
    private final Optional<Integer> hashChannel;

    private final ChannelSetBuilder channelSetBuilder;

    // When the set is no longer needed, the isFinished method on this operator will return true.
    private final ListenableFuture<?> setDestroyed;

    private boolean setBuilt;

    @Nullable
    private Work<?> unfinishedWork;  // The pending work for current page.

    public SetBuilderOperator(
            OperatorContext operatorContext,
            SetBridge setBridge,
            int setChannel,
            Optional<Integer> hashChannel,
            int expectedPositions,
            JoinCompiler joinCompiler)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.setBridge = requireNonNull(setBridge, "setSupplierBridge is null");
        this.setChannel = setChannel;

        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        // Set builder is has a single channel which goes in channel 0, if hash is present, add a hachBlock to channel 1
        Optional<Integer> channelSetHashChannel = hashChannel.isPresent() ? Optional.of(1) : Optional.empty();
        this.channelSetBuilder = new ChannelSetBuilder(
                setBridge.getType(),
                channelSetHashChannel,
                expectedPositions,
                requireNonNull(operatorContext, "operatorContext is null"),
                requireNonNull(joinCompiler, "joinCompiler is null"));
        setDestroyed = setBridge.isDestroyed();
        setDestroyed.addListener(operatorContext::notifyAsync, directExecutor());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (setBuilt) {
            return;
        }

        ChannelSet channelSet = channelSetBuilder.build();
        setBridge.setChannelSet(channelSet);
        operatorContext.recordGeneratedOutput(channelSet.getEstimatedSizeInBytes(), channelSet.size());
        setBuilt = true;
    }

    @Override
    public boolean isFinished()
    {
        return setDestroyed.isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return setBuilt ? setDestroyed : NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        // Since SetBuilderOperator doesn't produce any output, the getOutput()
        // method may never be called. We need to handle any unfinished work
        // before addInput() can be called again.
        return !setBuilt && (unfinishedWork == null || processUnfinishedWork()) && !isFinished();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        Block sourceBlock = page.getBlock(setChannel);
        Page sourcePage = hashChannel.isPresent() ? new Page(sourceBlock, page.getBlock(hashChannel.get())) : new Page(sourceBlock);

        unfinishedWork = channelSetBuilder.addPage(sourcePage);
        processUnfinishedWork();
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    private boolean processUnfinishedWork()
    {
        // Processes the unfinishedWork for this page by adding the data to the hash table. If this page
        // can't be fully consumed (e.g. rehashing fails), the unfinishedWork will be left with non-empty value.
        checkState(unfinishedWork != null, "unfinishedWork is empty");
        boolean done = unfinishedWork.process();
        if (done) {
            unfinishedWork = null;
        }
        // We need to update the memory reservation again since the page builder memory may also be increasing.
        channelSetBuilder.updateMemoryReservation();
        return done;
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return channelSetBuilder.getCapacity();
    }
}
