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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static java.util.Objects.requireNonNull;

public class HashSemiJoinOperator
        implements Operator
{
    public static class HashSemiJoinOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final JoinBridgeLifecycleManager<SetBridge> joinBridgeManager;
        private final List<Type> probeTypes;
        private final int probeJoinChannel;
        private boolean closed;

        public HashSemiJoinOperatorFactory(int operatorId, PlanNodeId planNodeId, JoinBridgeDataManager<SetBridge> setBridgeDataManager, List<? extends Type> probeTypes, int probeJoinChannel)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.joinBridgeManager = JoinBridgeLifecycleManager.semiJoin(requireNonNull(setBridgeDataManager, "setBridgeDataManager is null"));
            this.probeTypes = ImmutableList.copyOf(probeTypes);
            checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");
            this.probeJoinChannel = probeJoinChannel;
        }

        public HashSemiJoinOperatorFactory(HashSemiJoinOperatorFactory other)
        {
            requireNonNull(other, "other is null");
            this.operatorId = other.operatorId;
            this.planNodeId = other.planNodeId;

            this.probeTypes = other.probeTypes;
            this.probeJoinChannel = other.probeJoinChannel;

            // joinBridgeManager must be duplicated here.
            // Otherwise, reference counting and lifecycle management will be wrong.
            this.joinBridgeManager = other.joinBridgeManager.duplicate();

            // closed is intentionally not copied
            closed = false;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            SetBridge setBridge = joinBridgeManager.getJoinBridge(driverContext.getLifespan());
            ReferenceCount probeReferenceCount = joinBridgeManager.getProbeReferenceCount(driverContext.getLifespan());

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashSemiJoinOperator.class.getSimpleName());

            probeReferenceCount.retain();
            return new HashSemiJoinOperator(operatorContext, setBridge, probeJoinChannel, probeReferenceCount::release);
        }

        @Override
        public void noMoreOperators()
        {
            if (closed) {
                return;
            }
            closed = true;
            joinBridgeManager.noMoreLifespan();
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            joinBridgeManager.getProbeReferenceCount(lifespan).release();
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashSemiJoinOperatorFactory(this);
        }
    }

    private final OperatorContext operatorContext;
    private final Runnable afterClose;

    private final int probeJoinChannel;
    private final ListenableFuture<ChannelSet> channelSetFuture;

    private ChannelSet channelSet;
    private Page outputPage;
    private boolean finishing;
    private boolean closed;

    public HashSemiJoinOperator(OperatorContext operatorContext, SetBridge setBridge, int probeJoinChannel, Runnable afterClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        // todo pass in desired projection
        requireNonNull(setBridge, "setBridge is null");
        checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");

        this.channelSetFuture = setBridge.getChannelSet();
        this.probeJoinChannel = probeJoinChannel;
        this.afterClose = requireNonNull(afterClose, "afterClose is null");
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
        boolean finished = finishing && outputPage == null;

        if (finished) {
            close();
        }
        return finished;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return channelSetFuture;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || outputPage != null) {
            return false;
        }

        if (channelSet == null) {
            channelSet = tryGetFutureValue(channelSetFuture).orElse(null);
        }
        return channelSet != null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!finishing, "Operator is finishing");
        checkState(channelSet != null, "Set has not been built yet");
        checkState(outputPage == null, "Operator still has pending output");

        // create the block builder for the new boolean column
        // we know the exact size required for the block
        BlockBuilder blockBuilder = BOOLEAN.createFixedSizeBlockBuilder(page.getPositionCount());

        Page probeJoinPage = new Page(page.getBlock(probeJoinChannel));

        // update hashing strategy to use probe cursor
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (probeJoinPage.getBlock(0).isNull(position)) {
                if (channelSet.isEmpty()) {
                    BOOLEAN.writeBoolean(blockBuilder, false);
                }
                else {
                    blockBuilder.appendNull();
                }
            }
            else {
                boolean contains = channelSet.contains(position, probeJoinPage);
                if (!contains && channelSet.containsNull()) {
                    blockBuilder.appendNull();
                }
                else {
                    BOOLEAN.writeBoolean(blockBuilder, contains);
                }
            }
        }

        // add the new boolean column to the page
        outputPage = page.appendColumn(blockBuilder.build());
    }

    @Override
    public Page getOutput()
    {
        Page result = outputPage;
        outputPage = null;
        return result;
    }

    @Override
    public void close()
    {
        channelSet = null;
        // We don't want to release the supplier multiple times, since its reference counted
        if (closed) {
            return;
        }
        closed = true;
        // `afterClose` must be run last.
        afterClose.run();
    }
}
