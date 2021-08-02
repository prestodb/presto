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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.SetBuilderOperator.SetSupplier;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class HashSemiJoinOperator
        implements Operator
{
    private final OperatorContext operatorContext;

    public static class HashSemiJoinOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final SetSupplier setSupplier;
        private final List<Type> probeTypes;
        private final int probeJoinChannel;
        private boolean closed;

        public HashSemiJoinOperatorFactory(int operatorId, PlanNodeId planNodeId, SetSupplier setSupplier, List<? extends Type> probeTypes, int probeJoinChannel)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.setSupplier = setSupplier;
            this.probeTypes = ImmutableList.copyOf(probeTypes);
            checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");
            this.probeJoinChannel = probeJoinChannel;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashSemiJoinOperator.class.getSimpleName());
            return new HashSemiJoinOperator(operatorContext, setSupplier, probeJoinChannel);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashSemiJoinOperatorFactory(operatorId, planNodeId, setSupplier, probeTypes, probeJoinChannel);
        }
    }

    private final int probeJoinChannel;
    private final ListenableFuture<ChannelSet> channelSetFuture;

    private ChannelSet channelSet;
    private Page outputPage;
    private boolean finishing;

    public HashSemiJoinOperator(OperatorContext operatorContext, SetSupplier channelSetFuture, int probeJoinChannel)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");

        // todo pass in desired projection
        requireNonNull(channelSetFuture, "hashProvider is null");
        checkArgument(probeJoinChannel >= 0, "probeJoinChannel is negative");

        this.channelSetFuture = channelSetFuture.getChannelSet();
        this.probeJoinChannel = probeJoinChannel;
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
        return finishing && outputPage == null;
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
        // use an effectively-final local variable instead of the non-final instance field inside of the loop
        ChannelSet channelSet = this.channelSet;
        checkState(channelSet != null, "Set has not been built yet");
        checkState(outputPage == null, "Operator still has pending output");

        // create the block builder for the new boolean column
        // we know the exact size required for the block
        BlockBuilder blockBuilder = BOOLEAN.createFixedSizeBlockBuilder(page.getPositionCount());

        Page probeJoinPage = page.getLoadedPage(probeJoinChannel);
        Block probeJoinNulls = probeJoinPage.getBlock(0).mayHaveNull() ? probeJoinPage.getBlock(0) : null;

        // update hashing strategy to use probe cursor
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (probeJoinNulls != null && probeJoinNulls.isNull(position)) {
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
}
