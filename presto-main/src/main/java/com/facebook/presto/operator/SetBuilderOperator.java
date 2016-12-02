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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class SetBuilderOperator
        implements Operator
{
    public static class SetSupplier
    {
        private final Type type;
        private final SettableFuture<ChannelSet> channelSetFuture = SettableFuture.create();

        public SetSupplier(Type type)
        {
            this.type = requireNonNull(type, "type is null");
        }

        public Type getType()
        {
            return type;
        }

        public ListenableFuture<ChannelSet> getChannelSet()
        {
            return channelSetFuture;
        }

        void setChannelSet(ChannelSet channelSet)
        {
            boolean wasSet = channelSetFuture.set(requireNonNull(channelSet, "channelSet is null"));
            checkState(wasSet, "ChannelSet already set");
        }
    }

    public static class SetBuilderOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Optional<Integer> hashChannel;
        private final SetSupplier setProvider;
        private final int setChannel;
        private final Optional<MethodHandle> equalFunction;
        private final Optional<Integer> nullChannel;
        private final int expectedPositions;
        private boolean closed;

        public SetBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Type type,
                int setChannel,
                Optional<MethodHandle> equalFunction,
                Optional<Integer> nullChannel,
                Optional<Integer> hashChannel,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            Preconditions.checkArgument(setChannel >= 0, "setChannel is negative");
            this.setProvider = new SetSupplier(requireNonNull(type, "type is null"));
            this.setChannel = setChannel;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.equalFunction = requireNonNull(equalFunction, "equalFunction is null");
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.expectedPositions = expectedPositions;
        }

        public SetSupplier getSetProvider()
        {
            return setProvider;
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, SetBuilderOperator.class.getSimpleName());
            return new SetBuilderOperator(operatorContext, setProvider, setChannel, hashChannel, nullChannel, equalFunction, expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new SetBuilderOperatorFactory(operatorId, planNodeId, setProvider.getType(), setChannel, equalFunction, nullChannel, hashChannel, expectedPositions);
        }
    }

    private final OperatorContext operatorContext;
    private final SetSupplier setSupplier;
    private final int setChannel;
    private final Optional<Integer> nullChannel;
    private final Optional<Integer> hashChannel;

    private final ChannelSetBuilder channelSetBuilder;

    private boolean finished;

    public SetBuilderOperator(
            OperatorContext operatorContext,
            SetSupplier setSupplier,
            int setChannel,
            Optional<Integer> hashChannel,
            Optional<Integer> nullChannel,
            Optional<MethodHandle> equalFunction,
            int expectedPositions)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.setSupplier = requireNonNull(setSupplier, "setProvider is null");
        this.setChannel = setChannel;

        this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");

        requireNonNull(equalFunction, "equalFunction is null");

        // Set builder is has a single channel which goes in channel 0
        int position = 0;

        // If null channel is present, then it goes channel 1
        Optional<Integer> indeterminateChannel = nullChannel.isPresent() ? Optional.of(++position) : Optional.empty();

        // Hashblock goes to the last channel
        Optional<Integer> channelSetHashChannel = hashChannel.isPresent() ? Optional.of(++position) : Optional.empty();

        this.channelSetBuilder = new ChannelSetBuilder(
                setSupplier.getType(),
                channelSetHashChannel,
                indeterminateChannel,
                equalFunction,
                expectedPositions,
                requireNonNull(operatorContext, "operatorContext is null"));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.of();
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }

        ChannelSet channelSet = channelSetBuilder.build();
        setSupplier.setChannelSet(channelSet);
        operatorContext.recordGeneratedOutput(channelSet.getEstimatedSizeInBytes(), channelSet.size());
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return !finished;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(!isFinished(), "Operator is already finished");

        Block sourceBlock = page.getBlock(setChannel);
        List<Block> blocks = new ArrayList<>();
        blocks.add(sourceBlock);
        if (nullChannel.isPresent()) {
            blocks.add(page.getBlock(nullChannel.get()));
        }
        if (hashChannel.isPresent()) {
            blocks.add(page.getBlock(hashChannel.get()));
        }
        Page sourcePage = new Page(blocks.toArray(new Block[blocks.size()]));
        channelSetBuilder.addPage(sourcePage);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
