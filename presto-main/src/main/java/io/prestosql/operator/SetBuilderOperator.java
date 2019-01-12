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
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.operator.ChannelSet.ChannelSetBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

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
        private final int expectedPositions;
        private boolean closed;
        private final JoinCompiler joinCompiler;

        public SetBuilderOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                Type type,
                int setChannel,
                Optional<Integer> hashChannel,
                int expectedPositions,
                JoinCompiler joinCompiler)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            Preconditions.checkArgument(setChannel >= 0, "setChannel is negative");
            this.setProvider = new SetSupplier(requireNonNull(type, "type is null"));
            this.setChannel = setChannel;
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.expectedPositions = expectedPositions;
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        }

        public SetSupplier getSetProvider()
        {
            return setProvider;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, SetBuilderOperator.class.getSimpleName());
            return new SetBuilderOperator(operatorContext, setProvider, setChannel, hashChannel, expectedPositions, joinCompiler);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new SetBuilderOperatorFactory(operatorId, planNodeId, setProvider.getType(), setChannel, hashChannel, expectedPositions, joinCompiler);
        }
    }

    private final OperatorContext operatorContext;
    private final SetSupplier setSupplier;
    private final int setChannel;
    private final Optional<Integer> hashChannel;

    private final ChannelSetBuilder channelSetBuilder;

    private boolean finished;

    @Nullable
    private Work<?> unfinishedWork;  // The pending work for current page.

    public SetBuilderOperator(
            OperatorContext operatorContext,
            SetSupplier setSupplier,
            int setChannel,
            Optional<Integer> hashChannel,
            int expectedPositions,
            JoinCompiler joinCompiler)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.setSupplier = requireNonNull(setSupplier, "setProvider is null");
        this.setChannel = setChannel;

        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        // Set builder is has a single channel which goes in channel 0, if hash is present, add a hachBlock to channel 1
        Optional<Integer> channelSetHashChannel = hashChannel.isPresent() ? Optional.of(1) : Optional.empty();
        this.channelSetBuilder = new ChannelSetBuilder(
                setSupplier.getType(),
                channelSetHashChannel,
                expectedPositions,
                requireNonNull(operatorContext, "operatorContext is null"),
                requireNonNull(joinCompiler, "joinCompiler is null"));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (finished) {
            return;
        }

        ChannelSet channelSet = channelSetBuilder.build();
        setSupplier.setChannelSet(channelSet);
        operatorContext.recordOutput(channelSet.getEstimatedSizeInBytes(), channelSet.size());
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
        // Since SetBuilderOperator doesn't produce any output, the getOutput()
        // method may never be called. We need to handle any unfinished work
        // before addInput() can be called again.
        return !finished && (unfinishedWork == null || processUnfinishedWork());
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
