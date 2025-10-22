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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.getDone;
import static com.facebook.presto.spi.plan.SpatialJoinNode.SpatialJoinType.INNER;
import static com.facebook.presto.spi.plan.SpatialJoinNode.SpatialJoinType.LEFT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class SpatialJoinOperator
        implements Operator
{
    public static final class SpatialJoinOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final SpatialJoinNode.SpatialJoinType joinType;
        private final List<Type> probeTypes;
        private final List<Integer> probeOutputChannels;
        private final int probeGeometryChannel;
        private final Optional<Integer> partitionChannel;
        private final PagesSpatialIndexFactory pagesSpatialIndexFactory;

        private boolean closed;

        public SpatialJoinOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                SpatialJoinNode.SpatialJoinType joinType,
                List<Type> probeTypes,
                List<Integer> probeOutputChannels,
                int probeGeometryChannel,
                Optional<Integer> partitionChannel,
                PagesSpatialIndexFactory pagesSpatialIndexFactory)
        {
            checkArgument(joinType == INNER || joinType == LEFT, "unsupported join type: %s", joinType);
            this.operatorId = operatorId;
            this.planNodeId = planNodeId;
            this.joinType = joinType;
            this.probeTypes = ImmutableList.copyOf(probeTypes);
            this.probeOutputChannels = ImmutableList.copyOf(probeOutputChannels);
            this.probeGeometryChannel = probeGeometryChannel;
            this.partitionChannel = requireNonNull(partitionChannel, "partitionChannel is null");
            this.pagesSpatialIndexFactory = pagesSpatialIndexFactory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(
                    operatorId,
                    planNodeId,
                    SpatialJoinOperator.class.getSimpleName());
            return new SpatialJoinOperator(
                    operatorContext,
                    joinType,
                    probeTypes,
                    probeOutputChannels,
                    probeGeometryChannel,
                    partitionChannel,
                    pagesSpatialIndexFactory);
        }

        @Override
        public void noMoreOperators()
        {
            if (closed) {
                return;
            }

            pagesSpatialIndexFactory.noMoreProbeOperators();
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            checkState(!closed, "Factory is already closed");
            pagesSpatialIndexFactory.addProbeOperatorFactory();
            return new SpatialJoinOperatorFactory(operatorId, planNodeId, joinType, probeTypes, probeOutputChannels, probeGeometryChannel, partitionChannel, pagesSpatialIndexFactory);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final SpatialJoinNode.SpatialJoinType joinType;
    private final List<Type> probeTypes;
    private final List<Integer> probeOutputChannels;
    private final int probeGeometryChannel;
    private final Optional<Integer> partitionChannel;
    private final PagesSpatialIndexFactory pagesSpatialIndexFactory;

    private ListenableFuture<PagesSpatialIndex> pagesSpatialIndexFuture;
    private final PageBuilder pageBuilder;
    @Nullable
    private Page probe;

    // The following fields represent the state of the operator in case when processProbe yielded or
    // filled up pageBuilder before processing all records in a probe page.
    private int probePosition;
    @Nullable
    private int[] joinPositions;
    private int nextJoinPositionIndex;
    private boolean matchFound;

    private boolean finishing;
    private boolean finished;

    public SpatialJoinOperator(
            OperatorContext operatorContext,
            SpatialJoinNode.SpatialJoinType joinType,
            List<Type> probeTypes,
            List<Integer> probeOutputChannels,
            int probeGeometryChannel,
            Optional<Integer> partitionChannel,
            PagesSpatialIndexFactory pagesSpatialIndexFactory)
    {
        this.operatorContext = operatorContext;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.joinType = joinType;
        this.probeTypes = ImmutableList.copyOf(probeTypes);
        this.probeOutputChannels = ImmutableList.copyOf(probeOutputChannels);
        this.probeGeometryChannel = probeGeometryChannel;
        this.partitionChannel = requireNonNull(partitionChannel, "partitionChannel is null");
        this.pagesSpatialIndexFactory = pagesSpatialIndexFactory;
        this.pagesSpatialIndexFuture = pagesSpatialIndexFactory.createPagesSpatialIndex();
        this.pageBuilder = new PageBuilder(ImmutableList.<Type>builder()
                .addAll(probeOutputChannels.stream()
                        .map(probeTypes::get)
                        .iterator())
                .addAll(pagesSpatialIndexFactory.getOutputTypes())
                .build());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && pagesSpatialIndexFuture.isDone() && !pageBuilder.isFull() && probe == null;
    }

    @Override
    public void addInput(Page page)
    {
        verify(probe == null);
        probe = page;
        probePosition = 0;

        joinPositions = null;
    }

    @Override
    public Page getOutput()
    {
        verify(!finished);
        if (!pageBuilder.isFull() && probe != null) {
            processProbe();
        }

        if (pageBuilder.isFull()) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }

        if (finishing && probe == null) {
            Page page = null;
            if (!pageBuilder.isEmpty()) {
                page = pageBuilder.build();
                pageBuilder.reset();
            }
            pagesSpatialIndexFactory.probeOperatorFinished();
            pagesSpatialIndexFuture = null;
            finished = true;
            return page;
        }

        return null;
    }

    private void processProbe()
    {
        verify(probe != null);

        PagesSpatialIndex pagesSpatialIndex = getDone(pagesSpatialIndexFuture);
        DriverYieldSignal yieldSignal = operatorContext.getDriverContext().getYieldSignal();
        while (probePosition < probe.getPositionCount()) {
            if (joinPositions == null) {
                joinPositions = pagesSpatialIndex.findJoinPositions(probePosition, probe, probeGeometryChannel, partitionChannel);
                localUserMemoryContext.setBytes(sizeOf(joinPositions));
                nextJoinPositionIndex = 0;
                matchFound = false;
                if (yieldSignal.isSet()) {
                    return;
                }
            }

            while (nextJoinPositionIndex < joinPositions.length) {
                if (pageBuilder.isFull()) {
                    return;
                }

                int joinPosition = joinPositions[nextJoinPositionIndex];

                if (pagesSpatialIndex.isJoinPositionEligible(joinPosition, probePosition, probe)) {
                    pageBuilder.declarePosition();
                    appendProbe();
                    pagesSpatialIndex.appendTo(joinPosition, pageBuilder, probeOutputChannels.size());
                    matchFound = true;
                }

                nextJoinPositionIndex++;

                if (yieldSignal.isSet()) {
                    return;
                }
            }

            if (!matchFound && joinType == LEFT) {
                if (pageBuilder.isFull()) {
                    return;
                }

                pageBuilder.declarePosition();
                appendProbe();
                int buildColumnCount = pagesSpatialIndexFactory.getOutputTypes().size();
                for (int i = 0; i < buildColumnCount; i++) {
                    pageBuilder.getBlockBuilder(probeOutputChannels.size() + i).appendNull();
                }
            }

            joinPositions = null;
            localUserMemoryContext.setBytes(0);
            probePosition++;
        }

        this.probe = null;
        this.probePosition = 0;
    }

    private void appendProbe()
    {
        int outputChannelOffset = 0;
        for (int outputIndex : probeOutputChannels) {
            Type type = probeTypes.get(outputIndex);
            Block block = probe.getBlock(outputIndex);
            type.appendTo(block, probePosition, pageBuilder.getBlockBuilder(outputChannelOffset));
            outputChannelOffset++;
        }
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public void close()
    {
        pagesSpatialIndexFuture = null;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }
}
