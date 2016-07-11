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

import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class RowNumberOperator
        implements Operator
{
    public static class RowNumberOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Optional<Integer> maxRowsPerPartition;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<Integer> partitionChannels;
        private final List<Type> partitionTypes;
        private final Optional<Integer> hashChannel;
        private final int expectedPositions;
        private final List<Type> types;
        private boolean closed;

        public RowNumberOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> partitionChannels,
                List<? extends Type> partitionTypes,
                Optional<Integer> maxRowsPerPartition,
                Optional<Integer> hashChannel,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
            this.partitionTypes = ImmutableList.copyOf(requireNonNull(partitionTypes, "partitionTypes is null"));
            this.maxRowsPerPartition = requireNonNull(maxRowsPerPartition, "maxRowsPerPartition is null");

            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            checkArgument(expectedPositions > 0, "expectedPositions < 0");
            this.expectedPositions = expectedPositions;
            this.types = toTypes(sourceTypes, outputChannels);
        }

        @Override
        public List<Type> getTypes()
        {
            return types;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, RowNumberOperator.class.getSimpleName());
            return new RowNumberOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    partitionChannels,
                    partitionTypes,
                    maxRowsPerPartition,
                    hashChannel,
                    expectedPositions);
        }

        @Override
        public void close()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new RowNumberOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, partitionChannels, partitionTypes, maxRowsPerPartition, hashChannel, expectedPositions);
        }
    }

    private final OperatorContext operatorContext;
    private boolean finishing;

    private final int[] outputChannels;
    private final List<Type> types;

    private GroupByIdBlock partitionIds;
    private final Optional<GroupByHash> groupByHash;

    private Page inputPage;
    private final LongBigArray partitionRowCount;
    private final Optional<Integer> maxRowsPerPartition;

    public RowNumberOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            List<Integer> partitionChannels,
            List<Type> partitionTypes,
            Optional<Integer> maxRowsPerPartition,
            Optional<Integer> hashChannel,
            int expectedPositions)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(outputChannels);
        this.maxRowsPerPartition = maxRowsPerPartition;

        this.partitionRowCount = new LongBigArray(0);
        if (partitionChannels.isEmpty()) {
            this.groupByHash = Optional.empty();
        }
        else {
            int[] channels = Ints.toArray(partitionChannels);
            this.groupByHash = Optional.of(createGroupByHash(operatorContext.getSession(), partitionTypes, channels, hashChannel, expectedPositions));
        }
        this.types = toTypes(sourceTypes, outputChannels);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        if (isSinglePartition() && maxRowsPerPartition.isPresent()) {
            if (finishing && inputPage == null) {
                return true;
            }
            return partitionRowCount.get(0) == maxRowsPerPartition.get();
        }

        return finishing && inputPage == null;
    }

    @Override
    public boolean needsInput()
    {
        if (isSinglePartition() && maxRowsPerPartition.isPresent()) {
            // Check if single partition is done
            return partitionRowCount.get(0) < maxRowsPerPartition.get() && !finishing && inputPage == null;
        }
        return !finishing && inputPage == null;
    }

    private long getEstimatedByteSize()
    {
        return groupByHash.map(GroupByHash::getEstimatedSize).orElse(0L) + partitionRowCount.sizeOf();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(inputPage == null);
        inputPage = page;
        if (groupByHash.isPresent()) {
            partitionIds = groupByHash.get().getGroupIds(inputPage);
            partitionRowCount.ensureCapacity(partitionIds.getGroupCount());
        }
        operatorContext.setMemoryReservation(getEstimatedByteSize());
    }

    @Override
    public Page getOutput()
    {
        if (inputPage == null) {
            return null;
        }

        Page outputPage;
        if (maxRowsPerPartition.isPresent()) {
            outputPage = getSelectedRows();
        }
        else {
            outputPage = getRowsWithRowNumber();
        }

        inputPage = null;
        operatorContext.setMemoryReservation(getEstimatedByteSize());
        return outputPage;
    }

    private boolean isSinglePartition()
    {
        return !groupByHash.isPresent();
    }

    private Page getRowsWithRowNumber()
    {
        Block rowNumberBlock = createRowNumberBlock();
        Block[] sourceBlocks = new Block[inputPage.getChannelCount()];
        for (int i = 0; i < outputChannels.length; i++) {
            sourceBlocks[i] = inputPage.getBlock(outputChannels[i]);
        }

        Block[] outputBlocks = Arrays.copyOf(sourceBlocks, sourceBlocks.length + 1); // +1 for the row number column
        outputBlocks[sourceBlocks.length] = rowNumberBlock;

        return new Page(inputPage.getPositionCount(), outputBlocks);
    }

    private Block createRowNumberBlock()
    {
        BlockBuilder rowNumberBlock = BIGINT.createFixedSizeBlockBuilder(inputPage.getPositionCount());
        for (int currentPosition = 0; currentPosition < inputPage.getPositionCount(); currentPosition++) {
            long partitionId = getPartitionId(currentPosition);
            long nextRowCount = partitionRowCount.get(partitionId) + 1;
            BIGINT.writeLong(rowNumberBlock, nextRowCount);
            partitionRowCount.set(partitionId, nextRowCount);
        }
        return rowNumberBlock.build();
    }

    private Page getSelectedRows()
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        int rowNumberChannel = types.size() - 1;

        for (int currentPosition = 0; currentPosition < inputPage.getPositionCount(); currentPosition++) {
            long partitionId = getPartitionId(currentPosition);
            long rowCount = partitionRowCount.get(partitionId);
            if (rowCount == maxRowsPerPartition.get()) {
                continue;
            }
            pageBuilder.declarePosition();
            for (int i = 0; i < outputChannels.length; i++) {
                int channel = outputChannels[i];
                Type type = types.get(i);
                type.appendTo(inputPage.getBlock(channel), currentPosition, pageBuilder.getBlockBuilder(i));
            }
            BIGINT.writeLong(pageBuilder.getBlockBuilder(rowNumberChannel), rowCount + 1);
            partitionRowCount.set(partitionId, rowCount + 1);
        }
        if (pageBuilder.isEmpty()) {
            return null;
        }
        return pageBuilder.build();
    }

    private long getPartitionId(int position)
    {
        return isSinglePartition() ? 0 : partitionIds.getGroupId(position);
    }

    private static List<Type> toTypes(List<? extends Type> sourceTypes, List<Integer> outputChannels)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int channel : outputChannels) {
            types.add(sourceTypes.get(channel));
        }
        types.add(BIGINT);
        return types.build();
    }
}
