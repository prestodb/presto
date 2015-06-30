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

import com.facebook.presto.operator.simple.ClusterBoundaryCallback;
import com.facebook.presto.operator.simple.OperatorBuilder;
import com.facebook.presto.operator.simple.ProcessorBase;
import com.facebook.presto.operator.simple.ProcessorInput;
import com.facebook.presto.operator.simple.ProcessorPageOutput;
import com.facebook.presto.operator.simple.SimpleOperator.ProcessorState;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.PageCompiler;
import com.facebook.presto.util.array.LongBigArray;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.operator.simple.SimpleOperator.ProcessorState.HAS_OUTPUT;
import static com.facebook.presto.operator.simple.SimpleOperator.ProcessorState.NEEDS_INPUT;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class RowNumberProcessor
        implements ProcessorBase, ProcessorInput<Page>, ProcessorPageOutput, ClusterBoundaryCallback
{
    public static class RowNumberOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final Optional<Integer> maxRowsPerPartition;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<Integer> partitionChannels;
        private final List<Type> partitionTypes;
        private final List<Integer> prePartitionedChannels;
        private final Optional<Integer> hashChannel;
        private final int expectedPositions;
        private final List<Type> types;
        private final DataSize minClusterCallbackSize;
        private boolean closed;

        public RowNumberOperatorFactory(
                int operatorId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> partitionChannels,
                List<? extends Type> partitionTypes,
                Optional<Integer> maxRowsPerPartition,
                List<Integer> prePartitionedChannels,
                Optional<Integer> hashChannel,
                int expectedPositions)
        {
            this(operatorId,
                    sourceTypes,
                    outputChannels,
                    partitionChannels,
                    partitionTypes,
                    maxRowsPerPartition,
                    prePartitionedChannels,
                    hashChannel,
                    new DataSize(PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES, DataSize.Unit.BYTE),
                    expectedPositions);
        }

        public RowNumberOperatorFactory(
                int operatorId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> partitionChannels,
                List<? extends Type> partitionTypes,
                Optional<Integer> maxRowsPerPartition,
                List<Integer> prePartitionedChannels,
                Optional<Integer> hashChannel,
                DataSize minClusterCallbackSize,
                int expectedPositions)
        {
            this.operatorId = operatorId;
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(checkNotNull(outputChannels, "outputChannels is null"));
            this.partitionChannels = ImmutableList.copyOf(checkNotNull(partitionChannels, "partitionChannels is null"));
            this.partitionTypes = ImmutableList.copyOf(checkNotNull(partitionTypes, "partitionTypes is null"));
            this.maxRowsPerPartition = checkNotNull(maxRowsPerPartition, "maxRowsPerPartition is null");
            this.prePartitionedChannels = ImmutableList.copyOf(checkNotNull(prePartitionedChannels, "prePartitionedChannels is null"));
            checkArgument(partitionChannels.containsAll(prePartitionedChannels), "prePartitionedChannels must be a subset of partitionChannels");

            this.hashChannel = checkNotNull(hashChannel, "hashChannel is null");
            checkArgument(expectedPositions > 0, "expectedPositions < 0");
            this.expectedPositions = expectedPositions;
            this.types = toTypes(sourceTypes, outputChannels);
            this.minClusterCallbackSize = checkNotNull(minClusterCallbackSize, "minClusterCallbackSize is null");
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

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, RowNumberProcessor.class.getSimpleName());
            RowNumberProcessor processor = new RowNumberProcessor(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    partitionChannels,
                    partitionTypes,
                    maxRowsPerPartition,
                    hashChannel,
                    expectedPositions);
            PagePositionEqualitor equalitor = PageCompiler.INSTANCE.compilePagePositionEqualitor(Lists.transform(prePartitionedChannels, sourceTypes::get), prePartitionedChannels);
            return OperatorBuilder.newOperatorBuilder(processor)
                    .bindInput(processor)
                    .bindOutput(processor)
                    .withClusterBoundaryCallback(equalitor, minClusterCallbackSize, processor)
                    .withMemoryTracking(processor::getEstimatedByteSize)
                    .build();
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;

    private final int[] outputChannels;
    private final List<Type> types;

    private GroupByIdBlock partitionIds;
    private final Supplier<Optional<GroupByHash>> groupByHashSupplier;
    private Optional<GroupByHash> groupByHash;

    private Page inputPage;
    private LongBigArray partitionRowCount;
    private final Optional<Integer> maxRowsPerPartition;

    public RowNumberProcessor(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            List<Integer> partitionChannels,
            List<Type> partitionTypes,
            Optional<Integer> maxRowsPerPartition,
            Optional<Integer> hashChannel,
            int expectedPositions)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(outputChannels);
        this.maxRowsPerPartition = maxRowsPerPartition;

        this.partitionRowCount = new LongBigArray(0);

        groupByHashSupplier = () -> {
            if (partitionChannels.isEmpty()) {
                return Optional.empty();
            }
            else {
                int[] channels = Ints.toArray(partitionChannels);
                return Optional.of(createGroupByHash(partitionTypes, channels, Optional.<Integer>empty(), hashChannel, expectedPositions));
            }
        };
        this.groupByHash = groupByHashSupplier.get();
        this.types = toTypes(sourceTypes, outputChannels);
    }

    @Override
    public String getName()
    {
        return RowNumberProcessor.class.getSimpleName();
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

    public long getEstimatedByteSize()
    {
        return groupByHash.map(GroupByHash::getEstimatedSize).orElse(0L) + partitionRowCount.sizeOf();
    }

    @Override
    public ProcessorState addInput(@Nullable Page page)
    {
        if (page == null) {
            return NEEDS_INPUT;
        }

        checkState(inputPage == null);
        inputPage = page;
        if (groupByHash.isPresent()) {
            partitionIds = groupByHash.get().getGroupIds(inputPage);
            partitionRowCount.ensureCapacity(partitionIds.getGroupCount());
        }
        return HAS_OUTPUT;
    }

    @Override
    public Page extractOutput()
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
        return outputPage;
    }

    @Override
    public ProcessorState clusterBoundary()
    {
        groupByHash = groupByHashSupplier.get();
        partitionRowCount = new LongBigArray(0);
        return NEEDS_INPUT;
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
                Type type = types.get(channel);
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
