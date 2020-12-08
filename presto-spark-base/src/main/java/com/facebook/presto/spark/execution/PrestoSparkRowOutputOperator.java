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
package com.facebook.presto.spark.execution;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spark.execution.PrestoSparkRowBatch.PrestoSparkRowBatchBuilder;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PrestoSparkRowOutputOperator
        implements Operator
{
    public static class PrestoSparkRowOutputFactory
            implements OutputFactory
    {
        private static final OutputPartitioning SINGLE_PARTITION = new OutputPartitioning(
                new ConstantPartitionFunction(),
                ImmutableList.of(),
                ImmutableList.of(),
                false,
                OptionalInt.empty());

        private final Optional<OutputPartitioning> preDeterminedPartition;

        private final PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer;
        private final DataSize targetAverageRowSize;

        public PrestoSparkRowOutputFactory(
                PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer,
                DataSize targetAverageRowSize,
                Optional<OutputPartitioning> preDeterminedPartition)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.targetAverageRowSize = requireNonNull(targetAverageRowSize, "targetAverageRowSize is null");
            this.preDeterminedPartition = requireNonNull(preDeterminedPartition, "preDeterminedPartition is null");
        }

        @Override
        public OperatorFactory createOutputOperator(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                Function<Page, Page> pagePreprocessor,
                Optional<OutputPartitioning> outputPartitioning,
                PagesSerdeFactory serdeFactory)
        {
            OutputPartitioning partitioning = outputPartitioning.orElse(preDeterminedPartition.orElse(SINGLE_PARTITION));
            return new PrestoSparkRowOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    outputBuffer,
                    pagePreprocessor,
                    partitioning.getPartitionFunction(),
                    partitioning.getPartitionChannels(),
                    partitioning.getPartitionConstants().stream()
                            .map(constant -> constant.map(ConstantExpression::getValueBlock))
                            .collect(toImmutableList()),
                    partitioning.isReplicateNullsAndAny(),
                    partitioning.getNullChannel(),
                    toIntExact(targetAverageRowSize.toBytes()));
        }
    }

    private static class ConstantPartitionFunction
            implements PartitionFunction
    {
        @Override
        public int getPartitionCount()
        {
            return 1;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            return 0;
        }
    }

    public static class PreDeterminedPartitionFunction
            implements PartitionFunction
    {
        private final int partitionId;
        private final int partitionCount;

        public PreDeterminedPartitionFunction(int partitionId, int partitionCount)
        {
            checkArgument(partitionId >= 0 && partitionId < partitionCount,
                    "partitionId should be non-negative and less than partitionCount");
            this.partitionId = partitionId;
            this.partitionCount = partitionCount;
        }

        @Override
        public int getPartitionCount()
        {
            return partitionCount;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            return partitionId;
        }
    }

    public static class PrestoSparkRowOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer;
        private final Function<Page, Page> pagePreprocessor;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<Block>> partitionConstants;
        private final boolean replicateNullsAndAny;
        private final OptionalInt nullChannel;
        private final int targetAverageRowSizeInBytes;

        public PrestoSparkRowOutputOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer,
                Function<Page, Page> pagePreprocessor,
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<Block>> partitionConstants,
                boolean replicateNullsAndAny,
                OptionalInt nullChannel,
                int targetAverageRowSizeInBytes)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicateNullsAndAny = replicateNullsAndAny;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.targetAverageRowSizeInBytes = targetAverageRowSizeInBytes;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PrestoSparkRowOutputOperator.class.getSimpleName());
            return new PrestoSparkRowOutputOperator(
                    operatorContext,
                    operatorContext.newLocalSystemMemoryContext(PrestoSparkRowOutputOperator.class.getSimpleName()),
                    outputBuffer,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicateNullsAndAny,
                    nullChannel,
                    targetAverageRowSizeInBytes);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PrestoSparkRowOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    outputBuffer,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicateNullsAndAny,
                    nullChannel,
                    targetAverageRowSizeInBytes);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer;
    private final Function<Page, Page> pagePreprocessor;
    private final PartitionFunction partitionFunction;
    private final List<Integer> partitionChannels;
    private final List<Optional<Block>> partitionConstants;
    private final boolean replicateNullsAndAny;
    private final OptionalInt nullChannel;
    private final int targetAverageRowSizeInBytes;

    private PrestoSparkRowBatchBuilder rowBatchBuilder;

    private ListenableFuture<?> isBlocked = NOT_BLOCKED;
    private boolean finished;
    private boolean hasAnyRowBeenReplicated;

    public PrestoSparkRowOutputOperator(
            OperatorContext operatorContext,
            LocalMemoryContext systemMemoryContext,
            PrestoSparkOutputBuffer<PrestoSparkRowBatch> outputBuffer,
            Function<Page, Page> pagePreprocessor,
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<Block>> partitionConstants,
            boolean replicateNullsAndAny,
            OptionalInt nullChannel,
            int targetAverageRowSizeInBytes)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
        this.partitionConstants = ImmutableList.copyOf(requireNonNull(partitionConstants, "partitionConstants is null"));
        this.replicateNullsAndAny = replicateNullsAndAny;
        this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
        this.targetAverageRowSizeInBytes = targetAverageRowSizeInBytes;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (isBlocked.isDone()) {
            isBlocked = outputBuffer.isFull();
            if (isBlocked.isDone()) {
                isBlocked = NOT_BLOCKED;
            }
        }
        return isBlocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        page = pagePreprocessor.apply(page);

        int positionCount = page.getPositionCount();
        if (positionCount == 0) {
            return;
        }

        int partitionCount = partitionFunction.getPartitionCount();

        if (rowBatchBuilder == null) {
            rowBatchBuilder = PrestoSparkRowBatch.builder(partitionCount, targetAverageRowSizeInBytes);
        }

        int channelCount = page.getChannelCount();
        Page partitionFunctionArguments = getPartitionFunctionArguments(page);
        for (int position = 0; position < positionCount; position++) {
            if (rowBatchBuilder.isFull()) {
                outputBuffer.enqueue(rowBatchBuilder.build());
                rowBatchBuilder = PrestoSparkRowBatch.builder(partitionCount, targetAverageRowSizeInBytes);
            }

            SliceOutput output = rowBatchBuilder.beginRowEntry();
            for (int channel = 0; channel < channelCount; channel++) {
                Block block = page.getBlock(channel);
                block.writePositionTo(position, output);
            }
            boolean shouldReplicate = (replicateNullsAndAny && !hasAnyRowBeenReplicated) ||
                    nullChannel.isPresent() && page.getBlock(nullChannel.getAsInt()).isNull(position);
            if (shouldReplicate) {
                hasAnyRowBeenReplicated = true;
                rowBatchBuilder.closeEntryForReplicatedRow();
            }
            else {
                int partition = getPartition(partitionFunctionArguments, position);
                rowBatchBuilder.closeEntryForNonReplicatedRow(partition);
            }
        }
        updateMemoryContext();
    }

    private int getPartition(Page partitionFunctionArgs, int position)
    {
        return partitionFunction.getPartition(partitionFunctionArgs, position);
    }

    private Page getPartitionFunctionArguments(Page page)
    {
        Block[] blocks = new Block[partitionChannels.size()];
        for (int i = 0; i < blocks.length; i++) {
            Optional<Block> partitionConstant = partitionConstants.get(i);
            if (partitionConstant.isPresent()) {
                blocks[i] = new RunLengthEncodedBlock(partitionConstant.get(), page.getPositionCount());
            }
            else {
                blocks[i] = page.getBlock(partitionChannels.get(i));
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void finish()
    {
        if (rowBatchBuilder != null && !rowBatchBuilder.isEmpty()) {
            outputBuffer.enqueue(rowBatchBuilder.build());
            rowBatchBuilder = null;
        }
        updateMemoryContext();
        finished = true;
    }

    private void updateMemoryContext()
    {
        systemMemoryContext.setBytes(rowBatchBuilder == null ? 0 : rowBatchBuilder.getRetainedSizeInBytes());
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }
}
