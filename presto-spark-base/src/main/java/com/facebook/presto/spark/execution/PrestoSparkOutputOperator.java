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
import com.facebook.presto.spark.classloader_interface.PrestoSparkRow;
import com.facebook.presto.spark.execution.PrestoSparkRowBuffer.BufferedRows;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.sizeOfObjectArray;
import static java.lang.Integer.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class PrestoSparkOutputOperator
        implements Operator
{
    private static final int BATCH_SIZE = 1024 * 1024;
    private static final int EXPECTED_ROWS_COUNT_PER_BATCH = 10000;

    public static class PrestoSparkOutputFactory
            implements OutputFactory
    {
        private static final OutputPartitioning SINGLE_PARTITION = new OutputPartitioning(
                new ConstantPartitionFunction(),
                ImmutableList.of(),
                ImmutableList.of(),
                false,
                OptionalInt.empty());

        private final PrestoSparkRowBuffer rowBuffer;

        public PrestoSparkOutputFactory(PrestoSparkRowBuffer rowBuffer)
        {
            this.rowBuffer = requireNonNull(rowBuffer, "rowBuffer is null");
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
            OutputPartitioning partitioning = outputPartitioning.orElse(SINGLE_PARTITION);
            return new PrestoSparkOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    rowBuffer,
                    pagePreprocessor,
                    partitioning.getPartitionFunction(),
                    partitioning.getPartitionChannels(),
                    partitioning.getPartitionConstants().stream()
                            .map(constant -> constant.map(ConstantExpression::getValueBlock))
                            .collect(toImmutableList()),
                    partitioning.isReplicateNullsAndAny(),
                    partitioning.getNullChannel());
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

    public static class PrestoSparkOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PrestoSparkRowBuffer rowBuffer;
        private final Function<Page, Page> pagePreprocessor;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<Block>> partitionConstants;
        private final boolean replicateNullsAndAny;
        private final OptionalInt nullChannel;

        public PrestoSparkOutputOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PrestoSparkRowBuffer rowBuffer,
                Function<Page, Page> pagePreprocessor,
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<Block>> partitionConstants,
                boolean replicateNullsAndAny,
                OptionalInt nullChannel)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.rowBuffer = requireNonNull(rowBuffer, "rowBuffer is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicateNullsAndAny = replicateNullsAndAny;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PrestoSparkOutputOperator.class.getSimpleName());
            return new PrestoSparkOutputOperator(
                    operatorContext,
                    operatorContext.newLocalSystemMemoryContext(PrestoSparkOutputOperator.class.getSimpleName()),
                    rowBuffer,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicateNullsAndAny,
                    nullChannel);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PrestoSparkOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    rowBuffer,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicateNullsAndAny,
                    nullChannel);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final PrestoSparkRowBuffer rowBuffer;
    private final Function<Page, Page> pagePreprocessor;
    private final PartitionFunction partitionFunction;
    private final List<Integer> partitionChannels;
    private final List<Optional<Block>> partitionConstants;
    private final boolean replicateNullsAndAny;
    private final OptionalInt nullChannel;

    private ImmutableList.Builder<PrestoSparkRow> currentBatch;
    private long currentBatchSize;

    private boolean finished;
    private boolean hasAnyRowBeenReplicated;

    public PrestoSparkOutputOperator(
            OperatorContext operatorContext,
            LocalMemoryContext systemMemoryContext,
            PrestoSparkRowBuffer rowBuffer,
            Function<Page, Page> pagePreprocessor,
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<Block>> partitionConstants,
            boolean replicateNullsAndAny,
            OptionalInt nullChannel)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.rowBuffer = requireNonNull(rowBuffer, "rowBuffer is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
        this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
        this.partitionConstants = ImmutableList.copyOf(requireNonNull(partitionConstants, "partitionConstants is null"));
        this.replicateNullsAndAny = replicateNullsAndAny;
        this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return rowBuffer.isFull();
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
        int channelCount = page.getChannelCount();
        int averageRowSizeInBytes = min(toIntExact(page.getLogicalSizeInBytes() / positionCount), 10);
        Page partitionFunctionArguments = getPartitionFunctionArguments(page);
        for (int position = 0; position < positionCount; position++) {
            SliceOutput output = new DynamicSliceOutput(averageRowSizeInBytes * 2);
            for (int channel = 0; channel < channelCount; channel++) {
                Block block = page.getBlock(channel);
                block.writePositionTo(position, output);
            }

            boolean shouldReplicate = (replicateNullsAndAny && !hasAnyRowBeenReplicated) ||
                    nullChannel.isPresent() && page.getBlock(nullChannel.getAsInt()).isNull(position);
            byte[] rowBytes = output.size() == 0 ? new byte[0] : output.getUnderlyingSlice().byteArray();
            if (shouldReplicate) {
                for (int i = 0; i < partitionFunction.getPartitionCount(); i++) {
                    appendRow(new PrestoSparkRow(i, output.size(), rowBytes));
                }
                hasAnyRowBeenReplicated = true;
            }
            else {
                int partition = getPartition(partitionFunctionArguments, position);
                appendRow(new PrestoSparkRow(partition, output.size(), rowBytes));
            }
        }
        updateMemoryContext();
    }

    private void appendRow(PrestoSparkRow row)
    {
        long rowSize = row.getRetainedSize();
        if (currentBatchSize + rowSize > BATCH_SIZE) {
            flush();
        }
        if (currentBatch == null) {
            currentBatch = ImmutableList.builderWithExpectedSize(EXPECTED_ROWS_COUNT_PER_BATCH);
        }
        currentBatch.add(row);
        currentBatchSize += rowSize;
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
        flush();
        updateMemoryContext();
        finished = true;
    }

    private void flush()
    {
        if (currentBatchSize > 0) {
            verify(currentBatch != null);
            // Uses currentBatch internally. Must be called before currentBatch is set to null.
            int rowsListRetainedSize = getCurrentBatchRetainedBytes();
            List<PrestoSparkRow> rowsList = currentBatch.build();
            BufferedRows bufferedRows = new BufferedRows(rowsList, rowsListRetainedSize);
            rowBuffer.enqueue(bufferedRows);
            currentBatch = null;
            currentBatchSize = 0;
        }
    }

    private void updateMemoryContext()
    {
        systemMemoryContext.setBytes(getCurrentBatchRetainedBytes());
    }

    private int getCurrentBatchRetainedBytes()
    {
        if (currentBatch != null) {
            return toIntExact(currentBatchSize + sizeOfObjectArray(EXPECTED_ROWS_COUNT_PER_BATCH));
        }
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }
}
