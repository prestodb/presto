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
package com.facebook.presto.operator.repartition;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.facebook.presto.common.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputOperator
        implements Operator
{
    public static class PartitionedOutputFactory
            implements OutputFactory
    {
        private final OutputBuffer outputBuffer;
        private final DataSize maxMemory;

        public PartitionedOutputFactory(OutputBuffer outputBuffer, DataSize maxMemory)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
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
            checkArgument(outputPartitioning.isPresent(), "outputPartitioning is not present");
            return new PartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    types,
                    pagePreprocessor,
                    outputPartitioning.get().getPartitionFunction(),
                    outputPartitioning.get().getPartitionChannels(),
                    outputPartitioning.get().getPartitionConstants(),
                    outputPartitioning.get().isReplicateNullsAndAny(),
                    outputPartitioning.get().getNullChannel(),
                    outputBuffer,
                    serdeFactory,
                    maxMemory);
        }
    }

    public static class PartitionedOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final Function<Page, Page> pagePreprocessor;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<ConstantExpression>> partitionConstants;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final OutputBuffer outputBuffer;
        private final PagesSerdeFactory serdeFactory;
        private final DataSize maxMemory;

        public PartitionedOutputOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> sourceTypes,
                Function<Page, Page> pagePreprocessor,
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<ConstantExpression>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                PagesSerdeFactory serdeFactory,
                DataSize maxMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PartitionedOutputOperator.class.getSimpleName());
            return new PartitionedOutputOperator(
                    operatorContext,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    serdeFactory,
                    maxMemory);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new PartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    serdeFactory,
                    maxMemory);
        }
    }

    private final OperatorContext operatorContext;
    private final Function<Page, Page> pagePreprocessor;
    private final PagePartitioner partitionFunction;
    private ListenableFuture<?> isBlocked = NOT_BLOCKED;
    private boolean finished;

    public PartitionedOutputOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            Function<Page, Page> pagePreprocessor,
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<ConstantExpression>> partitionConstants,
            boolean replicatesAnyRow,
            OptionalInt nullChannel,
            OutputBuffer outputBuffer,
            PagesSerdeFactory serdeFactory,
            DataSize maxMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.partitionFunction = new PagePartitioner(
                partitionFunction,
                partitionChannels,
                partitionConstants,
                replicatesAnyRow,
                nullChannel,
                outputBuffer,
                serdeFactory,
                sourceTypes,
                maxMemory,
                operatorContext);

        operatorContext.setInfoSupplier(this.partitionFunction.getPartitionedOutputInfoSupplier());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finished = true;
        partitionFunction.flush(true);
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        // Avoid re-synchronizing on the output buffer when operator is already blocked
        if (isBlocked.isDone()) {
            isBlocked = partitionFunction.isFull();
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
        requireNonNull(page, "page is null");

        if (page.getPositionCount() == 0) {
            return;
        }

        page = pagePreprocessor.apply(page);
        partitionFunction.partitionPage(page);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    @Override
    public void close()
    {
        partitionFunction.closeMemoryContext();
    }

    private static class PagePartitioner
    {
        private final OutputBuffer outputBuffer;
        private final Type[] sourceTypes;
        private final PartitionFunction partitionFunction;
        private final int[] partitionChannels;
        @Nullable
        private final Block[] partitionConstantBlocks; // when null, no constants are present. Only non-null elements are constants
        private final PagesSerde serde;
        private final PageBuilder[] pageBuilders;
        private final boolean replicatesAnyRow;
        private final int nullChannel; // when >= 0, send the position to every partition if this channel is null
        private final AtomicLong rowsAdded = new AtomicLong();
        private final AtomicLong pagesAdded = new AtomicLong();
        private boolean hasAnyRowBeenReplicated;
        private final OperatorContext operatorContext;
        private final LocalMemoryContext systemMemoryContext;

        public PagePartitioner(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<ConstantExpression>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                PagesSerdeFactory serdeFactory,
                List<Type> sourceTypes,
                DataSize maxMemory,
                OperatorContext operatorContext)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = Ints.toArray(requireNonNull(partitionChannels, "partitionChannels is null"));
            Block[] partitionConstantBlocks = requireNonNull(partitionConstants, "partitionConstants is null").stream()
                    .map(constant -> constant.map(ConstantExpression::getValueBlock).orElse(null))
                    .toArray(Block[]::new);
            if (Arrays.stream(partitionConstantBlocks).anyMatch(Objects::nonNull)) {
                this.partitionConstantBlocks = partitionConstantBlocks;
            }
            else {
                this.partitionConstantBlocks = null;
            }
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null").orElse(-1);
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null").toArray(new Type[0]);
            this.serde = requireNonNull(serdeFactory, "serdeFactory is null").createPagesSerde();
            this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
            this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(PartitionedOutputOperator.class.getSimpleName());
            this.systemMemoryContext.setBytes(getRetainedSizeInBytes());

            //  Ensure partition channels align with constant arguments provided
            for (int i = 0; i < this.partitionChannels.length; i++) {
                if (this.partitionChannels[i] < 0) {
                    checkArgument(this.partitionConstantBlocks != null && this.partitionConstantBlocks[i] != null,
                            "Expected constant for partitioning channel %s, but none was found", i);
                }
            }

            int partitionCount = partitionFunction.getPartitionCount();
            int pageSize = min(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, ((int) maxMemory.toBytes()) / partitionCount);
            pageSize = max(1, pageSize);

            this.pageBuilders = new PageBuilder[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                pageBuilders[i] = PageBuilder.withMaxPageSize(pageSize, sourceTypes);
            }
        }

        public void closeMemoryContext()
        {
            systemMemoryContext.close();
        }

        public ListenableFuture<?> isFull()
        {
            return outputBuffer.isFull();
        }

        public long getSizeInBytes()
        {
            // We use a foreach loop instead of streams
            // as it has much better performance.
            long sizeInBytes = serde.getSizeInBytes();
            if (pageBuilders != null) {
                for (PageBuilder pageBuilder : pageBuilders) {
                    sizeInBytes += pageBuilder.getSizeInBytes();
                }
            }
            return sizeInBytes;
        }

        /**
         * This method can be expensive for complex types.
         */
        public long getRetainedSizeInBytes()
        {
            long sizeInBytes = serde.getRetainedSizeInBytes();
            if (pageBuilders != null) {
                for (PageBuilder pageBuilder : pageBuilders) {
                    sizeInBytes += pageBuilder.getRetainedSizeInBytes();
                }
            }
            return sizeInBytes;
        }

        public Supplier<PartitionedOutputInfo> getPartitionedOutputInfoSupplier()
        {
            // Must be a separate static method to avoid embedding references to "this" in the supplier
            return PartitionedOutputInfo.createPartitionedOutputInfoSupplier(rowsAdded, pagesAdded, outputBuffer);
        }

        public void partitionPage(Page page)
        {
            requireNonNull(page, "page is null");
            if (page.getPositionCount() == 0) {
                return;
            }

            int position;
            // Handle "any row" replication outside of the inner loop processing
            if (replicatesAnyRow && !hasAnyRowBeenReplicated) {
                for (PageBuilder pageBuilder : pageBuilders) {
                    appendRow(pageBuilder, page, 0);
                }
                hasAnyRowBeenReplicated = true;
                position = 1;
            }
            else {
                position = 0;
            }

            Page partitionFunctionArgs = getPartitionFunctionArguments(page);
            // Skip null block checks if mayHaveNull reports that no positions will be null
            if (nullChannel >= 0 && page.getBlock(nullChannel).mayHaveNull()) {
                Block nullsBlock = page.getBlock(nullChannel);
                for (; position < page.getPositionCount(); position++) {
                    if (nullsBlock.isNull(position)) {
                        for (PageBuilder pageBuilder : pageBuilders) {
                            appendRow(pageBuilder, page, position);
                        }
                    }
                    else {
                        int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                        appendRow(pageBuilders[partition], page, position);
                    }
                }
            }
            else {
                for (; position < page.getPositionCount(); position++) {
                    int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                    appendRow(pageBuilders[partition], page, position);
                }
            }

            // We track the memory before it's flushed to avoid under counting when the page size is large.
            systemMemoryContext.setBytes(getRetainedSizeInBytes());

            flush(false);
        }

        private Page getPartitionFunctionArguments(Page page)
        {
            // Fast path for no constants
            if (partitionConstantBlocks == null) {
                return page.extractChannels(partitionChannels);
            }

            Block[] blocks = new Block[partitionChannels.length];
            for (int i = 0; i < blocks.length; i++) {
                int channel = partitionChannels[i];
                if (channel < 0) {
                    blocks[i] = new RunLengthEncodedBlock(partitionConstantBlocks[i], page.getPositionCount());
                }
                else {
                    blocks[i] = page.getBlock(channel);
                }
            }
            return new Page(page.getPositionCount(), blocks);
        }

        private void appendRow(PageBuilder pageBuilder, Page page, int position)
        {
            pageBuilder.declarePosition();

            for (int channel = 0; channel < sourceTypes.length; channel++) {
                Type type = sourceTypes[channel];
                type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
            }
        }

        public void flush(boolean force)
        {
            // add all full pages to output buffer
            for (int partition = 0; partition < pageBuilders.length; partition++) {
                PageBuilder partitionPageBuilder = pageBuilders[partition];
                if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                    Page pagePartition = partitionPageBuilder.build();
                    partitionPageBuilder.reset();

                    operatorContext.recordOutput(pagePartition.getSizeInBytes(), pagePartition.getPositionCount());

                    outputBuffer.enqueue(operatorContext.getDriverContext().getLifespan(), partition, splitAndSerializePage(pagePartition));
                    pagesAdded.incrementAndGet();
                    rowsAdded.addAndGet(pagePartition.getPositionCount());
                }
            }
        }

        private List<SerializedPage> splitAndSerializePage(Page pagePartition)
        {
            List<Page> pagesFromSplitting = splitPage(pagePartition, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
            ImmutableList.Builder<SerializedPage> builder = ImmutableList.builderWithExpectedSize(pagesFromSplitting.size());
            for (Page p : pagesFromSplitting) {
                builder.add(serde.serialize(p));
            }
            return builder.build();
        }
    }
}
