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
import com.facebook.presto.common.block.ArrayAllocator;
import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockFlattener;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.MapBlock;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.UncheckedStackArrayAllocator;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.array.Arrays.ExpansionFactor.MEDIUM;
import static com.facebook.presto.array.Arrays.ExpansionFactor.SMALL;
import static com.facebook.presto.array.Arrays.ExpansionOption.INITIALIZE;
import static com.facebook.presto.array.Arrays.ExpansionOption.PRESERVE;
import static com.facebook.presto.array.Arrays.ensureCapacity;
import static com.facebook.presto.common.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.facebook.presto.operator.repartition.AbstractBlockEncodingBuffer.createBlockEncodingBuffers;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OptimizedPartitionedOutputOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final Function<Page, Page> pagePreprocessor;
    private final PagePartitioner pagePartitioner;
    private final LocalMemoryContext systemMemoryContext;
    private boolean finished;

    public OptimizedPartitionedOutputOperator(
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
        this.pagePartitioner = new PagePartitioner(
                partitionFunction,
                partitionChannels,
                partitionConstants,
                replicatesAnyRow,
                nullChannel,
                outputBuffer,
                serdeFactory,
                sourceTypes,
                maxMemory,
                operatorContext.getDriverContext().getLifespan());

        operatorContext.setInfoSupplier(this::getInfo);
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(PartitionedOutputOperator.class.getSimpleName());
        this.systemMemoryContext.setBytes(pagePartitioner.getRetainedSizeInBytes());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public PartitionedOutputInfo getInfo()
    {
        return pagePartitioner.getInfo();
    }

    @Override
    public void finish()
    {
        finished = true;
        pagePartitioner.flush();
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = pagePartitioner.isFull();
        return blocked.isDone() ? NOT_BLOCKED : blocked;
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
        pagePartitioner.partitionPage(page);

        // TODO: PartitionedOutputOperator reports incorrect output data size #11770
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());

        systemMemoryContext.setBytes(pagePartitioner.getRetainedSizeInBytes());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    /**
     * Flatten the block and convert the nested-typed block into ColumnarArray/Map/Row.
     * For performance considerations we decode the block only once for each block instead of for each batch.
     *
     * @return A tree structure that contains the decoded block
     */
    @VisibleForTesting
    static DecodedBlockNode decodeBlock(BlockFlattener flattener, Closer blockLeaseCloser, Block block)
    {
        BlockLease lease = flattener.flatten(block);
        blockLeaseCloser.register(lease::close);
        Block decodedBlock = lease.get();

        if (decodedBlock instanceof ArrayBlock) {
            ColumnarArray columnarArray = ColumnarArray.toColumnarArray(decodedBlock);
            return new DecodedBlockNode(columnarArray, ImmutableList.of(decodeBlock(flattener, blockLeaseCloser, columnarArray.getElementsBlock())));
        }

        if (decodedBlock instanceof MapBlock) {
            ColumnarMap columnarMap = ColumnarMap.toColumnarMap(decodedBlock);
            return new DecodedBlockNode(columnarMap, ImmutableList.of(decodeBlock(flattener, blockLeaseCloser, columnarMap.getKeysBlock()), decodeBlock(flattener, blockLeaseCloser, columnarMap.getValuesBlock())));
        }

        if (decodedBlock instanceof RowBlock) {
            ColumnarRow columnarRow = ColumnarRow.toColumnarRow(decodedBlock);
            ImmutableList.Builder<DecodedBlockNode> children = ImmutableList.builder();
            for (int i = 0; i < columnarRow.getFieldCount(); i++) {
                children.add(decodeBlock(flattener, blockLeaseCloser, columnarRow.getField(i)));
            }
            return new DecodedBlockNode(columnarRow, children.build());
        }

        if (decodedBlock instanceof DictionaryBlock) {
            return new DecodedBlockNode(decodedBlock, ImmutableList.of(decodeBlock(flattener, blockLeaseCloser, ((DictionaryBlock) decodedBlock).getDictionary())));
        }

        if (decodedBlock instanceof RunLengthEncodedBlock) {
            return new DecodedBlockNode(decodedBlock, ImmutableList.of(decodeBlock(flattener, blockLeaseCloser, ((RunLengthEncodedBlock) decodedBlock).getValue())));
        }

        return new DecodedBlockNode(decodedBlock, ImmutableList.of());
    }

    public static class OptimizedPartitionedOutputFactory
            implements OutputFactory
    {
        private final OutputBuffer outputBuffer;
        private final DataSize maxMemory;

        public OptimizedPartitionedOutputFactory(OutputBuffer outputBuffer, DataSize maxMemory)
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
            return new OptimizedPartitionedOutputOperatorFactory(
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

    public static class OptimizedPartitionedOutputOperatorFactory
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

        public OptimizedPartitionedOutputOperatorFactory(
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
            return new OptimizedPartitionedOutputOperator(
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
            return new OptimizedPartitionedOutputOperatorFactory(
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

    private static class PagePartitioner
    {
        private final OutputBuffer outputBuffer;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<Block>> partitionConstants;
        private final PagesSerde serde;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel; // when present, send the position to every partition if this channel is null.
        private final AtomicLong rowsAdded = new AtomicLong();
        private final AtomicLong pagesAdded = new AtomicLong();

        // The ArrayAllocator used by BlockFlattener for decoding blocks.
        // There could be queries that shuffles data with up to 1000 columns so we need to set the maxOutstandingArrays a high number.
        private final ArrayAllocator blockDecodingAllocator = new UncheckedStackArrayAllocator(500);
        private final BlockFlattener flattener = new BlockFlattener(blockDecodingAllocator);
        private final Closer blockLeaseCloser = Closer.create();

        // The ArrayAllocator for the buffers used in repartitioning, e.g. PartitionBuffer#serializedRowSizes, BlockEncodingBuffer#mappedPositions.
        private final ArrayAllocator bufferAllocator = new UncheckedStackArrayAllocator(2000);

        private final PartitionBuffer[] partitionBuffers;
        private final List<Type> sourceTypes;
        private final List<Integer> variableWidthChannels;
        private final int fixedWidthRowSize;
        private final DecodedBlockNode[] decodedBlocks;

        private boolean hasAnyRowBeenReplicated;

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
                Lifespan lifespan)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "pagePartitioner is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null").stream()
                    .map(constant -> constant.map(ConstantExpression::getValueBlock))
                    .collect(toImmutableList());
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.serde = requireNonNull(serdeFactory, "serdeFactory is null").createPagesSerde();

            int partitionCount = partitionFunction.getPartitionCount();

            int partitionBufferCapacity = max(1, min(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, toIntExact(maxMemory.toBytes()) / partitionCount));

            partitionBuffers = new PartitionBuffer[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                partitionBuffers[i] = new PartitionBuffer(i, sourceTypes.size(), partitionBufferCapacity, pagesAdded, rowsAdded, serde, lifespan, bufferAllocator);
            }

            this.sourceTypes = sourceTypes;
            decodedBlocks = new DecodedBlockNode[sourceTypes.size()];

            ImmutableList.Builder<Integer> variableWidthChannels = ImmutableList.builder();
            int fixedWidthRowSize = 0;
            for (int i = 0; i < sourceTypes.size(); i++) {
                int bytesPerPosition = getFixedWidthTypeSize(sourceTypes.get(i));
                fixedWidthRowSize += bytesPerPosition;

                if (bytesPerPosition == 0) {
                    variableWidthChannels.add(i);
                }
            }
            this.variableWidthChannels = variableWidthChannels.build();
            this.fixedWidthRowSize = fixedWidthRowSize;
        }

        public ListenableFuture<?> isFull()
        {
            return outputBuffer.isFull();
        }

        public PartitionedOutputInfo getInfo()
        {
            return new PartitionedOutputInfo(rowsAdded.get(), pagesAdded.get(), outputBuffer.getPeakMemoryUsage());
        }

        public void partitionPage(Page page)
        {
            // Populate positions to copy for each destination partition.
            int positionCount = page.getPositionCount();

            // We initialize the size of the positions array in each partitionBuffers to be at most the incoming page's positionCount, or roughly two times of positionCount
            // divided by the number of partitions. This is because the latter could be greater than the positionCount when the number of partitions is 1 or positionCount is 1.
            int initialPositionCountForEachBuffer = min(positionCount, (positionCount / partitionFunction.getPartitionCount() + 1) * 2);
            for (int i = 0; i < partitionBuffers.length; i++) {
                partitionBuffers[i].resetPositions(initialPositionCountForEachBuffer);
            }

            Block nullBlock = nullChannel.isPresent() ? page.getBlock(nullChannel.getAsInt()) : null;
            Page partitionFunctionArgs = getPartitionFunctionArguments(page);

            for (int position = 0; position < positionCount; position++) {
                boolean shouldReplicate = (replicatesAnyRow && !hasAnyRowBeenReplicated) ||
                        nullBlock != null && nullBlock.isNull(position);

                if (shouldReplicate) {
                    for (int i = 0; i < partitionBuffers.length; i++) {
                        partitionBuffers[i].addPosition(position);
                    }
                    hasAnyRowBeenReplicated = true;
                }
                else {
                    int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                    partitionBuffers[partition].addPosition(position);
                }
            }

            // Decode the page just once. The decoded blocks will be fed to each PartitionBuffer object to set up AbstractBlockEncodingBuffer.
            long estimatedSerializedPageSize = 0;
            for (int i = 0; i < decodedBlocks.length; i++) {
                decodedBlocks[i] = decodeBlock(flattener, blockLeaseCloser, page.getBlock(i));
                estimatedSerializedPageSize += decodedBlocks[i].getEstimatedSerializedSizeInBytes();
            }

            // Copy the data to their destination partitions and flush when the buffer is full.
            for (int i = 0; i < partitionBuffers.length; i++) {
                partitionBuffers[i].appendData(decodedBlocks, estimatedSerializedPageSize, fixedWidthRowSize, variableWidthChannels, outputBuffer);
            }

            // Return all borrowed arrays
            try {
                blockLeaseCloser.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void flush()
        {
            for (int i = 0; i < partitionBuffers.length; i++) {
                partitionBuffers[i].flush(outputBuffer);
            }
        }

        public long getRetainedSizeInBytes()
        {
            // When called by the operator constructor, the blockDecodingAllocator was empty at the moment.
            // When called in addInput(), the arrays have been returned to the blockDecodingAllocator already,
            // but they're still owned by the decodedBlock which will be counted as part of the decodedBlock.
            // In both cases, the blockDecodingAllocator doesn't need to be counted. But we need to count
            // bufferAllocator which contains buffers used during partitioning, e.g. serializedRowSizes,
            // mappedPositions, etc.
            long size = bufferAllocator.getEstimatedSizeInBytes();

            for (int i = 0; i < partitionBuffers.length; i++) {
                size += partitionBuffers[i].getRetainedSizeInBytes();
            }

            for (int i = 0; i < decodedBlocks.length; i++) {
                size += decodedBlocks[i] == null ? 0 : decodedBlocks[i].getRetainedSizeInBytes();
            }

            return size;
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

        private static int getFixedWidthTypeSize(Type type)
        {
            int bytesPerPosition = 0;
            if (type instanceof FixedWidthType) {
                bytesPerPosition = ((FixedWidthType) type).getFixedSize() + 1;
            }

            return bytesPerPosition;
        }
    }

    private static class PartitionBuffer
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(PartitionBuffer.class).instanceSize();

        private final int partition;
        private final AtomicLong rowsAdded;
        private final AtomicLong pagesAdded;
        private final PagesSerde serde;
        private final Lifespan lifespan;
        private final int capacity;
        private final int channelCount;
        private final ArrayAllocator bufferAllocator;

        private int[] positions;   // the default positions array for top level BlockEncodingBuffer
        private int positionCount;  // number of positions to be copied for this partition
        private BlockEncodingBuffer[] blockEncodingBuffers;

        private int bufferedRowCount;
        private boolean bufferFull;

        PartitionBuffer(int partition, int channelCount, int capacity, AtomicLong pagesAdded, AtomicLong rowsAdded, PagesSerde serde, Lifespan lifespan, ArrayAllocator bufferAllocator)
        {
            this.partition = partition;
            this.channelCount = channelCount;
            this.capacity = capacity;
            this.pagesAdded = requireNonNull(pagesAdded, "pagesAdded is null");
            this.rowsAdded = requireNonNull(rowsAdded, "rowsAdded is null");
            this.serde = requireNonNull(serde, "serde is null");
            this.lifespan = requireNonNull(lifespan, "lifespan is null");
            this.bufferAllocator = requireNonNull(bufferAllocator, "bufferAllocator is null");
        }

        private void resetPositions(int estimatedPositionCount)
        {
            positions = ensureCapacity(positions, estimatedPositionCount);
            this.positionCount = 0;
        }

        private void addPosition(int position)
        {
            positions = ensureCapacity(positions, positionCount + 1, MEDIUM, PRESERVE);
            positions[positionCount++] = position;
        }

        private void appendData(DecodedBlockNode[] decodedBlocks, long estimatedSerializedPageSize, int fixedWidthRowSize, List<Integer> variableWidthChannels, OutputBuffer outputBuffer)
        {
            if (decodedBlocks.length != channelCount) {
                throw new IllegalArgumentException(format("Unexpected number of decoded blocks %d. It should be %d.", decodedBlocks.length, channelCount));
            }

            if (positionCount == 0) {
                return;
            }

            if (channelCount == 0) {
                bufferedRowCount += positionCount;
                return;
            }

            initializeBlockEncodingBuffers(decodedBlocks);

            for (int i = 0; i < channelCount; i++) {
                blockEncodingBuffers[i].setupDecodedBlocksAndPositions(decodedBlocks[i], positions, positionCount, capacity, estimatedSerializedPageSize);
            }

            int[] serializedRowSizes = ensureCapacity(null, positionCount, SMALL, INITIALIZE, bufferAllocator);
            try {
                populateSerializedRowSizes(fixedWidthRowSize, variableWidthChannels, serializedRowSizes);

                // Due to the limitation of buffer size, we append the data batch by batch
                int offset = 0;
                do {
                    bufferFull = false;
                    int batchSize = calculateNextBatchSize(fixedWidthRowSize, variableWidthChannels, offset, serializedRowSizes);

                    for (int i = 0; i < channelCount; i++) {
                        blockEncodingBuffers[i].setNextBatch(offset, batchSize);
                        blockEncodingBuffers[i].appendDataInBatch();
                    }

                    bufferedRowCount += batchSize;
                    offset += batchSize;

                    if (bufferFull) {
                        flush(outputBuffer);
                    }
                }
                while (offset < positionCount);
            }
            finally {
                // Return the borrowed array for serializedRowSizes when the current page for the current partition is finished.
                bufferAllocator.returnArray(serializedRowSizes);
                for (int i = channelCount - 1; i >= 0; i--) {
                    blockEncodingBuffers[i].noMoreBatches();
                }
            }
        }

        private void initializeBlockEncodingBuffers(DecodedBlockNode[] decodedBlocks)
        {
            // Create buffers has to be done after seeing the first page.
            if (blockEncodingBuffers == null) {
                BlockEncodingBuffer[] buffers = new BlockEncodingBuffer[channelCount];
                for (int i = 0; i < channelCount; i++) {
                    buffers[i] = createBlockEncodingBuffers(decodedBlocks[i], bufferAllocator, false);
                }
                blockEncodingBuffers = buffers;
            }
        }

        /**
         * Calculate the row sizes in bytes and write them to serializedRowSizes.
         */
        private void populateSerializedRowSizes(int fixedWidthRowSize, List<Integer> variableWidthChannels, int[] serializedRowSizes)
        {
            if (variableWidthChannels.isEmpty()) {
                return;
            }

            for (int i : variableWidthChannels) {
                blockEncodingBuffers[i].accumulateSerializedRowSizes(serializedRowSizes);
            }

            for (int i = 0; i < positionCount; i++) {
                serializedRowSizes[i] += fixedWidthRowSize;
            }
        }

        private int calculateNextBatchSize(int fixedWidthRowSize, List<Integer> variableWidthChannels, int startPosition, int[] serializedRowSizes)
        {
            int bytesRemaining = capacity - getSerializedBuffersSizeInBytes();

            if (variableWidthChannels.isEmpty()) {
                int maxPositionsFit = max(bytesRemaining / fixedWidthRowSize, 1);
                if (maxPositionsFit <= positionCount - startPosition) {
                    bufferFull = true;
                    return maxPositionsFit;
                }
                return positionCount - startPosition;
            }

            verify(serializedRowSizes != null);
            for (int i = startPosition; i < positionCount; i++) {
                bytesRemaining -= serializedRowSizes[i];

                if (bytesRemaining <= 0) {
                    bufferFull = true;
                    return max(i - startPosition, 1);
                }
            }

            return positionCount - startPosition;
        }

        private void flush(OutputBuffer outputBuffer)
        {
            if (bufferedRowCount == 0) {
                return;
            }

            SliceOutput output = new DynamicSliceOutput(toIntExact(getSerializedBuffersSizeInBytes()));
            output.writeInt(channelCount);

            for (int i = 0; i < channelCount; i++) {
                blockEncodingBuffers[i].serializeTo(output);
                blockEncodingBuffers[i].resetBuffers();
            }

            SerializedPage serializedPage = serde.serialize(output.slice(), bufferedRowCount);
            outputBuffer.enqueue(lifespan, partition, ImmutableList.of(serializedPage));
            pagesAdded.incrementAndGet();
            rowsAdded.addAndGet(bufferedRowCount);

            bufferedRowCount = 0;
        }

        private long getRetainedSizeInBytes()
        {
            long size = INSTANCE_SIZE + sizeOf(positions);

            // Some destination partitions might get 0 rows. In that case the BlockEncodingBuffer won't be created.
            if (blockEncodingBuffers != null) {
                for (int i = 0; i < channelCount; i++) {
                    size += blockEncodingBuffers[i].getRetainedSizeInBytes();
                }
            }

            return size;
        }

        private int getSerializedBuffersSizeInBytes()
        {
            int size = 0;

            for (int i = 0; i < channelCount; i++) {
                size += blockEncodingBuffers[i].getSerializedSizeInBytes();
            }

            return SIZE_OF_INT + size;  // channelCount takes one int
        }
    }
}
