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

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingBuffers;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Verify;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.spi.block.BlockEncodingSerde.createBlockEncodingBuffers;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputOperator
        implements Operator
{
    public static class PartitionedOutputFactory
            implements OutputFactory
    {
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<NullableValue>> partitionConstants;
        private final OutputBuffer outputBuffer;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final DataSize maxMemory;

        public PartitionedOutputFactory(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<NullableValue>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                DataSize maxMemory)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
        }

        @Override
        public OperatorFactory createOutputOperator(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                Function<Page, Page> pagePreprocessor,
                PagesSerdeFactory serdeFactory)
        {
            return new PartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    types,
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

    public static class PartitionedOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final Function<Page, Page> pagePreprocessor;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<NullableValue>> partitionConstants;
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
                List<Optional<NullableValue>> partitionConstants,
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
    private final LocalMemoryContext systemMemoryContext;
    private final long partitionsInitialRetainedSize;
    private boolean finished;

    public PartitionedOutputOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            Function<Page, Page> pagePreprocessor,
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<NullableValue>> partitionConstants,
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
                operatorContext.getDriverContext().getLifespan());

        operatorContext.setInfoSupplier(this::getInfo);
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(PartitionedOutputOperator.class.getSimpleName());
        this.partitionsInitialRetainedSize = this.partitionFunction.getRetainedSizeInBytes();
        this.systemMemoryContext.setBytes(partitionsInitialRetainedSize);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public PartitionedOutputInfo getInfo()
    {
        return partitionFunction.getInfo();
    }

    @Override
    public void finish()
    {
        finished = true;
        partitionFunction.flush(true);
//       // System.out.println("\nfinish");
//       // System.out.println("timeForCalculateRowSizes " + partitionFunction.timeForCalculateRowSizes / 1000000 + " msec");
//       // System.out.println("timeForPopulatePositionsForEachPartition " + partitionFunction.timeForPopulatePositionsForEachPartition / 1000000 + " msec");
//       // System.out.println("timeForAppendRows " + partitionFunction.timeForAppendRows / 1000000 + " msec");
//
//        long timeForPrepareBuffers = 0;
//        long timeForSetupPositionsInBuffers = 0;
//        long timeForCalculateNumberRowsFitInBuffers = 0;
//        long timeForCopyValues = 0;
//        long timeForFlushAndPrepareBuffers = 0;
//        for (PartitionData partitionData : partitionFunction.partitionData) {
//            timeForPrepareBuffers += partitionData.timeForPrepareBuffers;
//            timeForSetupPositionsInBuffers += partitionData.timeForSetupPositionsInBuffers;
//            timeForCalculateNumberRowsFitInBuffers += partitionData.timeForCalculateNumberRowsFitInBuffers;
//            timeForCopyValues += partitionData.timeForCopyValues;
//            timeForFlushAndPrepareBuffers += partitionData.timeForFlushAndPrepareBuffers;
//        }
//       // System.out.println("timeForPrepareBuffers " + timeForPrepareBuffers / 1000000 + " msec");
//       // System.out.println("timeForSetupPositionsInBuffers " + timeForSetupPositionsInBuffers / 1000000 + " msec");
//       // System.out.println("timeForCalculateNumberRowsFitInBuffers " + timeForCalculateNumberRowsFitInBuffers / 1000000 + " msec");
//       // System.out.println("timeForCopyValues " + timeForCopyValues / 1000000 + " msec");
//       // System.out.println("timeForFlushAndPrepareBuffers " + timeForFlushAndPrepareBuffers / 1000000 + " msec");

//        long totalSerializedPageAllocationBytes = 0;
//        long totalSerializedPageAllocationCount = 0;
//        for (PartitionData partitionData : partitionFunction.partitionData) {
//            totalSerializedPageAllocationBytes += partitionData.serializedPageAllocationBytes;
//            totalSerializedPageAllocationCount += partitionData.serializedPageAllocationCount;
//        }
//       // System.out.println("totalSerializedPageAllocationBytes " + totalSerializedPageAllocationBytes);
//       // System.out.println("totalSerializedPageAllocationCount " + totalSerializedPageAllocationCount);
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = partitionFunction.isFull();
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
        //partitionFunction.partitionPage(page);
        partitionFunction.ariaPartitionPage(page);

        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());

        // We use getSizeInBytes() here instead of getRetainedSizeInBytes() for an approximation of
        // the amount of memory used by the pageBuilders, because calculating the retained
        // size can be expensive especially for complex types.
        long partitionsSizeInBytes = partitionFunction.getSizeInBytes();

        // We also add partitionsInitialRetainedSize as an approximation of the object overhead of the partitions.
        systemMemoryContext.setBytes(partitionsSizeInBytes + partitionsInitialRetainedSize);
    }

    @Override
    public Page getOutput()
    {
        return null;
    }

    private static class PartitionData
    {
        private static final int INITIAL_POSITION_COUNT = 1024;
        private final int partition;
        private final AtomicLong rowsAdded;
        private final AtomicLong pagesAdded;
        private final PagesSerde serde;
        private final Lifespan lifespan;

        private int positionCount;  // number of rows for this partition
        private int[] positions;   // 0.. positionCount-1
        BlockEncodingBuffers[] blockEncodingBuffers;
        private int capacity;
        private int bufferedRowCount;
        private boolean hasSetupPositionsInBuffers;

//        private long timeForPrepareBuffers;
//        private long timeForSetupPositionsInBuffers;
//        private long timeForCalculateNumberRowsFitInBuffers;
//        private long timeForCopyValues;
//        private long timeForFlushAndPrepareBuffers;

//        private long serializedPageAllocationBytes;
//        private long serializedPageAllocationCount;

        PartitionData(int partition, AtomicLong pagesAdded, AtomicLong rowsAdded, PagesSerde serde, Lifespan lifespan)
        {
            this.partition = partition;
            this.pagesAdded = pagesAdded;
            this.rowsAdded = rowsAdded;
            this.serde = serde;
            this.capacity = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
            this.lifespan = requireNonNull(lifespan);

            this.positions = new int[INITIAL_POSITION_COUNT];
        }

        private void createBuffers(Page page)
        {
            if (blockEncodingBuffers == null) {
                int channelCount = page.getChannelCount();
                blockEncodingBuffers = new BlockEncodingBuffers[channelCount];
                for (int i = 0; i < channelCount; i++) {
                    blockEncodingBuffers[i] = createBlockEncodingBuffers(page.getBlock(i), positions);
                }
            }
        }

        private void resetBuffers(Page page)
        {
            int channelCount = page.getChannelCount();
            for (int i = 0; i < channelCount; i++) {
                blockEncodingBuffers[i].resetBuffers();
            }
        }

        private int calculateNumberRowsFitInBuffers(boolean containsNonFixedWidthBlocks, int rowSizeForfixedWidthBlocks, @Nullable int[] positionSizesInBytes, int startPosition)
        {
            // TODO: validate rows and rowSizes length >= numberOfRows > firstRowIndex
            // Note that the capacity limit may be exceeded by 1 row.
            int remainingBytes = capacity - getBuffersSizeInBytes();

            if (!containsNonFixedWidthBlocks) {
                //int rowsFit = (int) ceil(((double) remainingBytes / rowSizeForfixedWidthBlocks));
                int rowsFit = remainingBytes / rowSizeForfixedWidthBlocks + 1;
                return min(rowsFit, positionCount - startPosition);
            }

            Verify.verify(positionSizesInBytes != null);
            for (int i = startPosition; i < positionCount; i++) {
                remainingBytes -= positionSizesInBytes[positions[i]];
                if (remainingBytes <= 0) {
                    return max(i - startPosition, 1);
                }
            }

            return positionCount - startPosition;
        }

        // TODO: verify if this is inlined
        public void appendRows(Page page, boolean containsNonFixedWidthBlocks, int rowSizeForfixedWidthBlocks, int[] rowSizes, OutputBuffer outputBuffer)
        {
            if (positionCount == 0) {
                return;
            }

//            long one = System.nanoTime();

            // Create buffers has to be done after seeing the first page.
            if (isBufferEmpty()) {
                createBuffers(page);
            }
//            long two = System.nanoTime();
//            timeForPrepareBuffers += two - one;

            // Why is this function expensive?
            // top level positions is not needed to be set.
            //setupPositionCountInBuffers();

//            long three = System.nanoTime();
//            timeForSetupPositionsInBuffers += three - two;

            int positionsWritten = 0;
            do {
//                long four = System.nanoTime();

                int numberOfRowsFit = calculateNumberRowsFitInBuffers(containsNonFixedWidthBlocks, rowSizeForfixedWidthBlocks, rowSizes, positionsWritten);

                //System.out.println("appendRows numberOfRowsFit " + numberOfRowsFit);
//                long five = System.nanoTime();
//                timeForCalculateNumberRowsFitInBuffers += five - four;

                for (int i = 0; i < page.getChannelCount(); i++) {
                    Block block = page.getBlock(i);
                    blockEncodingBuffers[i].copyValues(block, positionsWritten, numberOfRowsFit);
                }
                bufferedRowCount += numberOfRowsFit;

                positionsWritten += numberOfRowsFit;

//                long six = System.nanoTime();
//                timeForCopyValues += six - five;

                if (isBufferFull()) {
                    flush(outputBuffer);
                    resetBuffers(page);
                }

//                long seven = System.nanoTime();
//                timeForFlushAndPrepareBuffers += seven - six;
            }
            while (positionsWritten < positionCount);
        }

        public void resetPositionCount()
        {
            positionCount = 0;
        }

        public void appendPosition(int position)
        {
            ensurePositionsCapacity();
            positions[positionCount++] = position;
        }

        public void ensurePositionsCapacity()
        {
            if (positions.length <= positionCount) {
                positions = Arrays.copyOf(positions, 2 * positions.length);
                if (blockEncodingBuffers != null) {
                    for (int i = 0; i < blockEncodingBuffers.length; i++) {
                        blockEncodingBuffers[i].setPositions(positions);  // top level positions is not needed to be set.
                    }
                }
            }
        }

        private void flush(OutputBuffer outputBuffer)
        {
            if (isBufferEmpty()) {
                return;
            }

            Slice slice = serialize();
            SerializedPage serializedPage = serde.wrapBuffer(slice, bufferedRowCount);
            outputBuffer.enqueue(lifespan, partition, Collections.singletonList(serializedPage));
            pagesAdded.incrementAndGet();
            rowsAdded.addAndGet(bufferedRowCount);
            bufferedRowCount = 0;
        }

        // Copy the buffers in order into the final slice
        private Slice serialize()
        {
            // TODO: Validate the bufferedPositionCount > 0
            //  The performace using DynamicSliceOutput should be ok if the capacity is enough, and for each buffer we only do one range check.
            // TODO: size should include channelCount etc
            int bytes = getBuffersSizeInBytes();
           // System.out.println("serialize bytes " + bytes);
//            serializedPageAllocationBytes += bytes;
//            serializedPageAllocationCount++;
            Slice slice = Slices.wrappedBuffer(new byte[bytes]);
            SliceOutput sliceOutput = slice.getOutput();

           // System.out.println("serialize writing number of blocks at " + sliceOutput.size());
            sliceOutput.writeInt(blockEncodingBuffers.length);  // channelCount

           // System.out.println("serialize number of blocks " + blockEncodingBuffers.length + sliceOutput.size());

            for (BlockEncodingBuffers currentBlockEncodingBuffers : blockEncodingBuffers) {
                currentBlockEncodingBuffers.writeTo(sliceOutput);
            }
            return sliceOutput.slice();
        }

        private boolean isBufferEmpty()
        {
            return bufferedRowCount == 0;
        }

        private boolean isBufferFull()
        {
            return getBuffersSizeInBytes() >= capacity;
        }

        private int getBuffersSizeInBytes()
        {
            int blockEncodingBuffersSize = 0;
            for (BlockEncodingBuffers blockEncodingBuffers : blockEncodingBuffers) {
                blockEncodingBuffersSize += blockEncodingBuffers.getSizeInBytes();
            }
            return SIZE_OF_INT + blockEncodingBuffersSize;
        }
    }

    private static class PagePartitioner
    {
        private final OutputBuffer outputBuffer;
        private final List<Type> sourceTypes;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<Block>> partitionConstants;
        private final PagesSerde serde;
        private final Lifespan lifespan;
        private final PageBuilder[] pageBuilders;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel; // when present, send the position to every partition if this channel is null.
        private final AtomicLong rowsAdded = new AtomicLong();
        private final AtomicLong pagesAdded = new AtomicLong();
        private boolean hasAnyRowBeenReplicated;

        private final PartitionData[] partitionData;   // TODO: Check final
        private int[] rowSizes;
        private int rowSizeForfixedWidthBlocks;
        private boolean containsNonFixedWidthColumns;

        private long timeForCalculateRowSizes;
        private long timeForPopulatePositionsForEachPartition;
        private long timeForAppendRows;

        public PagePartitioner(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<NullableValue>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                PagesSerdeFactory serdeFactory,
                List<Type> sourceTypes,
                DataSize maxMemory,
                Lifespan lifespan)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null").stream()
                    .map(constant -> constant.map(NullableValue::asBlock))
                    .collect(toImmutableList());
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.serde = requireNonNull(serdeFactory, "serdeFactory is null").createPagesSerde();
            this.lifespan = requireNonNull(lifespan, "lifespan is null");

            int partitionCount = partitionFunction.getPartitionCount();
            int pageSize = min(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, ((int) maxMemory.toBytes()) / partitionCount);
            pageSize = max(1, pageSize);

            this.pageBuilders = new PageBuilder[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                pageBuilders[i] = PageBuilder.withMaxPageSize(pageSize, sourceTypes);
            }

            partitionData = new PartitionData[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                partitionData[i] = new PartitionData(i, pagesAdded, rowsAdded, serde, lifespan);
            }
        }

        public ListenableFuture<?> isFull()
        {
            return outputBuffer.isFull();
        }

        public long getSizeInBytes()
        {
            // We use a foreach loop instead of streams
            // as it has much better performance.
            long sizeInBytes = 0;
            for (PageBuilder pageBuilder : pageBuilders) {
                sizeInBytes += pageBuilder.getSizeInBytes();
            }
            return sizeInBytes;
        }

        /**
         * This method can be expensive for complex types.
         */
        public long getRetainedSizeInBytes()
        {
            long sizeInBytes = 0;
            for (PageBuilder pageBuilder : pageBuilders) {
                sizeInBytes += pageBuilder.getRetainedSizeInBytes();
            }
            return sizeInBytes;
        }

        public PartitionedOutputInfo getInfo()
        {
            return new PartitionedOutputInfo(rowsAdded.get(), pagesAdded.get(), outputBuffer.getPeakMemoryUsage());
        }

        public void partitionPage(Page page)
        {
            requireNonNull(page, "page is null");

            Page partitionFunctionArgs = getPartitionFunctionArguments(page);
            for (int position = 0; position < page.getPositionCount(); position++) {
                boolean shouldReplicate = (replicatesAnyRow && !hasAnyRowBeenReplicated) ||
                        nullChannel.isPresent() && page.getBlock(nullChannel.getAsInt()).isNull(position);
                if (shouldReplicate) {
                    for (PageBuilder pageBuilder : pageBuilders) {
                        appendRow(pageBuilder, page, position);
                    }
                    hasAnyRowBeenReplicated = true;
                }
                else {
                    int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                    appendRow(pageBuilders[partition], page, position);
                }
            }
            flush(false);
        }

        void ariaPartitionPage(Page page)
        {
            //long one = System.nanoTime();
            calculateRowSizes(page);
            //long two = System.nanoTime();
            //System.out.println("calculateRowSizes");
            //timeForCalculateRowSizes += two - one;

            populatePositionsForEachPartition(page);

            //System.out.println("populatePositionsForEachPartition");

//            long three = System.nanoTime();
//            timeForPopulatePositionsForEachPartition += three - two;

            for (int i = 0; i < partitionData.length; i++) {
                //System.out.println("appendRows for partition " + i);
                partitionData[i].appendRows(page, containsNonFixedWidthColumns, rowSizeForfixedWidthBlocks, rowSizes, outputBuffer);
            }
            //System.out.println("appendRows");

//            long four = System.nanoTime();
//            timeForAppendRows += four - three;
        }

        private void calculateRowSizes(Page page)
        {
            rowSizeForfixedWidthBlocks = 0;
            containsNonFixedWidthColumns = false;
            for (int i = 0; i < page.getChannelCount(); i++) {
                rowSizeForfixedWidthBlocks += 8;
            }

            if (!containsNonFixedWidthColumns) {
                return;
            }

            if (rowSizes == null || rowSizes.length < page.getPositionCount()) {
                rowSizes = new int[(int) (page.getPositionCount() * 1.2)];
            }
            Arrays.fill(rowSizes, 0);
            for (int i = 0; i < page.getChannelCount(); i++) {
                page.getBlock(i).appendPositionSizesInBytes(rowSizes);
            }
        }

        private void populatePositionsForEachPartition(Page page)
        {
            for (int i = 0; i < partitionData.length; i++) {
                PartitionData target = partitionData[i];
                target.resetPositionCount();
                // Note that we're over allocating here. This is temporary because the profile shows the cost is non-neglegible if we put it int the loop below
            }

            Page partitionFunctionArgs = getPartitionFunctionArguments(page);
            int positionCount = page.getPositionCount();
            for (int position = 0; position < positionCount; position++) {
                // TODO: This is still the row-based way. We need to change it to vectorized way if the profile shows it necessary.
                int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                PartitionData target = partitionData[partition];

                target.appendPosition(position);  // This was inlined
                //target.positions[target.positionCount++] = position;
            }
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

        private void appendRow(PageBuilder pageBuilder, Page page, int position)
        {
            pageBuilder.declarePosition();

            for (int channel = 0; channel < sourceTypes.size(); channel++) {
                Type type = sourceTypes.get(channel);
                type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
            }
        }

        public void flush(boolean force)
        {
            if (partitionData != null) {
                if (!force) {
                    return;
                }
                for (PartitionData target : partitionData) {
                    target.flush(outputBuffer);
                }
                return;
            }

            // add all full pages to output buffer
            for (int partition = 0; partition < pageBuilders.length; partition++) {
                PageBuilder partitionPageBuilder = pageBuilders[partition];
                if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                    Page pagePartition = partitionPageBuilder.build();
                    partitionPageBuilder.reset();

                    List<SerializedPage> serializedPages = splitPage(pagePartition, DEFAULT_MAX_PAGE_SIZE_IN_BYTES).stream()
                            .map(serde::serialize)
                            .collect(toImmutableList());

                    outputBuffer.enqueue(lifespan, partition, serializedPages);
                    pagesAdded.incrementAndGet();
                    rowsAdded.addAndGet(pagePartition.getPositionCount());
                }
            }
        }
    }

    public static class PartitionedOutputInfo
            implements Mergeable<PartitionedOutputInfo>, OperatorInfo
    {
        private final long rowsAdded;
        private final long pagesAdded;
        private final long outputBufferPeakMemoryUsage;

        @JsonCreator
        public PartitionedOutputInfo(
                @JsonProperty("rowsAdded") long rowsAdded,
                @JsonProperty("pagesAdded") long pagesAdded,
                @JsonProperty("outputBufferPeakMemoryUsage") long outputBufferPeakMemoryUsage)
        {
            this.rowsAdded = rowsAdded;
            this.pagesAdded = pagesAdded;
            this.outputBufferPeakMemoryUsage = outputBufferPeakMemoryUsage;
        }

        @JsonProperty
        public long getRowsAdded()
        {
            return rowsAdded;
        }

        @JsonProperty
        public long getPagesAdded()
        {
            return pagesAdded;
        }

        @JsonProperty
        public long getOutputBufferPeakMemoryUsage()
        {
            return outputBufferPeakMemoryUsage;
        }

        @Override
        public PartitionedOutputInfo mergeWith(PartitionedOutputInfo other)
        {
            return new PartitionedOutputInfo(
                    rowsAdded + other.rowsAdded,
                    pagesAdded + other.pagesAdded,
                    Math.max(outputBufferPeakMemoryUsage, other.outputBufferPeakMemoryUsage));
        }

        @Override
        public boolean isFinal()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("rowsAdded", rowsAdded)
                    .add("pagesAdded", pagesAdded)
                    .add("outputBufferPeakMemoryUsage", outputBufferPeakMemoryUsage)
                    .toString();
        }
    }
}
