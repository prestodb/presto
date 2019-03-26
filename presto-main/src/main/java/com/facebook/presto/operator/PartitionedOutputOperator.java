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

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.AriaFlags;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockDecoder;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.EncodingState;
import com.facebook.presto.spi.block.IntArrayAllocator;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.memory.ByteArrayPool;
import com.facebook.presto.spi.memory.Caches;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VariableWidthType;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
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
        int ariaFlags = SystemSessionProperties.ariaFlags(operatorContext.getSession());
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
                operatorContext.getDriverContext().getLifespan(),
                ariaFlags);

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
        partitionFunction.partitionPage(page);

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
        private final int partition;
        private final AtomicLong rowsAdded;
        private final AtomicLong pagesAdded;
        int numNewRows;
        int bufferedRows;
        int maxRows;
        int bufferedBytes;
        int maxBytes;
        // Row numbers in the current Page that go to this destination. 0..numNewRows
        int[] rows = new int[100];
        byte[] topLevelBuffer;
        EncodingState[] encodingStates;
        BlockEncoding[] encodings;
        Slice topLevelSlice;
        PagesSerde serde;
        ByteArrayPool pool;
        Lifespan lifespan;

        PartitionData(int partition, AtomicLong pagesAdded, AtomicLong rowsAdded, PagesSerde serde, ByteArrayPool pool, Lifespan lifespan)
        {
            this.partition = partition;
            this.pagesAdded = pagesAdded;
            this.rowsAdded = rowsAdded;
            this.serde = serde;
            this.pool = pool;
            this.lifespan = lifespan;
        }

        public long getRetainedSizeInBytes()
        {
            return topLevelSlice != null ? topLevelSlice.length() : 0;
        }

        private byte[] newBytes(int size)
        {
            if (pool != null) {
                return pool.getBytes(size);
            }
            return new byte[size];
        }

        void prepareBatch()
        {
            numNewRows = 0;
        }

        void appendRows(BlockDecoder[] contents, int partition, int fixedRowSize, int[] rowSizes, OutputBuffer outputBuffer)
        {
            if (bufferedRows == 0) {
                prepareBuffer(contents, serde);
            }
            int rowsToWrite = numNewRows;
            int rowsWritten = 0;
            do {
                int numRowsFit = calculateNumRowsInBatch(rowsWritten, fixedRowSize, rowSizes);
                for (int i = 0; i < contents.length; i++) {
                    encodings[i].addValues(contents[i], rows, rowsWritten, numRowsFit, encodingStates[i]);
                }
                if (numRowsFit + rowsWritten < numNewRows) {
                    flush(outputBuffer);
                    prepareBuffer(contents, serde);
                }
                rowsWritten += numRowsFit;
            } while (rowsWritten < rowsToWrite);
            numNewRows = 0;
        }

        int calculateNumRowsInBatch(int firstToWrite, int fixedRowSize, int[] rowSizes)
        {
            if (rowSizes == null) {
                int space = Math.min(numNewRows - firstToWrite, maxRows - bufferedRows);
                bufferedRows += space;
                return space;
            }
            int row = firstToWrite;
            int last = Math.min(numNewRows, firstToWrite + maxRows - bufferedRows);
            for (; row < last; row++) {
                int size = rowSizes[rows[row]];
                if (size + bufferedBytes > maxBytes) {
                    if (bufferedBytes == 0) {
                        bufferedBytes = size;
                        return 1;
                    }
                    return row - firstToWrite;
                }
                bufferedBytes += size;
            }
            return row - firstToWrite;
        }

        void flush(OutputBuffer outputBuffer)
        {
            if (bufferedRows == 0) {
                return;
            }
            int finalSize = 4;
            boolean allFits = true;
            for (int i = 0; i < encodings.length; i++) {
                EncodingState state = encodingStates[i];
                int size = encodings[i].prepareFinish(state, finalSize);
                finalSize += size;
                if (size > state.getBytesInBuffer()) {
                    allFits = false;
                }
            }
            Slice buffer;
            if (allFits) {
                buffer = Slices.wrappedBuffer(topLevelBuffer, 0, finalSize);
            }
            else {
                byte[] bytes = newBytes(finalSize);
                buffer = Slices.wrappedBuffer(bytes);
                buffer.setInt(0, encodings.length);
            }
            for (int i = 0; i < encodings.length; i++) {
                encodings[i].finish(encodingStates[i], buffer);
            }
            SerializedPage serialized = serde.wrapBuffer(buffer, bufferedRows);
            serialized.setByteArrayPool(pool);
            if (serialized.getSlice() == buffer) {
                topLevelBuffer = null;
                topLevelSlice = null;
            }
            ArrayList<SerializedPage> list = new ArrayList();
            list.add(serialized);
            outputBuffer.enqueue(lifespan, partition, list);
            pagesAdded.incrementAndGet();
            rowsAdded.addAndGet(bufferedRows);
            bufferedRows = 0;
            bufferedBytes = 0;
        }

        static int elementSize(Block block)
        {
            return 8;
        }

        void prepareBuffer(BlockDecoder[] contents, PagesSerde serde)
        {
            int targetBytes = Math.max(64 * 1024, DEFAULT_MAX_PAGE_SIZE_IN_BYTES) - contents.length * 20;
            if (encodings == null) {
                encodings = new BlockEncoding[contents.length];
                encodingStates = new EncodingState[contents.length];
                int rowBytes = 0;
                for (int i = 0; i < contents.length; i++) {
                    BlockDecoder content = contents[i];
                    rowBytes += elementSize(content.leafBlock);
                    encodings[i] = serde.getBlockEncodingSerde().getEncoding(content.leafBlock);
                    encodingStates[i] = new EncodingState();
                }
                maxRows = Math.max(1, targetBytes / rowBytes);
            }
            int bufferSize = 4;
            for (int i = 0; i < encodings.length; i++) {
                bufferSize = encodings[i].reserveBytesInBuffer(contents[i], maxRows, bufferSize, encodingStates[i]);
            }
            if (topLevelBuffer == null || topLevelBuffer.length < bufferSize) {
                topLevelBuffer = newBytes(bufferSize);
                topLevelSlice = Slices.wrappedBuffer(topLevelBuffer);
                topLevelSlice.setInt(0, contents.length);
            }
            for (EncodingState state : encodingStates) {
                state.setBuffer(topLevelSlice);
            }
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
        private PartitionData[] partitionData;
        private int[] partitionOfRow;
        private int fixedRowSize;
        private int[] rowSizes;
        private ArrayList<Integer> variableWidthChannels;
        private BlockDecoder[] blockContents;
        private BlockDecoder partitionDecoder;
        private BlockEncoding[] encodings;
        private IntArrayAllocator intArrayAllocator;
        private boolean useAria;
        private boolean recycleBuffers;
        private ByteArrayPool pool;

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
                Lifespan lifespan,
                int ariaFlags)
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

            useAria = (ariaFlags & AriaFlags.repartition) != 0;
            recycleBuffers = (ariaFlags & AriaFlags.exchangeReuse) != 0;
            if (recycleBuffers) {
                pool = Caches.getByteArrayPool();
            }
            int partitionCount = partitionFunction.getPartitionCount();
            int pageSize = min(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, ((int) maxMemory.toBytes()) / partitionCount);
            pageSize = max(1, pageSize);
            for (int i = 0; i < sourceTypes.size(); i++) {
                Type type = sourceTypes.get(i);
                fixedRowSize += fixedSerializedLength(type);
                if (isVariableWidth(type)) {
                    if (variableWidthChannels == null) {
                        variableWidthChannels = new ArrayList();
                    }
                    variableWidthChannels.add(i);
                }
            }
            if (useAria) {
                partitionData = new PartitionData[partitionCount];
                for (int i = 0; i < partitionCount; i++) {
                    partitionData[i] = new PartitionData(i, pagesAdded, rowsAdded, serde, pool, lifespan);
                }
                blockContents = new BlockDecoder[sourceTypes.size()];
                intArrayAllocator = new IntArrayAllocator();
                partitionDecoder = new BlockDecoder(intArrayAllocator);
                for (int i = 0; i < blockContents.length; i++) {
                    blockContents[i] = new BlockDecoder();
                }
                this.pageBuilders = null;
                return;
            }
            this.pageBuilders = new PageBuilder[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                pageBuilders[i] = PageBuilder.withMaxPageSize(pageSize, sourceTypes);
            }
        }

        private int fixedSerializedLength(Type type)
        {
            return 8;
        }

        private boolean isVariableWidth(Type type)
        {
            return type instanceof VariableWidthType;
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
            if (pageBuilders == null) {
                return getRetainedSizeInBytes();
            }
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
            if (pageBuilders != null) {
                for (PageBuilder pageBuilder : pageBuilders) {
                    sizeInBytes += pageBuilder.getRetainedSizeInBytes();
                }
            }
            else {
                for (PartitionData data : partitionData) {
                    sizeInBytes += data.getRetainedSizeInBytes();
                }
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
            if (useAria) {
                int positionCount = page.getPositionCount();
                if (partitionOfRow == null || partitionOfRow.length < positionCount) {
                    partitionOfRow = new int[(int) (positionCount * 1.2)];
                }
                partitionFunction.getPartitions(partitionData.length, page, partitionDecoder, partitionOfRow);
                ariaPartitionPage(page, positionCount);
                return;
            }
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
                for (PartitionData data : partitionData) {
                    data.flush(outputBuffer);
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

        int getRowByteSize(int position)
        {
            return sourceTypes.size() * 8;
        }

        void ariaPartitionPage(Page page, int positionCount)
        {
            for (int i = 0; i < sourceTypes.size(); i++) {
                blockContents[i].decodeBlock(page.getBlock(i), intArrayAllocator);
            }

            if (variableWidthChannels != null) {
                if (rowSizes == null || rowSizes.length < positionCount) {
                    rowSizes = new int[(int) (positionCount * 1.2)];
                }
                Arrays.fill(rowSizes, 0);
                for (int i = 0; i < variableWidthChannels.size(); i++) {
                    page.getBlock(variableWidthChannels.get(i).intValue()).addElementSizes(null, rowSizes, intArrayAllocator);
                }
            }
            for (PartitionData target : partitionData) {
                target.prepareBatch();
            }
            for (int i = 0; i < positionCount; i++) {
                PartitionData target = partitionData[partitionOfRow[i]];
                if (target.rows.length <= target.numNewRows) {
                    target.rows = Arrays.copyOf(target.rows, 2 * target.rows.length);
                }
                target.rows[target.numNewRows++] = i;
            }
            for (int i = 0; i < partitionData.length; i++) {
                partitionData[i].appendRows(blockContents, i, fixedRowSize, rowSizes, outputBuffer);
            }
            for (int i = 0; i < sourceTypes.size(); i++) {
                blockContents[i].release(intArrayAllocator);
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

    @Override
    public boolean retainsInputPages()
    {
        return false;
    }
}
