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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockFlattener;
import com.facebook.presto.spi.block.ByteArrayBlock;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.block.ColumnarRow;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.Int128ArrayBlock;
import com.facebook.presto.spi.block.IntArrayBlock;
import com.facebook.presto.spi.block.LongArrayBlock;
import com.facebook.presto.spi.block.MapBlock;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.block.ShortArrayBlock;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.LongDecimalType;
import com.facebook.presto.spi.type.ShortDecimalType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.IntervalDayTimeType;
import com.facebook.presto.type.IntervalYearMonthType;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.facebook.presto.execution.buffer.PageSplitterUtil.splitPage;
import static com.facebook.presto.operator.BlockEncodingBuffers.createBlockEncodingBuffers;
import static com.facebook.presto.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
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
        private final List<Optional<ConstantExpression>> partitionConstants;
        private final OutputBuffer outputBuffer;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final DataSize maxMemory;

        public PartitionedOutputFactory(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<ConstantExpression>> partitionConstants,
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
                    maxMemory,
                    SystemSessionProperties.isOptimizedPartitionedOutputEnabled(operatorContext.getSession()));
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
    private boolean useOptmizedPartitionedOutputOperator;

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
            DataSize maxMemory,
            boolean useOptmizedPartitionedOutputOperator)
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
                operatorContext.getDriverContext().getLifespan(),
                useOptmizedPartitionedOutputOperator);

        operatorContext.setInfoSupplier(this::getInfo);
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(PartitionedOutputOperator.class.getSimpleName());
        this.partitionsInitialRetainedSize = this.partitionFunction.getRetainedSizeInBytes();
        this.systemMemoryContext.setBytes(partitionsInitialRetainedSize);
        this.useOptmizedPartitionedOutputOperator = requireNonNull(useOptmizedPartitionedOutputOperator, "useOptmizedPartitionedOutputOperator is null");
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
        if (useOptmizedPartitionedOutputOperator) {
            partitionFunction.ariaPartitionPage(page);
        }
        else {
            partitionFunction.partitionPage(page);
        }

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
        private static final int DEFAULT_ELEMENT_SIZE_IN_BYTES = 8;

        private final int partition;
        private final AtomicLong rowsAdded;
        private final AtomicLong pagesAdded;
        private final PagesSerde serde;
        private final Lifespan lifespan;

        private int positionCount;  // number of rows for this partition
        private int channelCount;
        private int[] positions;   // the default positions array
        private int[] rowSizes;
        private BlockEncodingBuffers[] blockEncodingBuffers;
        private int capacity;
        private int bufferedRowCount;
        private boolean bufferFull;

        PartitionData(int partition, int channelCount, AtomicLong pagesAdded, AtomicLong rowsAdded, PagesSerde serde, Lifespan lifespan)
        {
            this.partition = partition;
            this.channelCount = channelCount;
            this.pagesAdded = pagesAdded;
            this.rowsAdded = rowsAdded;
            this.serde = serde;
            this.capacity = DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
            this.lifespan = requireNonNull(lifespan);

            this.positions = new int[INITIAL_POSITION_COUNT];
            this.rowSizes = new int[INITIAL_POSITION_COUNT];
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

        public void createBuffersIfNecessary(DecodedObjectNode[] decodedBlocks)
        {
            // Create buffers has to be done after seeing the first page.
            if (blockEncodingBuffers == null) {
                createBuffers(decodedBlocks);
            }
        }

        public void appendRows(DecodedObjectNode[] decodedBlocks, boolean containsNonFixedWidthBlocks, int rowSizeForfixedWidthBlocks, ArrayList<Integer> variableWidthBlockChannels, OutputBuffer outputBuffer)
        {
            if (positionCount == 0) {
                return;
            }

            createBuffersIfNecessary(decodedBlocks);

            setupDecodedBlocksAndPositions(decodedBlocks);

            calculateRowSizes(containsNonFixedWidthBlocks, rowSizeForfixedWidthBlocks, variableWidthBlockChannels);

            int positionsWritten = 0;
            do {
                int numberOfRowsFit = calculateNumberRowsFitInBuffers(containsNonFixedWidthBlocks, rowSizeForfixedWidthBlocks, positionsWritten);
                verify(numberOfRowsFit > 0 && numberOfRowsFit <= positionCount - positionsWritten);

                for (int i = 0; i < channelCount; i++) {
                    blockEncodingBuffers[i].setNextBatch(positionsWritten, numberOfRowsFit);
                    blockEncodingBuffers[i].copyValues();
                }

                bufferedRowCount += numberOfRowsFit;
                positionsWritten += numberOfRowsFit;

                if (isBufferFull()) {
                    flush(outputBuffer);
                    resetBuffers();
                }
            }
            while (positionsWritten < positionCount);
        }

        private void createBuffers(DecodedObjectNode[] decodedObjectNodes)
        {
            int channelCount = decodedObjectNodes.length;
            int initialElementCount = min(capacity / channelCount / DEFAULT_ELEMENT_SIZE_IN_BYTES, INITIAL_POSITION_COUNT);

            blockEncodingBuffers = new BlockEncodingBuffers[channelCount];
            for (int i = 0; i < channelCount; i++) {
                blockEncodingBuffers[i] = createBlockEncodingBuffers(decodedObjectNodes[i], initialElementCount);
            }
        }

        private void resetBuffers()
        {
            bufferFull = false;
            for (int i = 0; i < blockEncodingBuffers.length; i++) {
                blockEncodingBuffers[i].resetBuffers();
            }
        }

        private void setupDecodedBlocksAndPositions(DecodedObjectNode[] decodedObjectNodes)
        {
            int channelCount = decodedObjectNodes.length;
            for (int i = 0; i < channelCount; i++) {
                blockEncodingBuffers[i].setupDecodedBlocksAndPositions(decodedObjectNodes[i], positions, positionCount);
            }
        }

        private void calculateRowSizes(boolean containsNonFixedWidthBlocks, int rowSizeForfixedWidthBlocks, ArrayList<Integer> variableWidthBlockChannels)
        {
            if (!containsNonFixedWidthBlocks) {
                return;
            }

            ensureRowSizesCapacity();

            resetRowSizes();

            for (int variableWidthChannel : variableWidthBlockChannels) {
                blockEncodingBuffers[variableWidthChannel].accumulateRowSizes(rowSizes);
            }

            for (int i = 0; i < positionCount; i++) {
                rowSizes[i] += rowSizeForfixedWidthBlocks;
            }
        }

        private void resetRowSizes()
        {
            Arrays.fill(rowSizes, 0, positionCount, 0);
        }

        private int calculateNumberRowsFitInBuffers(boolean containsNonFixedWidthBlocks, int rowSizeForfixedWidthBlocks, int startPosition)
        {
            // TODO: validate rows and rowSizes length >= numberOfRows > firstRowIndex
            // Note that the capacity limit may be exceeded by 1 row.
            int remainingBytes = capacity - getBuffersSizeInBytes();

            if (!containsNonFixedWidthBlocks) {
                //int rowsFit = (int) ceil(((double) remainingBytes / rowSizeForfixedWidthBlocks));
                int rowsFit = max(remainingBytes / rowSizeForfixedWidthBlocks, 1);
                bufferFull = (rowsFit <= positionCount - startPosition);
                return min(rowsFit, positionCount - startPosition);
            }

            verify(rowSizes != null);
            for (int i = startPosition; i < positionCount; i++) {
                int newRemainingBytes = remainingBytes - rowSizes[i];

                if (newRemainingBytes <= 0) {
                    bufferFull = true;
                    return max(i - startPosition, 1);
                }
                remainingBytes = newRemainingBytes;
            }

            return positionCount - startPosition;
        }

        private void flush(OutputBuffer outputBuffer)
        {
            if (isBufferEmpty()) {
                return;
            }

            Slice slice = serialize();
            SerializedPage serializedPage = serde.wrapBuffer(slice, bufferedRowCount);
            //serializedPage.print();
            outputBuffer.enqueue(lifespan, partition, Collections.singletonList(serializedPage));
            pagesAdded.incrementAndGet();
            rowsAdded.addAndGet(bufferedRowCount);
            bufferedRowCount = 0;
        }

        // Copy the buffers in order into the final slice
        private Slice serialize()
        {
            int bytes = getBuffersSizeInBytes();

            Slice slice = Slices.wrappedBuffer(new byte[bytes]);
            SliceOutput sliceOutput = slice.getOutput();
            sliceOutput.writeInt(blockEncodingBuffers.length);  // channelCount

            for (int i = 0; i < blockEncodingBuffers.length; i++) {
                BlockEncodingBuffers currentBlockEncodingBuffers = blockEncodingBuffers[i];
                currentBlockEncodingBuffers.writeTo(sliceOutput);
            }
            return sliceOutput.slice();
        }

        private void ensurePositionsCapacity()
        {
            if (positions.length <= positionCount) {
                positions = Arrays.copyOf(positions, max(2 * positions.length, positionCount));
            }
        }

        private void ensureRowSizesCapacity()
        {
            if (rowSizes.length <= positionCount) {
                rowSizes = Arrays.copyOf(rowSizes, max(2 * rowSizes.length, positionCount));
            }
        }

        private boolean isBufferEmpty()
        {
            return bufferedRowCount == 0;
        }

        private boolean isBufferFull()
        {
            return bufferFull;
        }

        private int getBuffersSizeInBytes()
        {
            int blockEncodingBuffersSize = 0;
            for (int i = 0; i < blockEncodingBuffers.length; i++) {
                BlockEncodingBuffers blockEncodingBuffers = this.blockEncodingBuffers[i];
                blockEncodingBuffersSize += blockEncodingBuffers.getSizeInBytes();
            }
            return SIZE_OF_INT + blockEncodingBuffersSize;  // Channel count for one int
        }
    }

    static class DecodedObjectNode
    {
        private final Object decodedBlock;
        private final ArrayList<DecodedObjectNode> children;

        public DecodedObjectNode(Object decodedBlock)
        {
            this.decodedBlock = decodedBlock;
            children = new ArrayList<>();
        }

        public Object getDecodedBlock()
        {
            return decodedBlock;
        }

        private void addChild(DecodedObjectNode node)
        {
            children.add(node);
        }

        public DecodedObjectNode addChild(Object decodedBlock)
        {
            DecodedObjectNode childNode = new DecodedObjectNode(decodedBlock);
            addChild(childNode);
            return childNode;
        }

        public ArrayList<DecodedObjectNode> getChildren()
        {
            return children;
        }

        public DecodedObjectNode getChild(int index)
        {
            if (index < children.size()) {
                return children.get(index);
            }

            return null;
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
        private int rowSizeForfixedWidthBlocks;
        private boolean containsNonFixedWidthColumns;
        private ArrayList<Integer> variableWidthBlockChannels = new ArrayList<>();
        private BlockFlattener flattener;
        private DecodedObjectNode[] decodedBlocks;
        private boolean useOptmizedPartitionedOutputOperator;

        private static final Map<Class, Integer> FIXED_WIDTH_TYPE_SERIALIZED_BYTES = new HashMap<>();

        static {
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(BigintType.class, 9);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(IntegerType.class, 5);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(BooleanType.class, 2);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(CharType.class, 2);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(DateType.class, 5);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(DoubleType.class, 9);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(IntervalDayTimeType.class, 9);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(IntervalYearMonthType.class, 5);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(LongDecimalType.class, 17);  // Why is LongDecimalType, ShortDecimalType not public?
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(ShortDecimalType.class, 9);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(SmallintType.class, 3);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(TimeType.class, 9);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(TimeWithTimeZoneType.class, 9);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(TimestampType.class, 9);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(TimestampWithTimeZoneType.class, 9);
            FIXED_WIDTH_TYPE_SERIALIZED_BYTES.put(TinyintType.class, 2);
        }

        private static Map<Class, Integer> serializedBytesPerRow2 = new HashMap<>();

        static {
            serializedBytesPerRow2.put(LongArrayBlock.class, 9);
            serializedBytesPerRow2.put(IntArrayBlock.class, 5);
            serializedBytesPerRow2.put(ShortArrayBlock.class, 3);
            serializedBytesPerRow2.put(ByteArrayBlock.class, 2);
            serializedBytesPerRow2.put(Int128ArrayBlock.class, 17);
        }

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
                Lifespan lifespan,
                boolean useOptmizedPartitionedOutputOperator)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null").stream()
                    .map(constant -> constant.map(ConstantExpression::getValueBlock))
                    .collect(toImmutableList());
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.serde = requireNonNull(serdeFactory, "serdeFactory is null").createPagesSerde();
            this.lifespan = requireNonNull(lifespan, "lifespan is null");
            this.useOptmizedPartitionedOutputOperator = requireNonNull(useOptmizedPartitionedOutputOperator, "useOptmizedPartitionedOutputOperator is null");

            int partitionCount = partitionFunction.getPartitionCount();
            int pageSize = min(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, ((int) maxMemory.toBytes()) / partitionCount);
            pageSize = max(1, pageSize);

            this.pageBuilders = new PageBuilder[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                pageBuilders[i] = PageBuilder.withMaxPageSize(pageSize, sourceTypes);
            }

            partitionData = new PartitionData[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                partitionData[i] = new PartitionData(i, sourceTypes.size(), pagesAdded, rowsAdded, serde, lifespan);
            }

            this.flattener = new BlockFlattener(new SimpleArrayAllocator(sourceTypes.size()));

            calculateRowSizeForFixedWidthBlocks();
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

        public void ariaPartitionPage(Page page)
        {
            populatePositionsForEachPartition(page);

            decodePage(page);

            for (int i = 0; i < partitionData.length; i++) {
                PartitionData target = partitionData[i];
                target.appendRows(decodedBlocks, containsNonFixedWidthColumns, rowSizeForfixedWidthBlocks, variableWidthBlockChannels, outputBuffer);
            }
        }

        private void calculateRowSizeForFixedWidthBlocks()
        {
            rowSizeForfixedWidthBlocks = 0;
            containsNonFixedWidthColumns = false;
            for (int i = 0; i < sourceTypes.size(); i++) {
                Type type = sourceTypes.get(i);
                Integer serializedBytesPerRowForCurrentBlock = FIXED_WIDTH_TYPE_SERIALIZED_BYTES.getOrDefault(type.getClass(), 0);
                rowSizeForfixedWidthBlocks += serializedBytesPerRowForCurrentBlock;

                if (serializedBytesPerRowForCurrentBlock == 0) {
                    containsNonFixedWidthColumns = true;
                    variableWidthBlockChannels.add(i);
                }
            }
        }

        private void decodePage(Page page)
        {
            int channelCount = page.getChannelCount();

            if (decodedBlocks == null) {
                decodedBlocks = new DecodedObjectNode[channelCount];
            }

            for (int i = 0; i < page.getChannelCount(); i++) {
                decodedBlocks[i] = decodeBlock(page.getBlock(i));
            }
        }

        // Dict/RLE is a separate node. There can't be consequetive Dict/RLE nodes because we call flatten
        private DecodedObjectNode decodeBlock(Block block)
        {
            Block decodedBlock = flattener.flatten(block).get();

            if (decodedBlock instanceof ArrayBlock) {
                ColumnarArray columnarArray = ColumnarArray.toColumnarArray(decodedBlock);
                DecodedObjectNode node = new DecodedObjectNode(columnarArray);
                node.addChild(decodeBlock(columnarArray.getElementsBlock()));
                return node;
            }
            else if (decodedBlock instanceof MapBlock) {
                ColumnarMap columnarMap = ColumnarMap.toColumnarMap(decodedBlock);
                DecodedObjectNode node = new DecodedObjectNode(columnarMap);
                node.addChild(decodeBlock(columnarMap.getKeysBlock()));
                node.addChild(decodeBlock(columnarMap.getValuesBlock()));
                return node;
            }
            else if (decodedBlock instanceof RowBlock) {
                ColumnarRow columnarRow = ColumnarRow.toColumnarRow(decodedBlock);
                DecodedObjectNode node = new DecodedObjectNode(columnarRow);
                for (int i = 0; i < columnarRow.getFieldCount(); i++) {
                    node.addChild(decodeBlock(columnarRow.getField(i)));
                }
                return node;
            }
            else if (decodedBlock instanceof DictionaryBlock) {
                DecodedObjectNode node = new DecodedObjectNode(decodedBlock);
                node.addChild(decodeBlock(((DictionaryBlock) decodedBlock).getDictionary()));
                return node;
            }
            else if (decodedBlock instanceof RunLengthEncodedBlock) {
                DecodedObjectNode node = new DecodedObjectNode(decodedBlock);
                node.addChild(decodeBlock(((RunLengthEncodedBlock) decodedBlock).getValue()));
                return node;
            }
            else {
                DecodedObjectNode node = new DecodedObjectNode(decodedBlock);
                return node;
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
            if (useOptmizedPartitionedOutputOperator) {
                if (force) {
                    for (int i = 0; i < partitionData.length; i++) {
                        PartitionData target = partitionData[i];
                        target.flush(outputBuffer);
                    }
                    return;
                }
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
