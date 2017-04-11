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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageIndexer;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.IntArrayBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(HivePageSink.class);

    private static final int MAX_PAGE_POSITIONS = 4096;

    private final HiveWriterFactory writerFactory;

    private final int[] dataColumnInputIndex; // ordinal of columns (not counting sample weight column)
    private final int[] partitionColumnsInputIndex; // ordinal of columns (not counting sample weight column)

    private final int[] bucketColumns;
    private final HiveBucketFunction bucketFunction;

    private final HiveWriterPagePartitioner pagePartitioner;
    private final HdfsEnvironment hdfsEnvironment;

    private final int maxOpenWriters;
    private final ListeningExecutorService writeVerificationExecutor;

    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;

    private final List<HiveWriter> writers = new ArrayList<>();
    private final List<WriterPositions> writerPositions = new ArrayList<>();

    private final ConnectorSession session;

    private long systemMemoryUsage;

    public HivePageSink(
            HiveWriterFactory writerFactory,
            List<HiveColumnHandle> inputColumns,
            Optional<HiveBucketProperty> bucketProperty,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            int maxOpenWriters,
            ListeningExecutorService writeVerificationExecutor,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ConnectorSession session)
    {
        this.writerFactory = requireNonNull(writerFactory, "writerFactory is null");

        requireNonNull(inputColumns, "inputColumns is null");

        requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.maxOpenWriters = maxOpenWriters;
        this.writeVerificationExecutor = requireNonNull(writeVerificationExecutor, "writeVerificationExecutor is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");

        requireNonNull(bucketProperty, "bucketProperty is null");
        this.pagePartitioner = new HiveWriterPagePartitioner(
                inputColumns,
                bucketProperty.isPresent(),
                pageIndexerFactory,
                typeManager);

        // determine the input index of the partition columns and data columns
        // and determine the input index and type of bucketing columns
        ImmutableList.Builder<Integer> partitionColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> dataColumnsInputIndex = ImmutableList.builder();
        Object2IntMap<String> dataColumnNameToIdMap = new Object2IntOpenHashMap<>();
        Map<String, HiveType> dataColumnNameToTypeMap = new HashMap<>();
        // sample weight column is passed separately, so index must be calculated without this column
        for (int inputIndex = 0; inputIndex < inputColumns.size(); inputIndex++) {
            HiveColumnHandle column = inputColumns.get(inputIndex);
            if (column.isPartitionKey()) {
                partitionColumns.add(inputIndex);
            }
            else {
                dataColumnsInputIndex.add(inputIndex);
                dataColumnNameToIdMap.put(column.getName(), inputIndex);
                dataColumnNameToTypeMap.put(column.getName(), column.getHiveType());
            }
        }
        this.partitionColumnsInputIndex = Ints.toArray(partitionColumns.build());
        this.dataColumnInputIndex = Ints.toArray(dataColumnsInputIndex.build());

        if (bucketProperty.isPresent()) {
            int bucketCount = bucketProperty.get().getBucketCount();
            bucketColumns = bucketProperty.get().getBucketedBy().stream()
                    .mapToInt(dataColumnNameToIdMap::get)
                    .toArray();
            List<HiveType> bucketColumnTypes = bucketProperty.get().getBucketedBy().stream()
                    .map(dataColumnNameToTypeMap::get)
                    .collect(toList());
            bucketFunction = new HiveBucketFunction(bucketCount, bucketColumnTypes);
        }
        else {
            bucketColumns = null;
            bucketFunction = null;
        }

        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryUsage;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#commit -> RecordWriter#close
        ListenableFuture<Collection<Slice>> result = hdfsEnvironment.doAs(session.getUser(), this::doFinish);
        return MoreFutures.toCompletableFuture(result);
    }

    private ListenableFuture<Collection<Slice>> doFinish()
    {
        ImmutableList.Builder<Slice> partitionUpdates = ImmutableList.builder();
        List<Callable<Object>> verificationTasks = new ArrayList<>();
        for (HiveWriter writer : writers) {
            writer.commit();
            PartitionUpdate partitionUpdate = writer.getPartitionUpdate();
            partitionUpdates.add(wrappedBuffer(partitionUpdateCodec.toJsonBytes(partitionUpdate)));
            writer.getVerificationTask()
                    .map(Executors::callable)
                    .ifPresent(verificationTasks::add);
        }
        List<Slice> result = partitionUpdates.build();

        if (verificationTasks.isEmpty()) {
            return Futures.immediateFuture(result);
        }

        try {
            List<ListenableFuture<?>> futures = writeVerificationExecutor.invokeAll(verificationTasks).stream()
                    .map(future -> (ListenableFuture<?>) future)
                    .collect(toList());
            return Futures.transform(Futures.allAsList(futures), input -> result);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort()
    {
        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#rollback -> RecordWriter#close
        hdfsEnvironment.doAs(session.getUser(), this::doAbort);
    }

    private void doAbort()
    {
        Optional<Exception> rollbackException = Optional.empty();
        for (HiveWriter writer : writers) {
            // writers can contain nulls if an exception is thrown when doAppend expends the writer list
            if (writer != null) {
                try {
                    writer.rollback();
                }
                catch (Exception e) {
                    log.warn("exception '%s' while rollback on %s", e, writer);
                    rollbackException = Optional.of(e);
                }
            }
        }
        if (rollbackException.isPresent()) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", rollbackException.get());
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() > 0) {
            // Must be wrapped in doAs entirely
            // Implicit FileSystem initializations are possible in HiveRecordWriter#addRow or #createWriter
            hdfsEnvironment.doAs(session.getUser(), () -> doAppend(page));
        }

        return NOT_BLOCKED;
    }

    private void doAppend(Page page)
    {
        while (page.getPositionCount() > MAX_PAGE_POSITIONS) {
            Page chunk = page.getRegion(0, MAX_PAGE_POSITIONS);
            page = page.getRegion(MAX_PAGE_POSITIONS, page.getPositionCount() - MAX_PAGE_POSITIONS);
            writePage(chunk);
        }

        writePage(page);
    }

    private void writePage(Page page)
    {
        int[] writerIndexes = getWriterIndexes(page);

        // record which positions are used by which writer
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            writerPositions.get(writerIndex).add(position);
        }

        // invoke the writers
        Page dataPage = getDataPage(page);
        IntSet writersUsed = new IntArraySet(writerIndexes);
        for (IntIterator iterator = writersUsed.iterator(); iterator.hasNext(); ) {
            int writerIndex = iterator.nextInt();
            WriterPositions currentWriterPositions = writerPositions.get(writerIndex);
            if (currentWriterPositions.isEmpty()) {
                continue;
            }

            // If write is partitioned across multiple writers, filter page using dictionary blocks
            Page pageForWriter = dataPage;
            if (currentWriterPositions.size() != dataPage.getPositionCount()) {
                Block[] blocks = new Block[dataPage.getChannelCount()];
                for (int channel = 0; channel < dataPage.getChannelCount(); channel++) {
                    blocks[channel] = new DictionaryBlock(currentWriterPositions.size(), dataPage.getBlock(channel), currentWriterPositions.getPositionsArray());
                }
                pageForWriter = new Page(currentWriterPositions.size(), blocks);
            }

            HiveWriter writer = writers.get(writerIndex);

            long currentMemory = writer.getSystemMemoryUsage();
            writer.append(pageForWriter);
            systemMemoryUsage += (writer.getSystemMemoryUsage() - currentMemory);

            currentWriterPositions.clear();
        }
    }

    private int[] getWriterIndexes(Page page)
    {
        Page partitionColumns = extractColumns(page, partitionColumnsInputIndex);
        Block bucketBlock = buildBucketBlock(page);
        int[] writerIndexes = pagePartitioner.partitionPage(partitionColumns, bucketBlock);
        if (pagePartitioner.getMaxIndex() >= maxOpenWriters) {
            throw new PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS, "Too many open partitions");
        }

        // expand writers list to new size
        while (writers.size() <= pagePartitioner.getMaxIndex()) {
            writers.add(null);
            WriterPositions newWriterPositions = new WriterPositions();
            systemMemoryUsage += sizeOf(newWriterPositions.getPositionsArray());
            writerPositions.add(newWriterPositions);
        }

        // create missing writers
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            if (writers.get(writerIndex) != null) {
                continue;
            }

            OptionalInt bucketNumber = OptionalInt.empty();
            if (bucketBlock != null) {
                bucketNumber = OptionalInt.of(bucketBlock.getInt(position, 0));
            }
            HiveWriter writer = writerFactory.createWriter(partitionColumns, position, bucketNumber);
            writers.set(writerIndex, writer);
        }
        verify(writers.size() == pagePartitioner.getMaxIndex() + 1);
        verify(!writers.contains(null));

        return writerIndexes;
    }

    private Page getDataPage(Page page)
    {
        Block[] blocks = new Block[dataColumnInputIndex.length];
        for (int i = 0; i < dataColumnInputIndex.length; i++) {
            int dataColumn = dataColumnInputIndex[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private Block buildBucketBlock(Page page)
    {
        if (bucketFunction == null) {
            return null;
        }

        IntArrayBlockBuilder bucketColumnBuilder = new IntArrayBlockBuilder(new BlockBuilderStatus(), page.getPositionCount());
        Page bucketColumnsPage = extractColumns(page, bucketColumns);
        for (int position = 0; position < page.getPositionCount(); position++) {
            int bucket = bucketFunction.getBucket(bucketColumnsPage, position);
            bucketColumnBuilder.writeInt(bucket);
        }
        return bucketColumnBuilder.build();
    }

    private static Page extractColumns(Page page, int[] columns)
    {
        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int dataColumn = columns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private static class HiveWriterPagePartitioner
    {
        private final PageIndexer pageIndexer;

        public HiveWriterPagePartitioner(
                List<HiveColumnHandle> inputColumns,
                boolean bucketed,
                PageIndexerFactory pageIndexerFactory,
                TypeManager typeManager)
        {
            requireNonNull(inputColumns, "inputColumns is null");
            requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

            List<Type> partitionColumnTypes = inputColumns.stream()
                    .filter(HiveColumnHandle::isPartitionKey)
                    .map(column -> typeManager.getType(column.getTypeSignature()))
                    .collect(toList());

            if (bucketed) {
                partitionColumnTypes.add(INTEGER);
            }

            this.pageIndexer = pageIndexerFactory.createPageIndexer(partitionColumnTypes);
        }

        public int[] partitionPage(Page partitionColumns, Block bucketBlock)
        {
            if (bucketBlock != null) {
                Block[] blocks = new Block[partitionColumns.getChannelCount() + 1];
                for (int i = 0; i < partitionColumns.getChannelCount(); i++) {
                    blocks[i] = partitionColumns.getBlock(i);
                }
                blocks[blocks.length - 1] = bucketBlock;
                partitionColumns = new Page(partitionColumns.getPositionCount(), blocks);
            }
            return pageIndexer.indexPage(partitionColumns);
        }

        public int getMaxIndex()
        {
            return pageIndexer.getMaxIndex();
        }
    }

    private static final class WriterPositions
    {
        private final int[] positions = new int[MAX_PAGE_POSITIONS];
        private int size;

        public boolean isEmpty()
        {
            return size == 0;
        }

        public int size()
        {
            return size;
        }

        public int[] getPositionsArray()
        {
            return positions;
        }

        public void add(int position)
        {
            checkArgument(size < positions.length, "Too many page positions");
            positions[size] = position;
            size++;
        }

        public void clear()
        {
            size = 0;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("size", size)
                    .toString();
        }
    }
}
