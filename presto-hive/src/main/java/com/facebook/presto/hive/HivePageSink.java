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
import com.facebook.presto.spi.block.IntArrayBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

public class HivePageSink
        implements ConnectorPageSink
{
    private final HiveWriterFactory writerFactory;

    private final int[] dataColumnInputIndex; // ordinal of columns (not counting sample weight column)
    private final int[] partitionColumnsInputIndex; // ordinal of columns (not counting sample weight column)

    private final int[] bucketColumns;
    private final HiveBucketFunction bucketFunction;

    private final HiveWriterPagePartitioner pagePartitioner;
    private final HdfsEnvironment hdfsEnvironment;

    private final int maxOpenWriters;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;

    private final List<HiveWriter> writers = new ArrayList<>();

    private final ConnectorSession session;

    public HivePageSink(
            HiveWriterFactory writerFactory,
            List<HiveColumnHandle> inputColumns,
            Optional<HiveBucketProperty> bucketProperty,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            int maxOpenWriters,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ConnectorSession session)
    {
        this.writerFactory = requireNonNull(writerFactory, "writerFactory is null");

        requireNonNull(inputColumns, "inputColumns is null");

        requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.maxOpenWriters = maxOpenWriters;
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
    public CompletableFuture<Collection<Slice>> finish()
    {
        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#commit -> RecordWriter#close
        Collection<Slice> result = hdfsEnvironment.doAs(session.getUser(), this::doFinish);
        return completedFuture(result);
    }

    private ImmutableList<Slice> doFinish()
    {
        ImmutableList.Builder<Slice> partitionUpdates = ImmutableList.builder();
        for (HiveWriter writer : writers) {
            writer.commit();
            PartitionUpdate partitionUpdate = writer.getPartitionUpdate();
            partitionUpdates.add(wrappedBuffer(partitionUpdateCodec.toJsonBytes(partitionUpdate)));
        }
        return partitionUpdates.build();
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
        for (HiveWriter writer : writers) {
            // writers can contain nulls if an exception is thrown when doAppend expends the writer list
            if (writer != null) {
                writer.rollback();
            }
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#addRow or #createWriter
        return hdfsEnvironment.doAs(session.getUser(), () -> doAppend(page));
    }

    private CompletableFuture<?> doAppend(Page page)
    {
        int[] writerIndexes = getWriterIndexes(page);

        Block[] dataBlocks = getDataBlocks(page);
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            HiveWriter writer = writers.get(writerIndex);
            writer.addRow(dataBlocks, position);
        }

        return NOT_BLOCKED;
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

    private Block[] getDataBlocks(Page page)
    {
        Block[] blocks = new Block[dataColumnInputIndex.length];
        for (int i = 0; i < dataColumnInputIndex.length; i++) {
            int dataColumn = dataColumnInputIndex[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return blocks;
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
}
