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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePageSink
        implements ConnectorPageSink
{
    private final HiveWriterFactory writerFactory;

    private final int[] dataColumnInputIndex; // ordinal of columns (not counting sample weight column)
    private final int[] partitionColumnsInputIndex; // ordinal of columns (not counting sample weight column)

    private final OptionalInt bucketCount;
    private final int[] bucketColumns;
    private final HiveBucketFunction bucketFunction;

    private final PageIndexer pageIndexer;
    private final HdfsEnvironment hdfsEnvironment;

    private final int maxOpenPartitions;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;

    private HiveWriter[] writers;
    private final List<Int2ObjectMap<HiveWriter>> bucketWriters;
    private int bucketWriterCount;

    private final ConnectorSession session;

    public HivePageSink(
            HiveWriterFactory writerFactory,
            List<HiveColumnHandle> inputColumns,
            Optional<HiveBucketProperty> bucketProperty,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            int maxOpenPartitions,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ConnectorSession session)
    {
        this.writerFactory = requireNonNull(writerFactory, "writerFactory is null");

        requireNonNull(inputColumns, "inputColumns is null");

        requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.maxOpenPartitions = maxOpenPartitions;
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");

        // divide input columns into partition and data columns
        ImmutableList.Builder<Type> partitionColumnTypes = ImmutableList.builder();
        for (HiveColumnHandle column : inputColumns) {
            if (column.isPartitionKey()) {
                partitionColumnTypes.add(typeManager.getType(column.getTypeSignature()));
            }
        }
        this.pageIndexer = pageIndexerFactory.createPageIndexer(partitionColumnTypes.build());

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

        requireNonNull(bucketProperty, "bucketProperty is null");
        if (bucketProperty.isPresent()) {
            int bucketCount = bucketProperty.get().getBucketCount();
            this.bucketCount = OptionalInt.of(bucketCount);
            this.bucketColumns = bucketProperty.get().getBucketedBy().stream()
                    .mapToInt(dataColumnNameToIdMap::get)
                    .toArray();
            List<HiveType> bucketColumnTypes = bucketProperty.get().getBucketedBy().stream()
                    .map(dataColumnNameToTypeMap::get)
                    .collect(toList());
            bucketFunction = new HiveBucketFunction(bucketCount, bucketColumnTypes);
            bucketWriters = new ArrayList<>();
        }
        else {
            this.bucketCount = OptionalInt.empty();
            this.bucketColumns = null;
            bucketFunction = null;
            bucketWriters = null;
            writers = new HiveWriter[0];
        }

        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public Collection<Slice> finish()
    {
        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#commit -> RecordWriter#close
        return hdfsEnvironment.doAs(session.getUser(), this::doFinish);
    }

    private ImmutableList<Slice> doFinish()
    {
        ImmutableList.Builder<Slice> partitionUpdates = ImmutableList.builder();
        if (!bucketCount.isPresent()) {
            for (HiveWriter writer : writers) {
                if (writer != null) {
                    writer.commit();
                    PartitionUpdate partitionUpdate = writer.getPartitionUpdate();
                    partitionUpdates.add(wrappedBuffer(partitionUpdateCodec.toJsonBytes(partitionUpdate)));
                }
            }
        }
        else {
            for (Int2ObjectMap<HiveWriter> writers : bucketWriters) {
                PartitionUpdate firstPartitionUpdate = null;
                ImmutableList.Builder<String> fileNamesBuilder = ImmutableList.builder();
                for (HiveWriter writer : writers.values()) {
                    writer.commit();
                    PartitionUpdate partitionUpdate = writer.getPartitionUpdate();
                    if (firstPartitionUpdate == null) {
                        firstPartitionUpdate = partitionUpdate;
                    }
                    else {
                        verify(firstPartitionUpdate.getName().equals(partitionUpdate.getName()));
                        verify(firstPartitionUpdate.isNew() == partitionUpdate.isNew());
                        verify(firstPartitionUpdate.getTargetPath().equals(partitionUpdate.getTargetPath()));
                        verify(firstPartitionUpdate.getWritePath().equals(partitionUpdate.getWritePath()));
                    }
                    fileNamesBuilder.addAll(partitionUpdate.getFileNames());
                }
                if (firstPartitionUpdate == null) {
                    continue;
                }
                partitionUpdates.add(wrappedBuffer(partitionUpdateCodec.toJsonBytes(new PartitionUpdate(
                        firstPartitionUpdate.getName(),
                        firstPartitionUpdate.isNew(),
                        firstPartitionUpdate.getWritePath(),
                        firstPartitionUpdate.getTargetPath(),
                        fileNamesBuilder.build()))));
            }
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
        if (!bucketCount.isPresent()) {
            for (HiveWriter writer : writers) {
                if (writer != null) {
                    writer.rollback();
                }
            }
        }
        else {
            for (Int2ObjectMap<HiveWriter> writers : bucketWriters) {
                for (HiveWriter writer : writers.values()) {
                    writer.rollback();
                }
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
        Page partitionColumns = extractColumns(page, partitionColumnsInputIndex);
        int[] indexes = pageIndexer.indexPage(partitionColumns);
        if (pageIndexer.getMaxIndex() >= maxOpenPartitions) {
            throw new PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS, "Too many open partitions");
        }

        Block[] dataBlocks = getDataBlocks(page);
        if (!bucketCount.isPresent()) {
            if (pageIndexer.getMaxIndex() >= writers.length) {
                writers = Arrays.copyOf(writers, pageIndexer.getMaxIndex() + 1);
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                int writerIndex = indexes[position];
                HiveWriter writer = writers[writerIndex];
                if (writer == null) {
                    writer = writerFactory.createWriter(partitionColumns, position, OptionalInt.empty());
                    writers[writerIndex] = writer;
                }

                writer.addRow(dataBlocks, position);
            }
        }
        else {
            for (int i = bucketWriters.size(); i <= pageIndexer.getMaxIndex(); i++) {
                bucketWriters.add(new Int2ObjectOpenHashMap<>());
            }

            Page bucketColumnsPage = extractColumns(page, bucketColumns);
            for (int position = 0; position < page.getPositionCount(); position++) {
                int writerIndex = indexes[position];
                Int2ObjectMap<HiveWriter> writers = bucketWriters.get(writerIndex);
                int bucket = bucketFunction.getBucket(bucketColumnsPage, position);
                HiveWriter writer = writers.get(bucket);
                if (writer == null) {
                    if (bucketWriterCount >= maxOpenPartitions) {
                        throw new PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS, "Too many open writers for partitions and buckets");
                    }
                    bucketWriterCount++;
                    writer = writerFactory.createWriter(partitionColumns, position, OptionalInt.of(bucket));
                    writers.put(bucket, writer);
                }

                writer.addRow(dataBlocks, position);
            }
        }
        return NOT_BLOCKED;
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

    private static Page extractColumns(Page page, int[] columns)
    {
        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int dataColumn = columns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }
}
