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

import com.facebook.presto.hive.HiveWriteUtils.FieldSetter;
import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageIndexer;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.columnar.OptimizedLazyBinaryColumnarSerde;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.common.util.ReflectionUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_READ_ONLY;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static com.facebook.presto.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.HiveType.toHiveTypes;
import static com.facebook.presto.hive.HiveWriteUtils.createFieldSetter;
import static com.facebook.presto.hive.HiveWriteUtils.getField;
import static com.facebook.presto.hive.HiveWriteUtils.getRowColumnInspectors;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;

public class HivePageSink
        implements ConnectorPageSink
{
    private static final int MAX_BUCKET_COUNT = 100000;
    private static final int BUCKET_NUMBER_PADDING = Integer.toString(MAX_BUCKET_COUNT - 1).length();

    private final String schemaName;
    private final String tableName;

    private final int[] dataColumnInputIndex; // ordinal of columns (not counting sample weight column)
    private final List<DataColumn> dataColumns;

    private final int[] partitionColumnsInputIndex; // ordinal of columns (not counting sample weight column)
    private final List<String> partitionColumnNames;
    private final List<Type> partitionColumnTypes;

    private final OptionalInt bucketCount;
    private final int[] bucketColumns;
    private final List<TypeInfo> bucketColumnTypes;

    private final HiveStorageFormat tableStorageFormat;
    private final HiveStorageFormat partitionStorageFormat;
    private final LocationHandle locationHandle;
    private final LocationService locationService;
    private final String filePrefix;

    private final HiveMetastore metastore;
    private final PageIndexer pageIndexer;
    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final JobConf conf;

    private final int maxOpenPartitions;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;

    private final List<Object> partitionRow;

    private final Table table;
    private final boolean immutablePartitions;
    private final boolean compress;

    private HiveRecordWriter[] writers;
    private final List<Int2ObjectMap<HiveRecordWriter>> bucketWriters;
    private int bucketWriterCount = 0;

    private final ConnectorSession session;

    public HivePageSink(
            String schemaName,
            String tableName,
            boolean isCreateTable,
            List<HiveColumnHandle> inputColumns,
            HiveStorageFormat tableStorageFormat,
            HiveStorageFormat partitionStorageFormat,
            LocationHandle locationHandle,
            LocationService locationService,
            String filePrefix,
            Optional<HiveBucketProperty> bucketProperty,
            HiveMetastore metastore,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            int maxOpenPartitions,
            boolean immutablePartitions,
            boolean compress,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ConnectorSession session)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");

        requireNonNull(inputColumns, "inputColumns is null");

        this.tableStorageFormat = requireNonNull(tableStorageFormat, "tableStorageFormat is null");
        this.partitionStorageFormat = requireNonNull(partitionStorageFormat, "partitionStorageFormat is null");
        this.locationHandle = requireNonNull(locationHandle, "locationHandle is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.filePrefix = requireNonNull(filePrefix, "filePrefix is null");

        this.metastore = requireNonNull(metastore, "metastore is null");

        requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.maxOpenPartitions = maxOpenPartitions;
        this.immutablePartitions = immutablePartitions;
        this.compress = compress;
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");

        // divide input columns into partition and data columns
        ImmutableList.Builder<String> partitionColumnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> partitionColumnTypes = ImmutableList.builder();
        ImmutableList.Builder<DataColumn> dataColumns = ImmutableList.builder();
        for (HiveColumnHandle column : inputColumns) {
            if (column.isPartitionKey()) {
                partitionColumnNames.add(column.getName());
                partitionColumnTypes.add(typeManager.getType(column.getTypeSignature()));
            }
            else {
                dataColumns.add(new DataColumn(column.getName(), typeManager.getType(column.getTypeSignature()), column.getHiveType()));
            }
        }
        this.partitionColumnNames = partitionColumnNames.build();
        this.partitionColumnTypes = partitionColumnTypes.build();
        this.dataColumns = dataColumns.build();

        // determine the input index of the partition columns and data columns
        // and determine the input index and type of bucketing columns
        ImmutableList.Builder<Integer> partitionColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> dataColumnsInputIndex = ImmutableList.builder();
        Object2IntMap<String> dataColumnNameToIdMap = new Object2IntOpenHashMap<>();
        Map<String, HiveType> dataColumnNameToTypeMap = new HashMap<>();
        // sample weight column is passed separately, so index must be calculated without this column
        List<HiveColumnHandle> inputColumnsWithoutSample = inputColumns.stream()
                .filter(column -> !column.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME))
                .collect(toList());
        for (int inputIndex = 0; inputIndex < inputColumnsWithoutSample.size(); inputIndex++) {
            HiveColumnHandle column = inputColumnsWithoutSample.get(inputIndex);
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
            checkArgument(bucketCount < MAX_BUCKET_COUNT, "bucketCount must be smaller than 100000");
            this.bucketCount = OptionalInt.of(bucketCount);
            this.bucketColumns = bucketProperty.get().getBucketedBy().stream()
                    .mapToInt(dataColumnNameToIdMap::get)
                    .toArray();
            this.bucketColumnTypes = bucketProperty.get().getBucketedBy().stream()
                    .map(dataColumnNameToTypeMap::get)
                    .map(HiveType::getTypeInfo)
                    .collect(Collectors.toList());
            bucketWriters = new ArrayList<>();
        }
        else {
            this.bucketCount = OptionalInt.empty();
            this.bucketColumns = null;
            this.bucketColumnTypes = null;
            bucketWriters = null;
            writers = new HiveRecordWriter[0];
        }

        this.pageIndexer = pageIndexerFactory.createPageIndexer(this.partitionColumnTypes);

        // preallocate temp space for partition and data
        this.partitionRow = Arrays.asList(new Object[this.partitionColumnNames.size()]);

        if (isCreateTable) {
            this.table = null;
            Optional<Path> writePath = locationService.writePathRoot(locationHandle);
            checkArgument(writePath.isPresent(), "CREATE TABLE must have a write path");
            conf = new JobConf(hdfsEnvironment.getConfiguration(writePath.get()));
        }
        else {
            Optional<Table> table = metastore.getTable(schemaName, tableName);
            if (!table.isPresent()) {
                throw new PrestoException(HIVE_INVALID_METADATA, format("Table %s.%s was dropped during insert", schemaName, tableName));
            }
            this.table = table.get();
            Path hdfsEnvironmentPath = locationService.writePathRoot(locationHandle).orElseGet(() -> locationService.targetPathRoot(locationHandle));
            conf = new JobConf(hdfsEnvironment.getConfiguration(hdfsEnvironmentPath));
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
            for (HiveRecordWriter writer : writers) {
                if (writer != null) {
                    writer.commit();
                    PartitionUpdate partitionUpdate = writer.getPartitionUpdate();
                    partitionUpdates.add(wrappedBuffer(partitionUpdateCodec.toJsonBytes(partitionUpdate)));
                }
            }
        }
        else {
            for (Int2ObjectMap<HiveRecordWriter> writers : bucketWriters) {
                PartitionUpdate firstPartitionUpdate = null;
                ImmutableList.Builder<String> fileNamesBuilder = ImmutableList.builder();
                for (HiveRecordWriter writer : writers.values()) {
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
            for (HiveRecordWriter writer : writers) {
                if (writer != null) {
                    writer.rollback();
                }
            }
        }
        else {
            for (Int2ObjectMap<HiveRecordWriter> writers : bucketWriters) {
                for (HiveRecordWriter writer : writers.values()) {
                    writer.rollback();
                }
            }
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page, Block sampleWeightBlock)
    {
        if (page.getPositionCount() == 0) {
            return NOT_BLOCKED;
        }

        Block[] dataBlocks = getDataBlocks(page, sampleWeightBlock);
        Block[] partitionBlocks = getPartitionBlocks(page);

        int[] indexes = pageIndexer.indexPage(new Page(page.getPositionCount(), partitionBlocks));
        if (pageIndexer.getMaxIndex() >= maxOpenPartitions) {
            throw new PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS, "Too many open partitions");
        }

        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#addRow or #createWriter
        return hdfsEnvironment.doAs(session.getUser(), () -> doAppend(page, dataBlocks, partitionBlocks, indexes));
    }

    private CompletableFuture<?> doAppend(Page page, Block[] dataBlocks, Block[] partitionBlocks, int[] indexes)
    {
        if (!bucketCount.isPresent()) {
            if (pageIndexer.getMaxIndex() >= writers.length) {
                writers = Arrays.copyOf(writers, pageIndexer.getMaxIndex() + 1);
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                int writerIndex = indexes[position];
                HiveRecordWriter writer = writers[writerIndex];
                if (writer == null) {
                    for (int field = 0; field < partitionBlocks.length; field++) {
                        Object value = getField(partitionColumnTypes.get(field), partitionBlocks[field], position);
                        partitionRow.set(field, value);
                    }
                    writer = createWriter(partitionRow, filePrefix + "_" + randomUUID());
                    writers[writerIndex] = writer;
                }

                writer.addRow(dataBlocks, position);
            }
        }
        else {
            int bucketCount = this.bucketCount.getAsInt();
            Block[] bucketBlocks = new Block[bucketColumns.length];
            for (int i = 0; i < bucketColumns.length; i++) {
                bucketBlocks[i] = page.getBlock(bucketColumns[i]);
            }
            Page bucketColumnsPage = new Page(page.getPositionCount(), bucketBlocks);

            for (int i = bucketWriters.size(); i <= pageIndexer.getMaxIndex(); i++) {
                bucketWriters.add(new Int2ObjectOpenHashMap<>());
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                int writerIndex = indexes[position];
                Int2ObjectMap<HiveRecordWriter> writers = bucketWriters.get(writerIndex);
                int bucket = HiveBucketing.getHiveBucket(bucketColumnTypes, bucketColumnsPage, position, bucketCount);
                HiveRecordWriter writer = writers.get(bucket);
                if (writer == null) {
                    if (bucketWriterCount >= maxOpenPartitions) {
                        throw new PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS, "Too many open writers for partitions and buckets");
                    }
                    bucketWriterCount++;
                    for (int field = 0; field < partitionBlocks.length; field++) {
                        Object value = getField(partitionColumnTypes.get(field), partitionBlocks[field], position);
                        partitionRow.set(field, value);
                    }
                    writer = createWriter(partitionRow, computeBucketedFileName(filePrefix, bucket));
                    writers.put(bucket, writer);
                }

                writer.addRow(dataBlocks, position);
            }
        }
        return NOT_BLOCKED;
    }

    public static String computeBucketedFileName(String filePrefix, int bucket)
    {
        return filePrefix + "_bucket-" + Strings.padStart(Integer.toString(bucket), BUCKET_NUMBER_PADDING, '0');
    }

    private HiveRecordWriter createWriter(List<Object> partitionRow, String fileName)
    {
        checkArgument(partitionRow.size() == partitionColumnNames.size(), "size of partitionRow is different from partitionColumnNames");

        List<String> partitionValues = partitionRow.stream()
                .map(value -> (value == null) ? HIVE_DEFAULT_DYNAMIC_PARTITION : value.toString())
                .collect(toList());

        Optional<String> partitionName;
        if (!partitionColumnNames.isEmpty()) {
            partitionName = Optional.of(FileUtils.makePartName(partitionColumnNames, partitionValues));
        }
        else {
            partitionName = Optional.empty();
        }

        // attempt to get the existing partition (if this is an existing partitioned table)
        Optional<Partition> partition = Optional.empty();
        if (!partitionRow.isEmpty() && table != null) {
            partition = metastore.getPartition(schemaName, tableName, partitionName.get());
        }

        boolean isNew;
        Properties schema;
        Path target;
        Path write;
        String outputFormat;
        String serDe;
        if (!partition.isPresent()) {
            if (table == null) {
                // Write to: a new partition in a new partitioned table,
                //           or a new unpartitioned table.
                isNew = true;
                schema = new Properties();
                schema.setProperty(META_TABLE_COLUMNS, dataColumns.stream()
                        .map(DataColumn::getName)
                        .collect(joining(",")));
                schema.setProperty(META_TABLE_COLUMN_TYPES, dataColumns.stream()
                        .map(DataColumn::getHiveType)
                        .map(HiveType::getHiveTypeName)
                        .collect(joining(":")));
                target = locationService.targetPath(locationHandle, partitionName);
                write = locationService.writePath(locationHandle, partitionName).get();

                if (partitionName.isPresent() && !target.equals(write)) {
                    // When target path is different from write path,
                    // verify that the target directory for the partition does not already exist
                    if (HiveWriteUtils.pathExists(session.getUser(), hdfsEnvironment, target)) {
                        throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for new partition '%s' of table '%s.%s' already exists: %s",
                                partitionName,
                                schemaName,
                                tableName,
                                target));
                    }
                }
            }
            else {
                // Write to: a new partition in an existing partitioned table,
                //           or an existing unpartitioned table
                if (partitionName.isPresent()) {
                    isNew = true;
                }
                else {
                    if (bucketCount.isPresent()) {
                        throw new PrestoException(HIVE_PARTITION_READ_ONLY, "Can not insert into bucketed unpartitioned Hive table");
                    }
                    if (immutablePartitions) {
                        throw new PrestoException(HIVE_PARTITION_READ_ONLY, "Unpartitioned Hive tables are immutable");
                    }
                    isNew = false;
                }
                schema = MetaStoreUtils.getSchema(
                        table.getSd(),
                        table.getSd(),
                        table.getParameters(),
                        schemaName,
                        tableName,
                        table.getPartitionKeys());
                target = locationService.targetPath(locationHandle, partitionName);
                write = locationService.writePath(locationHandle, partitionName).orElse(target);
            }

            if (partitionName.isPresent()) {
                // Write to a new partition
                outputFormat = partitionStorageFormat.getOutputFormat();
                serDe = partitionStorageFormat.getSerDe();
            }
            else {
                // Write to a new/existing unpartitioned table
                outputFormat = tableStorageFormat.getOutputFormat();
                serDe = tableStorageFormat.getSerDe();
            }
        }
        else {
            // Write to: an existing partition in an existing partitioned table,
            if (bucketCount.isPresent()) {
                throw new PrestoException(HIVE_PARTITION_READ_ONLY, "Can not insert into existing partitions of bucketed Hive table");
            }
            if (immutablePartitions) {
                throw new PrestoException(HIVE_PARTITION_READ_ONLY, "Hive partitions are immutable");
            }
            isNew = false;

            // Append to an existing partition
            HiveWriteUtils.checkPartitionIsWritable(partitionName.get(), partition.get());

            StorageDescriptor storageDescriptor = partition.get().getSd();
            outputFormat = storageDescriptor.getOutputFormat();
            serDe = storageDescriptor.getSerdeInfo().getSerializationLib();
            schema = MetaStoreUtils.getSchema(partition.get(), table);

            target = locationService.targetPath(locationHandle, partition.get(), partitionName.get());
            write = locationService.writePath(locationHandle, partitionName).orElse(target);
        }
        return new HiveRecordWriter(
                schemaName,
                tableName,
                partitionName.orElse(""),
                compress,
                isNew,
                dataColumns,
                outputFormat,
                serDe,
                schema,
                fileName + getFileExtension(conf, outputFormat),
                write.toString(),
                target.toString(),
                typeManager,
                conf);
    }

    static String getFileExtension(JobConf conf, String outputFormat)
    {
        // text format files must have the correct extension when compressed
        if (!HiveConf.getBoolVar(conf, COMPRESSRESULT) || !HiveIgnoreKeyTextOutputFormat.class.getName().equals(outputFormat)) {
            return "";
        }

        String compressionCodecClass = conf.get("mapred.output.compression.codec");
        if (compressionCodecClass == null) {
            return new DefaultCodec().getDefaultExtension();
        }

        try {
            Class<? extends CompressionCodec> codecClass = conf.getClassByName(compressionCodecClass).asSubclass(CompressionCodec.class);
            return ReflectionUtil.newInstance(codecClass, conf).getDefaultExtension();
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Compression codec not found: " + compressionCodecClass, e);
        }
        catch (RuntimeException e) {
            throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Failed to load compression codec: " + compressionCodecClass, e);
        }
    }

    private Block[] getDataBlocks(Page page, Block sampleWeightBlock)
    {
        Block[] blocks = new Block[dataColumnInputIndex.length + (sampleWeightBlock != null ? 1 : 0)];
        for (int i = 0; i < dataColumnInputIndex.length; i++) {
            int dataColumn = dataColumnInputIndex[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        if (sampleWeightBlock != null) {
            // sample weight block is always last
            blocks[blocks.length - 1] = sampleWeightBlock;
        }
        return blocks;
    }

    private Block[] getPartitionBlocks(Page page)
    {
        Block[] blocks = new Block[partitionColumnsInputIndex.length];
        for (int i = 0; i < partitionColumnsInputIndex.length; i++) {
            int dataColumn = partitionColumnsInputIndex[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return blocks;
    }

    @VisibleForTesting
    public static class HiveRecordWriter
    {
        private final String partitionName;
        private final boolean isNew;
        private final String fileName;
        private final String writePath;
        private final String targetPath;
        private final int fieldCount;
        @SuppressWarnings("deprecation")
        private final Serializer serializer;
        private final RecordWriter recordWriter;
        private final SettableStructObjectInspector tableInspector;
        private final List<StructField> structFields;
        private final Object row;
        private final FieldSetter[] setters;

        public HiveRecordWriter(
                String schemaName,
                String tableName,
                String partitionName,
                boolean compress,
                boolean isNew,
                List<DataColumn> inputColumns,
                String outputFormat,
                String serDe,
                Properties schema,
                String fileName,
                String writePath,
                String targetPath,
                TypeManager typeManager,
                JobConf conf)
        {
            this.partitionName = partitionName;
            this.isNew = isNew;
            this.fileName = fileName;
            this.writePath = writePath;
            this.targetPath = targetPath;

            // existing tables may have columns in a different order
            List<String> fileColumnNames = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(schema.getProperty(META_TABLE_COLUMNS, ""));
            List<HiveType> fileColumnHiveTypes = toHiveTypes(schema.getProperty(META_TABLE_COLUMN_TYPES, ""));

            // verify we can write all input columns to the file
            Map<String, DataColumn> inputColumnMap = inputColumns.stream()
                    .collect(toMap(DataColumn::getName, identity()));
            Set<String> missingColumns = Sets.difference(inputColumnMap.keySet(), new HashSet<>(fileColumnNames));
            if (!missingColumns.isEmpty()) {
                throw new PrestoException(NOT_FOUND, format("Table %s.%s does not have columns %s", schema, tableName, missingColumns));
            }
            if (fileColumnNames.size() != fileColumnHiveTypes.size()) {
                throw new PrestoException(HIVE_INVALID_METADATA, format("Partition '%s' in table '%s.%s' has mismatched metadata for column names and types",
                        partitionName,
                        schemaName,
                        tableName));
            }

            // verify the file types match the input type
            // todo adapt input types to the file types as Hive does
            for (int fileIndex = 0; fileIndex < fileColumnNames.size(); fileIndex++) {
                String columnName = fileColumnNames.get(fileIndex);
                HiveType fileColumnHiveType = fileColumnHiveTypes.get(fileIndex);
                HiveType inputHiveType = inputColumnMap.get(columnName).getHiveType();

                if (!fileColumnHiveType.equals(inputHiveType)) {
                    // todo this should be moved to a helper
                    throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("" +
                                    "There is a mismatch between the table and partition schemas. " +
                                    "The column '%s' in table '%s.%s' is declared as type '%s', " +
                                    "but partition '%s' declared column '%s' as type '%s'.",
                            columnName,
                            schemaName,
                            tableName,
                            inputHiveType,
                            partitionName,
                            columnName,
                            fileColumnHiveType));
                }
            }

            fieldCount = fileColumnNames.size();

            if (serDe.equals(org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe.class.getName())) {
                serDe = OptimizedLazyBinaryColumnarSerde.class.getName();
            }
            serializer = initializeSerializer(conf, schema, serDe);
            recordWriter = HiveWriteUtils.createRecordWriter(new Path(writePath, fileName), conf, compress, schema, outputFormat);

            List<Type> fileColumnTypes = fileColumnHiveTypes.stream()
                    .map(hiveType -> hiveType.getType(typeManager))
                    .collect(toList());
            tableInspector = getStandardStructObjectInspector(fileColumnNames, getRowColumnInspectors(fileColumnTypes));

            // reorder (and possibly reduce) struct fields to match input
            structFields = ImmutableList.copyOf(inputColumns.stream()
                            .map(DataColumn::getName)
                            .map(tableInspector::getStructFieldRef)
                            .collect(toList()));

            row = tableInspector.create();

            setters = new FieldSetter[structFields.size()];
            for (int i = 0; i < setters.length; i++) {
                setters[i] = createFieldSetter(tableInspector, row, structFields.get(i), inputColumns.get(i).getType());
            }
        }

        public void addRow(Block[] columns, int position)
        {
            for (int field = 0; field < fieldCount; field++) {
                if (columns[field].isNull(position)) {
                    tableInspector.setStructFieldData(row, structFields.get(field), null);
                }
                else {
                    setters[field].setField(columns[field], position);
                }
            }

            try {
                recordWriter.write(serializer.serialize(row, tableInspector));
            }
            catch (SerDeException | IOException e) {
                throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
            }
        }

        public void commit()
        {
            try {
                recordWriter.close(false);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
            }
        }

        public void rollback()
        {
            try {
                recordWriter.close(true);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
            }
        }

        public PartitionUpdate getPartitionUpdate()
        {
            return new PartitionUpdate(
                    partitionName,
                    isNew,
                    writePath,
                    targetPath,
                    ImmutableList.of(fileName));
        }

        @SuppressWarnings("deprecation")
        private static Serializer initializeSerializer(Configuration conf, Properties properties, String serializerName)
        {
            try {
                Serializer result = (Serializer) Class.forName(serializerName).getConstructor().newInstance();
                result.initialize(conf, properties);
                return result;
            }
            catch (SerDeException | ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("partitionName", partitionName)
                    .add("writePath", writePath)
                    .add("fileName", fileName)
                    .toString();
        }
    }

    @VisibleForTesting
    public static class DataColumn
    {
        private final String name;
        private final Type type;
        private final HiveType hiveType;

        public DataColumn(String name, Type type, HiveType hiveType)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
            this.hiveType = requireNonNull(hiveType, "hiveType is null");
        }

        public String getName()
        {
            return name;
        }

        public Type getType()
        {
            return type;
        }

        public HiveType getHiveType()
        {
            return hiveType;
        }
    }
}
