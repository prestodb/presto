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

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageIndexer;
import com.facebook.presto.spi.PageIndexerFactory;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
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
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.common.util.ReflectionUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_READ_ONLY;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_ERROR;
import static com.facebook.presto.hive.HiveType.toHiveTypes;
import static com.facebook.presto.hive.HiveWriteUtils.getField;
import static com.facebook.presto.hive.HiveWriteUtils.getJavaObjectInspectors;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;

public class HivePageSink
        implements ConnectorPageSink
{
    private final String schemaName;
    private final String tableName;

    private final int[] dataColumns;
    private final List<String> dataColumnNames;
    private final List<Type> dataColumnTypes;

    private final int[] partitionColumns;
    private final List<String> partitionColumnNames;
    private final List<Type> partitionColumnTypes;

    private final HiveStorageFormat tableStorageFormat;
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

    private final List<Object> dataRow;
    private final List<Object> partitionRow;

    private final Table table;
    private final boolean immutablePartitions;
    private final boolean respectTableFormat;

    private HiveRecordWriter[] writers = new HiveRecordWriter[0];

    public HivePageSink(
            String schemaName,
            String tableName,
            boolean isCreateTable,
            List<HiveColumnHandle> inputColumns,
            HiveStorageFormat tableStorageFormat,
            LocationHandle locationHandle,
            LocationService locationService,
            String filePrefix,
            HiveMetastore metastore,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            boolean respectTableFormat,
            int maxOpenPartitions,
            boolean immutablePartitions,
            JsonCodec<PartitionUpdate> partitionUpdateCodec)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");

        requireNonNull(inputColumns, "inputColumns is null");

        this.tableStorageFormat = requireNonNull(tableStorageFormat, "tableStorageFormat is null");
        this.locationHandle = requireNonNull(locationHandle, "locationHandle is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.filePrefix = requireNonNull(filePrefix, "filePrefix is null");

        this.metastore = requireNonNull(metastore, "metastore is null");

        requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.respectTableFormat = respectTableFormat;
        this.maxOpenPartitions = maxOpenPartitions;
        this.immutablePartitions = immutablePartitions;
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");

        // divide input columns into partition and data columns
        ImmutableList.Builder<String> partitionColumnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> partitionColumnTypes = ImmutableList.builder();
        ImmutableList.Builder<String> dataColumnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> dataColumnTypes = ImmutableList.builder();
        for (HiveColumnHandle column : inputColumns) {
            if (column.isPartitionKey()) {
                partitionColumnNames.add(column.getName());
                partitionColumnTypes.add(typeManager.getType(column.getTypeSignature()));
            }
            else {
                dataColumnNames.add(column.getName());
                dataColumnTypes.add(typeManager.getType(column.getTypeSignature()));
            }
        }
        this.partitionColumnNames = partitionColumnNames.build();
        this.partitionColumnTypes = partitionColumnTypes.build();
        this.dataColumnNames = dataColumnNames.build();
        this.dataColumnTypes = dataColumnTypes.build();

        // determine the input index of the partition columns and data columns
        ImmutableList.Builder<Integer> partitionColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> dataColumns = ImmutableList.builder();
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
                dataColumns.add(inputIndex);
            }
        }
        this.partitionColumns = Ints.toArray(partitionColumns.build());
        this.dataColumns = Ints.toArray(dataColumns.build());

        this.pageIndexer = pageIndexerFactory.createPageIndexer(this.partitionColumnTypes);

        // preallocate temp space for partition and data
        this.partitionRow = Arrays.asList(new Object[this.partitionColumnNames.size()]);
        this.dataRow = Arrays.asList(new Object[this.dataColumnNames.size()]);

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
    }

    @Override
    public Collection<Slice> commit()
    {
        ImmutableList.Builder<Slice> partitionUpdates = ImmutableList.builder();
        for (HiveRecordWriter writer : writers) {
            if (writer != null) {
                writer.commit();
                PartitionUpdate partitionUpdate = writer.getPartitionUpdate();
                partitionUpdates.add(wrappedBuffer(partitionUpdateCodec.toJsonBytes(partitionUpdate)));
            }
        }
        return partitionUpdates.build();
    }

    @Override
    public void rollback()
    {
        for (HiveRecordWriter writer : writers) {
            if (writer != null) {
                writer.rollback();
            }
        }
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock)
    {
        if (page.getPositionCount() == 0) {
            return;
        }

        Block[] dataBlocks = getDataBlocks(page, sampleWeightBlock);
        Block[] partitionBlocks = getPartitionBlocks(page);

        int[] indexes = pageIndexer.indexPage(new Page(page.getPositionCount(), partitionBlocks));
        if (pageIndexer.getMaxIndex() >= maxOpenPartitions) {
            throw new PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS, "Too many open partitions");
        }
        if (pageIndexer.getMaxIndex() >= writers.length) {
            writers = Arrays.copyOf(writers, pageIndexer.getMaxIndex() + 1);
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = indexes[position];
            HiveRecordWriter writer = writers[writerIndex];
            if (writer == null) {
                buildRow(partitionColumnTypes, partitionRow, partitionBlocks, position);
                writer = createWriter(partitionRow);
                writers[writerIndex] = writer;
            }

            buildRow(dataColumnTypes, dataRow, dataBlocks, position);
            writer.addRow(dataRow);
        }
    }

    private HiveRecordWriter createWriter(List<Object> partitionRow)
    {
        checkArgument(partitionRow.size() == partitionColumnNames.size(), "size of partitionRow is different from partitionColumnNames");

        List<String> partitionValues = partitionRow.stream()
                .map(Object::toString) // todo this seems wrong
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
                schema.setProperty(META_TABLE_COLUMNS, Joiner.on(',').join(dataColumnNames));
                schema.setProperty(META_TABLE_COLUMN_TYPES, dataColumnTypes.stream()
                        .map(HiveType::toHiveType)
                        .map(HiveType::getHiveTypeName)
                        .collect(Collectors.joining(":")));
                target = locationService.targetPath(locationHandle, partitionName);
                write = locationService.writePath(locationHandle, partitionName).get();

                if (partitionName.isPresent()) {
                    // verify the target directory for the partition does not already exist
                    if (HiveWriteUtils.pathExists(hdfsEnvironment, target)) {
                        throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format("Target directory for new partition '%s' of table '%s.%s' already exists: %s",
                                partitionName,
                                schemaName,
                                tableName,
                                target));
                    }
                }
                outputFormat = tableStorageFormat.getOutputFormat();
                serDe = tableStorageFormat.getSerDe();
            }
            else {
                // Write to: a new partition in an existing partitioned table,
                //           or an existing unpartitioned table
                if (partitionName.isPresent()) {
                    isNew = true;
                }
                else {
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
                if (respectTableFormat) {
                    outputFormat = table.getSd().getOutputFormat();
                }
                else {
                    outputFormat = tableStorageFormat.getOutputFormat();
                }
                serDe = table.getSd().getSerdeInfo().getSerializationLib();
            }
        }
        else {
            // Write to: an existing partition in an existing partitioned table,
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
                isNew,
                dataColumnNames,
                dataColumnTypes,
                outputFormat,
                serDe,
                schema,
                generateRandomFileName(outputFormat),
                write.toString(),
                target.toString(),
                typeManager,
                conf);
    }

    private String generateRandomFileName(String outputFormat)
    {
        // text format files must have the correct extension when compressed
        String extension = "";
        if (HiveConf.getBoolVar(conf, COMPRESSRESULT) && HiveIgnoreKeyTextOutputFormat.class.getName().equals(outputFormat)) {
            extension = new DefaultCodec().getDefaultExtension();

            String compressionCodecClass = conf.get("mapred.output.compression.codec");
            if (compressionCodecClass != null) {
                try {
                    Class<? extends CompressionCodec> codecClass = conf.getClassByName(compressionCodecClass).asSubclass(CompressionCodec.class);
                    extension = ReflectionUtil.newInstance(codecClass, conf).getDefaultExtension();
                }
                catch (ClassNotFoundException e) {
                    throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Compression codec not found: " + compressionCodecClass, e);
                }
                catch (RuntimeException e) {
                    throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Failed to load compression codec: " + compressionCodecClass, e);
                }
            }
        }
        return filePrefix + "_" + randomUUID() + extension;
    }

    private Block[] getDataBlocks(Page page, Block sampleWeightBlock)
    {
        Block[] blocks = new Block[dataColumns.length + (sampleWeightBlock != null ? 1 : 0)];
        for (int i = 0; i < dataColumns.length; i++) {
            int dataColumn = dataColumns[i];
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
        Block[] blocks = new Block[partitionColumns.length];
        for (int i = 0; i < partitionColumns.length; i++) {
            int dataColumn = partitionColumns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return blocks;
    }

    private static void buildRow(List<Type> columnTypes, List<Object> row, Block[] blocks, int position)
    {
        for (int field = 0; field < blocks.length; field++) {
            row.set(field, getField(columnTypes.get(field), blocks[field], position));
        }
    }

    private static class HiveRecordWriter
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

        public HiveRecordWriter(
                String schemaName,
                String tableName,
                String partitionName,
                boolean isNew,
                List<String> inputColumnNames,
                List<Type> inputColumnTypes,
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
            List<Type> fileColumnTypes = toHiveTypes(schema.getProperty(META_TABLE_COLUMN_TYPES, "")).stream()
                    .map(hiveType -> hiveType.getType(typeManager))
                    .collect(toList());

            // verify we can write all input columns to the file
            Set<Object> missingColumns = Sets.difference(new HashSet<>(inputColumnNames), new HashSet<>(fileColumnNames));
            if (!missingColumns.isEmpty()) {
                throw new PrestoException(HIVE_WRITER_ERROR, format("Table %s.%s does not have columns %s", schema, tableName, missingColumns));
            }
            if (fileColumnNames.size() != fileColumnTypes.size()) {
                throw new PrestoException(HIVE_INVALID_METADATA, format("Partition '%s' in table '%s.%s' has metadata for column names or types",
                        partitionName,
                        schemaName,
                        tableName));
            }

            // verify the file types match the input type
            // todo adapt input types to the file types as Hive does
            for (int fileIndex = 0; fileIndex < fileColumnNames.size(); fileIndex++) {
                String columnName = fileColumnNames.get(fileIndex);
                Type fileColumnType = fileColumnTypes.get(fileIndex);
                int inputIndex = inputColumnNames.indexOf(columnName);
                Type inputType = inputColumnTypes.get(inputIndex);

                if (!inputType.equals(fileColumnType)) {
                    // todo this should be moved to a helper
                    throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("" +
                                    "There is a mismatch between the table and partition schemas. " +
                                    "The column '%s' in table '%s.%s' is declared as type '%s', " +
                                    "but partition '%s' declared column '%s' as type '%s'.",
                            columnName,
                            schemaName,
                            tableName,
                            inputType,
                            partitionName,
                            columnName,
                            fileColumnType));
                }
            }

            fieldCount = fileColumnNames.size();

            serializer = initializeSerializer(conf, schema, serDe);
            recordWriter = HiveWriteUtils.createRecordWriter(new Path(writePath, fileName), conf, schema, outputFormat);

            tableInspector = getStandardStructObjectInspector(fileColumnNames, getJavaObjectInspectors(fileColumnTypes));

            // reorder (and possibly reduce) struct fields to match input
            structFields = ImmutableList.copyOf(inputColumnNames.stream()
                    .map(tableInspector::getStructFieldRef)
                    .collect(toList()));

            row = tableInspector.create();
        }

        public void addRow(List<Object> fieldValues)
        {
            checkState(fieldValues.size() == fieldCount, "Invalid row");

            for (int field = 0; field < fieldCount; field++) {
                tableInspector.setStructFieldData(row, structFields.get(field), fieldValues.get(field));
            }

            try {
                recordWriter.write(serializer.serialize(row, tableInspector));
            }
            catch (SerDeException | IOException e) {
                throw new PrestoException(HIVE_WRITER_ERROR, e);
            }
        }

        public void commit()
        {
            try {
                recordWriter.close(false);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_WRITER_ERROR, "Error committing write to Hive", e);
            }
        }

        public void rollback()
        {
            try {
                recordWriter.close(true);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_WRITER_ERROR, "Error rolling back write to Hive", e);
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
}
