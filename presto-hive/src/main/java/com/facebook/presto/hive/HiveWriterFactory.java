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

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HivePageSinkMetadataProvider;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.common.util.ReflectionUtil;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_READ_ONLY;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.HiveType.toHiveTypes;
import static com.facebook.presto.hive.HiveWriteUtils.getField;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveSchema;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
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

public class HiveWriterFactory
{
    private static final int MAX_BUCKET_COUNT = 100_000;
    private static final int BUCKET_NUMBER_PADDING = Integer.toString(MAX_BUCKET_COUNT - 1).length();

    private final String schemaName;
    private final String tableName;

    private final List<DataColumn> dataColumns;

    private final List<String> partitionColumnNames;
    private final List<Type> partitionColumnTypes;

    private final HiveStorageFormat tableStorageFormat;
    private final HiveStorageFormat partitionStorageFormat;
    private final LocationHandle locationHandle;
    private final LocationService locationService;
    private final String filePrefix;

    private final HivePageSinkMetadataProvider pageSinkMetadataProvider;
    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final JobConf conf;

    private final Table table;
    private final boolean immutablePartitions;

    private final ConnectorSession session;
    private final OptionalInt bucketCount;

    public HiveWriterFactory(
            String schemaName,
            String tableName,
            boolean isCreateTable,
            List<HiveColumnHandle> inputColumns,
            HiveStorageFormat tableStorageFormat,
            HiveStorageFormat partitionStorageFormat,
            OptionalInt bucketCount,
            LocationHandle locationHandle,
            LocationService locationService,
            String filePrefix,
            HivePageSinkMetadataProvider pageSinkMetadataProvider,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            boolean immutablePartitions,
            ConnectorSession session)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");

        this.tableStorageFormat = requireNonNull(tableStorageFormat, "tableStorageFormat is null");
        this.partitionStorageFormat = requireNonNull(partitionStorageFormat, "partitionStorageFormat is null");
        this.locationHandle = requireNonNull(locationHandle, "locationHandle is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.filePrefix = requireNonNull(filePrefix, "filePrefix is null");

        this.pageSinkMetadataProvider = requireNonNull(pageSinkMetadataProvider, "pageSinkMetadataProvider is null");

        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.immutablePartitions = immutablePartitions;

        // divide input columns into partition and data columns
        requireNonNull(inputColumns, "inputColumns is null");
        ImmutableList.Builder<String> partitionColumnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> partitionColumnTypes = ImmutableList.builder();
        ImmutableList.Builder<DataColumn> dataColumns = ImmutableList.builder();
        for (HiveColumnHandle column : inputColumns) {
            HiveType hiveType = column.getHiveType();
            if (column.isPartitionKey()) {
                partitionColumnNames.add(column.getName());
                partitionColumnTypes.add(typeManager.getType(column.getTypeSignature()));
            }
            else {
                dataColumns.add(new DataColumn(column.getName(), typeManager.getType(column.getTypeSignature()), hiveType));
            }
        }
        this.partitionColumnNames = partitionColumnNames.build();
        this.partitionColumnTypes = partitionColumnTypes.build();
        this.dataColumns = dataColumns.build();

        if (isCreateTable) {
            this.table = null;
            Optional<Path> writePath = locationService.writePathRoot(locationHandle);
            checkArgument(writePath.isPresent(), "CREATE TABLE must have a write path");
            conf = new JobConf(hdfsEnvironment.getConfiguration(writePath.get()));
        }
        else {
            Optional<Table> table = pageSinkMetadataProvider.getTable();
            if (!table.isPresent()) {
                throw new PrestoException(HIVE_INVALID_METADATA, format("Table %s.%s was dropped during insert", schemaName, tableName));
            }
            this.table = table.get();
            Path hdfsEnvironmentPath = locationService.writePathRoot(locationHandle).orElseGet(() -> locationService.targetPathRoot(locationHandle));
            conf = new JobConf(hdfsEnvironment.getConfiguration(hdfsEnvironmentPath));
        }

        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        if (bucketCount.isPresent()) {
            checkArgument(bucketCount.getAsInt() < MAX_BUCKET_COUNT, "bucketCount must be smaller than " + MAX_BUCKET_COUNT);
        }

        this.session = requireNonNull(session, "session is null");
    }

    public HiveWriter createWriter(Page partitionColumns, int position, OptionalInt bucketNumber)
    {
        if (bucketCount.isPresent()) {
            checkArgument(bucketNumber.isPresent(), "Bucket not provided for bucketed table");
            checkArgument(bucketNumber.getAsInt() < bucketCount.getAsInt(), "Bucket number %s must be less than bucket count %s", bucketNumber, bucketCount);
        }
        else {
            checkArgument(!bucketNumber.isPresent(), "Bucket number provided by for table that is not bucketed");
        }

        String fileName;
        if (bucketNumber.isPresent()) {
            fileName = computeBucketedFileName(filePrefix, bucketNumber.getAsInt());
        }
        else {
            fileName = filePrefix + "_" + randomUUID();
        }

        List<String> partitionValues = toPartitionValues(partitionColumns, position);

        Optional<String> partitionName;
        if (!partitionColumnNames.isEmpty()) {
            partitionName = Optional.of(FileUtils.makePartName(partitionColumnNames, partitionValues));
        }
        else {
            partitionName = Optional.empty();
        }

        // attempt to get the existing partition (if this is an existing partitioned table)
        Optional<Partition> partition = Optional.empty();
        if (!partitionValues.isEmpty() && table != null) {
            partition = pageSinkMetadataProvider.getPartition(partitionValues);
        }

        boolean isNew;
        Properties schema;
        Path target;
        Path write;
        StorageFormat outputStorageFormat;
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
                        throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format(
                                "Target directory for new partition '%s' of table '%s.%s' already exists: %s",
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
                    if (bucketNumber.isPresent()) {
                        throw new PrestoException(HIVE_PARTITION_READ_ONLY, "Can not insert into bucketed unpartitioned Hive table");
                    }
                    if (immutablePartitions) {
                        throw new PrestoException(HIVE_PARTITION_READ_ONLY, "Unpartitioned Hive tables are immutable");
                    }
                    isNew = false;
                }
                schema = getHiveSchema(table);
                target = locationService.targetPath(locationHandle, partitionName);
                write = locationService.writePath(locationHandle, partitionName).orElse(target);
            }

            if (partitionName.isPresent()) {
                // Write to a new partition
                outputStorageFormat = fromHiveStorageFormat(partitionStorageFormat);
            }
            else {
                // Write to a new/existing unpartitioned table
                outputStorageFormat = fromHiveStorageFormat(tableStorageFormat);
            }
        }
        else {
            // Write to: an existing partition in an existing partitioned table,
            if (bucketNumber.isPresent()) {
                throw new PrestoException(HIVE_PARTITION_READ_ONLY, "Can not insert into existing partitions of bucketed Hive table");
            }
            if (immutablePartitions) {
                throw new PrestoException(HIVE_PARTITION_READ_ONLY, "Hive partitions are immutable");
            }
            isNew = false;

            // Check the column types in partition schema match the column types in table schema
            List<Column> tableColumns = table.getDataColumns();
            List<Column> existingPartitionColumns = partition.get().getColumns();
            for (int i = 0; i < min(existingPartitionColumns.size(), tableColumns.size()); i++) {
                HiveType tableType = tableColumns.get(i).getType();
                HiveType partitionType = existingPartitionColumns.get(i).getType();
                if (!tableType.equals(partitionType)) {
                    throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("" +
                                    "There is a mismatch between the table and partition schemas. " +
                                    "The column '%s' in table '%s' is declared as type '%s', " +
                                    "but partition '%s' declared column '%s' as type '%s'.",
                            tableColumns.get(i).getName(),
                            tableName,
                            tableType,
                            partitionName,
                            existingPartitionColumns.get(i).getName(),
                            partitionType));
                }
            }

            // Append to an existing partition
            HiveWriteUtils.checkPartitionIsWritable(partitionName.get(), partition.get());

            outputStorageFormat = partition.get().getStorage().getStorageFormat();
            schema = getHiveSchema(partition.get(), table);

            target = locationService.targetPath(locationHandle, partition.get(), partitionName.get());
            write = locationService.writePath(locationHandle, partitionName).orElse(target);
        }

        validateSchema(partitionName, schema);

        String fileNameWithExtension = fileName + getFileExtension(conf, outputStorageFormat);
        HiveRecordWriter hiveRecordWriter = new HiveRecordWriter(
                new Path(write, fileNameWithExtension),
                dataColumns.stream()
                        .map(DataColumn::getName)
                        .collect(toList()),
                outputStorageFormat,
                schema,
                typeManager,
                conf);
        return new HiveWriter(hiveRecordWriter, partitionName, isNew, fileNameWithExtension, write.toString(), target.toString());
    }

    private void validateSchema(Optional<String> partitionName, Properties schema)
    {
        // existing tables may have columns in a different order
        List<String> fileColumnNames = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(schema.getProperty(META_TABLE_COLUMNS, ""));
        List<HiveType> fileColumnHiveTypes = toHiveTypes(schema.getProperty(META_TABLE_COLUMN_TYPES, ""));

        // verify we can write all input columns to the file
        Map<String, DataColumn> inputColumnMap = dataColumns.stream()
                .collect(toMap(DataColumn::getName, identity()));
        Set<String> missingColumns = Sets.difference(inputColumnMap.keySet(), new HashSet<>(fileColumnNames));
        if (!missingColumns.isEmpty()) {
            throw new PrestoException(NOT_FOUND, format("Table %s.%s does not have columns %s", schema, tableName, missingColumns));
        }
        if (fileColumnNames.size() != fileColumnHiveTypes.size()) {
            throw new PrestoException(HIVE_INVALID_METADATA, format(
                    "Partition '%s' in table '%s.%s' has mismatched metadata for column names and types",
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
                throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                        "" +
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
    }

    private List<String> toPartitionValues(Page partitionColumns, int position)
    {
        ImmutableList.Builder<String> partitionValues = ImmutableList.builder();
        for (int field = 0; field < partitionColumns.getChannelCount(); field++) {
            Object value = getField(partitionColumnTypes.get(field), partitionColumns.getBlock(field), position);
            if (value == null) {
                partitionValues.add(HIVE_DEFAULT_DYNAMIC_PARTITION);
            }
            else {
                partitionValues.add(value.toString());
            }
        }
        return partitionValues.build();
    }

    public static String computeBucketedFileName(String filePrefix, int bucket)
    {
        return filePrefix + "_bucket-" + Strings.padStart(Integer.toString(bucket), BUCKET_NUMBER_PADDING, '0');
    }

    public static String getFileExtension(JobConf conf, StorageFormat storageFormat)
    {
        // text format files must have the correct extension when compressed
        if (!HiveConf.getBoolVar(conf, COMPRESSRESULT) || !HiveIgnoreKeyTextOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
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

    private static class DataColumn
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
