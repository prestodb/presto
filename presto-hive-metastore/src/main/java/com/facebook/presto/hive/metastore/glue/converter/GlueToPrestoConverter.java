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
package com.facebook.presto.hive.metastore.glue.converter;

import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.hive.metastore.SortingColumn.Order;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.PrincipalType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isDeltaLakeTable;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isIcebergTable;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.util.Memoizers.memoizeLast;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class GlueToPrestoConverter
{
    private static final String PUBLIC_OWNER = "PUBLIC";

    private GlueToPrestoConverter() {}

    public static Database convertDatabase(software.amazon.awssdk.services.glue.model.Database glueDb)
    {
        return Database.builder()
                .setDatabaseName(glueDb.name())
                .setLocation(Optional.ofNullable(glueDb.locationUri()))
                .setComment(Optional.ofNullable(glueDb.description()))
                .setParameters(convertParameters(glueDb.parameters()))
                .setOwnerName(PUBLIC_OWNER)
                .setOwnerType(PrincipalType.ROLE)
                .build();
    }

    public static Table convertTable(software.amazon.awssdk.services.glue.model.Table glueTable, String dbName)
    {
        Map<String, String> tableParameters = convertParameters(glueTable.parameters());

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(dbName)
                .setTableName(glueTable.name())
                .setOwner(nullToEmpty(glueTable.owner()))
                // Athena treats missing table type as EXTERNAL_TABLE.
                .setTableType(PrestoTableType.optionalValueOf(glueTable.tableType()).orElse(EXTERNAL_TABLE))
                .setParameters(tableParameters)
                .setViewOriginalText(Optional.ofNullable(glueTable.viewOriginalText()))
                .setViewExpandedText(Optional.ofNullable(glueTable.viewExpandedText()));

        StorageDescriptor sd = glueTable.storageDescriptor();
        if (isIcebergTable(tableParameters) || (sd == null && isDeltaLakeTable(tableParameters))) {
            // Iceberg and Delta Lake tables do not use the StorageDescriptor field, but we need to return a Table so the caller can check that
            // the table is an Iceberg/Delta table and decide whether to redirect or fail.
            tableBuilder.setDataColumns(ImmutableList.of(new Column("dummy", HIVE_INT, Optional.empty(), Optional.empty())));
            tableBuilder.getStorageBuilder().setStorageFormat(StorageFormat.fromHiveStorageFormat(HiveStorageFormat.PARQUET));
            tableBuilder.getStorageBuilder().setLocation(sd == null ? "" : sd.location());
        }
        else {
            if (sd == null) {
                throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, format("Table StorageDescriptor is null for table %s.%s (%s)", dbName, glueTable.name(), glueTable));
            }
            tableBuilder.setDataColumns(convertColumns(sd.columns()));
            if (glueTable.partitionKeys() != null) {
                tableBuilder.setPartitionColumns(convertColumns(glueTable.partitionKeys()));
            }
            else {
                tableBuilder.setPartitionColumns(ImmutableList.of());
            }

            new StorageConverter().setConvertedStorage(sd, tableBuilder.getStorageBuilder());
        }

        return tableBuilder.build();
    }

    private static Column convertColumn(software.amazon.awssdk.services.glue.model.Column glueColumn)
    {
        return new Column(glueColumn.name(), HiveType.valueOf(glueColumn.type().toLowerCase(Locale.ENGLISH)), Optional.ofNullable(glueColumn.comment()), Optional.empty());
    }

    private static List<Column> convertColumns(List<software.amazon.awssdk.services.glue.model.Column> glueColumns)
    {
        return mappedCopy(glueColumns, GlueToPrestoConverter::convertColumn);
    }

    private static Map<String, String> convertParameters(Map<String, String> input)
    {
        if (input == null || input.isEmpty()) {
            return ImmutableMap.of();
        }
        return ImmutableMap.copyOf(input);
    }

    private static Function<Map<String, String>, Map<String, String>> parametersConverter()
    {
        return memoizeLast(GlueToPrestoConverter::convertParameters);
    }

    private static boolean isNullOrEmpty(List<?> list)
    {
        return list == null || list.isEmpty();
    }

    public static final class GluePartitionConverter
            implements Function<software.amazon.awssdk.services.glue.model.Partition, Partition>
    {
        private final Function<List<software.amazon.awssdk.services.glue.model.Column>, List<Column>> columnsConverter = memoizeLast(GlueToPrestoConverter::convertColumns);
        private final Function<Map<String, String>, Map<String, String>> parametersConverter = parametersConverter();
        private final StorageConverter storageConverter = new StorageConverter();
        private final String databaseName;
        private final String tableName;

        public GluePartitionConverter(String databaseName, String tableName)
        {
            this.databaseName = requireNonNull(databaseName, "databaseName is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
        }

        @Override
        public Partition apply(software.amazon.awssdk.services.glue.model.Partition gluePartition)
        {
            requireNonNull(gluePartition.storageDescriptor(), "Partition StorageDescriptor is null");
            StorageDescriptor sd = gluePartition.storageDescriptor();

            if (!databaseName.equals(gluePartition.databaseName())) {
                throw new IllegalArgumentException(format("Unexpected databaseName, expected: %s, but found: %s", databaseName, gluePartition.databaseName()));
            }
            if (!tableName.equals(gluePartition.tableName())) {
                throw new IllegalArgumentException(format("Unexpected tableName, expected: %s, but found: %s", tableName, gluePartition.tableName()));
            }

            Partition.Builder partitionBuilder = Partition.builder()
                    .setCatalogName(Optional.empty())
                    .setDatabaseName(databaseName)
                    .setTableName(tableName)
                    .setValues(gluePartition.values()) // No memoization benefit
                    .setColumns(columnsConverter.apply(sd.columns()))
                    .setParameters(parametersConverter.apply(gluePartition.parameters()));

            storageConverter.setConvertedStorage(sd, partitionBuilder.getStorageBuilder());

            return partitionBuilder.build();
        }
    }

    private static final class StorageConverter
    {
        private final Function<List<String>, List<String>> bucketColumns = memoizeLast(ImmutableList::copyOf);
        private final Function<List<software.amazon.awssdk.services.glue.model.Order>, List<SortingColumn>> sortColumns = memoizeLast(StorageConverter::createSortingColumns);
        private final UnaryOperator<Optional<HiveBucketProperty>> bucketProperty = memoizeLast();
        private final Function<Map<String, String>, Map<String, String>> serdeParametersConverter = parametersConverter();
        private final Function<Map<String, String>, Map<String, String>> partitionParametersConverter = parametersConverter();
        private final StorageFormatConverter storageFormatConverter = new StorageFormatConverter();

        public void setConvertedStorage(StorageDescriptor sd, Storage.Builder storageBuilder)
        {
            requireNonNull(sd.serdeInfo(), "StorageDescriptor SerDeInfo is null");
            SerDeInfo serdeInfo = sd.serdeInfo();

            storageBuilder.setLocation(nullToEmpty(sd.location()))
                    .setBucketProperty(createBucketProperty(sd))
                    .setSkewed(sd.skewedInfo() != null && !isNullOrEmpty(sd.skewedInfo().skewedColumnNames()))
                    .setSerdeParameters(serdeParametersConverter.apply(serdeInfo.parameters()))
                    .setParameters(partitionParametersConverter.apply(sd.parameters()))
                    .setStorageFormat(storageFormatConverter.createStorageFormat(serdeInfo, sd));
        }

        private Optional<HiveBucketProperty> createBucketProperty(StorageDescriptor sd)
        {
            if (sd.numberOfBuckets() > 0) {
                if (isNullOrEmpty(sd.bucketColumns())) {
                    throw new PrestoException(HIVE_INVALID_METADATA, "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set");
                }
                List<String> bucketColumns = this.bucketColumns.apply(sd.bucketColumns());
                List<SortingColumn> sortedBy = this.sortColumns.apply(sd.sortColumns());
                return bucketProperty.apply(Optional.of(new HiveBucketProperty(bucketColumns, sd.numberOfBuckets(), sortedBy, HIVE_COMPATIBLE, Optional.empty())));
            }
            return Optional.empty();
        }

        private static List<SortingColumn> createSortingColumns(List<software.amazon.awssdk.services.glue.model.Order> sortColumns)
        {
            if (isNullOrEmpty(sortColumns)) {
                return ImmutableList.of();
            }
            return mappedCopy(sortColumns, column -> new SortingColumn(column.column(), Order.fromMetastoreApiOrder(column.sortOrder(), "unknown")));
        }
    }

    private static final class StorageFormatConverter
    {
        private static final StorageFormat ALL_NULLS = StorageFormat.createNullable(null, null, null);
        private final UnaryOperator<String> serializationLib = memoizeLast();
        private final UnaryOperator<String> inputFormat = memoizeLast();
        private final UnaryOperator<String> outputFormat = memoizeLast();
        // Second phase to attempt memoization on the entire instance beyond just the fields
        private final UnaryOperator<StorageFormat> storageFormat = memoizeLast();

        public StorageFormat createStorageFormat(SerDeInfo serdeInfo, StorageDescriptor storageDescriptor)
        {
            String serializationLib = this.serializationLib.apply(serdeInfo.serializationLibrary());
            String inputFormat = this.inputFormat.apply(storageDescriptor.inputFormat());
            String outputFormat = this.outputFormat.apply(storageDescriptor.outputFormat());
            if (serializationLib == null && inputFormat == null && outputFormat == null) {
                return ALL_NULLS;
            }
            return storageFormat.apply(StorageFormat.createNullable(serializationLib, inputFormat, outputFormat));
        }
    }

    public static <T, R> List<R> mappedCopy(List<T> list, Function<T, R> mapper)
    {
        requireNonNull(list, "list is null");
        requireNonNull(mapper, "mapper is null");
        //  Uses a pre-sized builder to avoid intermediate allocations and copies, which is especially significant when the
        //  number of elements is large and the size of the resulting list can be known in advance
        ImmutableList.Builder<R> builder = ImmutableList.builderWithExpectedSize(list.size());
        for (T item : list) {
            builder.add(mapper.apply(item));
        }
        return builder.build();
    }
}
