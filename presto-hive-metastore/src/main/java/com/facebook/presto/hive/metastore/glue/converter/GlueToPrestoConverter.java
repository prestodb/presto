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

import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.facebook.presto.hive.HiveBucketProperty;
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

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.util.Memoizers.memoizeLast;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class GlueToPrestoConverter
{
    private static final String PUBLIC_OWNER = "PUBLIC";

    private GlueToPrestoConverter() {}

    public static Database convertDatabase(com.amazonaws.services.glue.model.Database glueDb)
    {
        return Database.builder()
                .setDatabaseName(glueDb.getName())
                .setLocation(Optional.ofNullable(glueDb.getLocationUri()))
                .setComment(Optional.ofNullable(glueDb.getDescription()))
                .setParameters(convertParameters(glueDb.getParameters()))
                .setOwnerName(PUBLIC_OWNER)
                .setOwnerType(PrincipalType.ROLE)
                .build();
    }

    public static Table convertTable(com.amazonaws.services.glue.model.Table glueTable, String dbName)
    {
        requireNonNull(glueTable.getStorageDescriptor(), "Table StorageDescriptor is null");
        StorageDescriptor sd = glueTable.getStorageDescriptor();

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(dbName)
                .setTableName(glueTable.getName())
                .setOwner(nullToEmpty(glueTable.getOwner()))
                // Athena treats missing table type as EXTERNAL_TABLE.
                .setTableType(PrestoTableType.optionalValueOf(glueTable.getTableType()).orElse(EXTERNAL_TABLE))
                .setDataColumns(convertColumns(sd.getColumns()))
                .setParameters(convertParameters(glueTable.getParameters()))
                .setViewOriginalText(Optional.ofNullable(glueTable.getViewOriginalText()))
                .setViewExpandedText(Optional.ofNullable(glueTable.getViewExpandedText()));

        if (glueTable.getPartitionKeys() != null) {
            tableBuilder.setPartitionColumns(convertColumns(glueTable.getPartitionKeys()));
        }
        else {
            tableBuilder.setPartitionColumns(ImmutableList.of());
        }

        new StorageConverter().setConvertedStorage(sd, tableBuilder.getStorageBuilder());
        return tableBuilder.build();
    }

    private static Column convertColumn(com.amazonaws.services.glue.model.Column glueColumn)
    {
        return new Column(glueColumn.getName(), HiveType.valueOf(glueColumn.getType().toLowerCase(Locale.ENGLISH)), Optional.ofNullable(glueColumn.getComment()));
    }

    private static List<Column> convertColumns(List<com.amazonaws.services.glue.model.Column> glueColumns)
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
            implements Function<com.amazonaws.services.glue.model.Partition, Partition>
    {
        private final Function<List<com.amazonaws.services.glue.model.Column>, List<Column>> columnsConverter = memoizeLast(GlueToPrestoConverter::convertColumns);
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
        public Partition apply(com.amazonaws.services.glue.model.Partition gluePartition)
        {
            requireNonNull(gluePartition.getStorageDescriptor(), "Partition StorageDescriptor is null");
            StorageDescriptor sd = gluePartition.getStorageDescriptor();

            if (!databaseName.equals(gluePartition.getDatabaseName())) {
                throw new IllegalArgumentException(format("Unexpected databaseName, expected: %s, but found: %s", databaseName, gluePartition.getDatabaseName()));
            }
            if (!tableName.equals(gluePartition.getTableName())) {
                throw new IllegalArgumentException(format("Unexpected tableName, expected: %s, but found: %s", tableName, gluePartition.getTableName()));
            }

            Partition.Builder partitionBuilder = Partition.builder()
                    .setDatabaseName(databaseName)
                    .setTableName(tableName)
                    .setValues(gluePartition.getValues()) // No memoization benefit
                    .setColumns(columnsConverter.apply(sd.getColumns()))
                    .setParameters(parametersConverter.apply(gluePartition.getParameters()));

            storageConverter.setConvertedStorage(sd, partitionBuilder.getStorageBuilder());

            return partitionBuilder.build();
        }
    }

    private static final class StorageConverter
    {
        private final Function<List<String>, List<String>> bucketColumns = memoizeLast(ImmutableList::copyOf);
        private final Function<List<com.amazonaws.services.glue.model.Order>, List<SortingColumn>> sortColumns = memoizeLast(StorageConverter::createSortingColumns);
        private final UnaryOperator<Optional<HiveBucketProperty>> bucketProperty = memoizeLast();
        private final Function<Map<String, String>, Map<String, String>> serdeParametersConverter = parametersConverter();
        private final Function<Map<String, String>, Map<String, String>> partitionParametersConverter = parametersConverter();
        private final StorageFormatConverter storageFormatConverter = new StorageFormatConverter();

        public void setConvertedStorage(StorageDescriptor sd, Storage.Builder storageBuilder)
        {
            requireNonNull(sd.getSerdeInfo(), "StorageDescriptor SerDeInfo is null");
            SerDeInfo serdeInfo = sd.getSerdeInfo();

            storageBuilder.setLocation(nullToEmpty(sd.getLocation()))
                    .setBucketProperty(createBucketProperty(sd))
                    .setSkewed(sd.getSkewedInfo() != null && !isNullOrEmpty(sd.getSkewedInfo().getSkewedColumnNames()))
                    .setSerdeParameters(serdeParametersConverter.apply(serdeInfo.getParameters()))
                    .setParameters(partitionParametersConverter.apply(sd.getParameters()))
                    .setStorageFormat(storageFormatConverter.createStorageFormat(serdeInfo, sd));
        }

        private Optional<HiveBucketProperty> createBucketProperty(StorageDescriptor sd)
        {
            if (sd.getNumberOfBuckets() > 0) {
                if (isNullOrEmpty(sd.getBucketColumns())) {
                    throw new PrestoException(HIVE_INVALID_METADATA, "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set");
                }
                List<String> bucketColumns = this.bucketColumns.apply(sd.getBucketColumns());
                List<SortingColumn> sortedBy = this.sortColumns.apply(sd.getSortColumns());
                return bucketProperty.apply(Optional.of(new HiveBucketProperty(bucketColumns, sd.getNumberOfBuckets(), sortedBy, HIVE_COMPATIBLE, Optional.empty())));
            }
            return Optional.empty();
        }

        private static List<SortingColumn> createSortingColumns(List<com.amazonaws.services.glue.model.Order> sortColumns)
        {
            if (isNullOrEmpty(sortColumns)) {
                return ImmutableList.of();
            }
            return mappedCopy(sortColumns, column -> new SortingColumn(column.getColumn(), Order.fromMetastoreApiOrder(column.getSortOrder(), "unknown")));
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
            String serializationLib = this.serializationLib.apply(serdeInfo.getSerializationLibrary());
            String inputFormat = this.inputFormat.apply(storageDescriptor.getInputFormat());
            String outputFormat = this.outputFormat.apply(storageDescriptor.getOutputFormat());
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
