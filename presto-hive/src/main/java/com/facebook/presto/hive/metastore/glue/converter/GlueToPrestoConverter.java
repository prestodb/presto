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
import com.facebook.presto.hive.metastore.PrincipalType;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.hive.metastore.SortingColumn.Order;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

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
                .setParameters(firstNonNull(glueDb.getParameters(), ImmutableMap.of()))
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
                .setTableType(glueTable.getTableType())
                .setDataColumns(sd.getColumns().stream()
                        .map(GlueToPrestoConverter::convertColumn)
                        .collect(toList()))
                .setParameters(firstNonNull(glueTable.getParameters(), ImmutableMap.of()))
                .setViewOriginalText(Optional.ofNullable(glueTable.getViewOriginalText()))
                .setViewExpandedText(Optional.ofNullable(glueTable.getViewExpandedText()));

        if (glueTable.getPartitionKeys() != null) {
            tableBuilder.setPartitionColumns(glueTable.getPartitionKeys().stream()
                    .map(GlueToPrestoConverter::convertColumn)
                    .collect(toList()));
        }
        else {
            tableBuilder.setPartitionColumns(new ArrayList<>());
        }

        setStorageBuilder(sd, tableBuilder.getStorageBuilder());
        return tableBuilder.build();
    }

    private static void setStorageBuilder(StorageDescriptor sd, Storage.Builder storageBuilder)
    {
        requireNonNull(sd.getSerdeInfo(), "StorageDescriptor SerDeInfo is null");
        SerDeInfo serdeInfo = sd.getSerdeInfo();

        Optional<HiveBucketProperty> bucketProperty = Optional.empty();
        if (sd.getNumberOfBuckets() > 0) {
            if (isNullOrEmpty(sd.getBucketColumns())) {
                throw new PrestoException(HIVE_INVALID_METADATA, "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set");
            }
            List<SortingColumn> sortedBy = ImmutableList.of();
            if (!isNullOrEmpty(sd.getSortColumns())) {
                sortedBy = sd.getSortColumns().stream()
                        .map(column -> new SortingColumn(
                                column.getColumn(),
                                Order.fromMetastoreApiOrder(column.getSortOrder(), "unknown")))
                        .collect(toImmutableList());
            }
            bucketProperty = Optional.of(new HiveBucketProperty(sd.getBucketColumns(), sd.getNumberOfBuckets(), sortedBy));
        }

        storageBuilder.setStorageFormat(StorageFormat.createNullable(serdeInfo.getSerializationLibrary(), sd.getInputFormat(), sd.getOutputFormat()))
                .setLocation(nullToEmpty(sd.getLocation()))
                .setBucketProperty(bucketProperty)
                .setSkewed(sd.getSkewedInfo() != null && !isNullOrEmpty(sd.getSkewedInfo().getSkewedColumnNames()))
                .setSerdeParameters(firstNonNull(serdeInfo.getParameters(), ImmutableMap.of()))
                .build();
    }

    private static Column convertColumn(com.amazonaws.services.glue.model.Column glueColumn)
    {
        return new Column(glueColumn.getName(), HiveType.valueOf(glueColumn.getType().toLowerCase(Locale.ENGLISH)), Optional.ofNullable(glueColumn.getComment()));
    }

    public static Partition convertPartition(com.amazonaws.services.glue.model.Partition gluePartition)
    {
        requireNonNull(gluePartition.getStorageDescriptor(), "Partition StorageDescriptor is null");
        StorageDescriptor sd = gluePartition.getStorageDescriptor();

        Partition.Builder partitionBuilder = Partition.builder()
                .setDatabaseName(gluePartition.getDatabaseName())
                .setTableName(gluePartition.getTableName())
                .setValues(gluePartition.getValues())
                .setColumns(sd.getColumns().stream()
                        .map(GlueToPrestoConverter::convertColumn)
                        .collect(toList()))
                .setParameters(firstNonNull(gluePartition.getParameters(), ImmutableMap.of()));

        setStorageBuilder(sd, partitionBuilder.getStorageBuilder());
        return partitionBuilder.build();
    }

    private static boolean isNullOrEmpty(List list)
    {
        return list == null || list.isEmpty();
    }
}
