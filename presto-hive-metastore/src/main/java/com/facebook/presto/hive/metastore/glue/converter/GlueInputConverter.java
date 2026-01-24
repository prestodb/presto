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
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.Order;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.VIRTUAL_VIEW;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class GlueInputConverter
{
    private GlueInputConverter() {}

    public static DatabaseInput convertDatabase(Database database)
    {
        return DatabaseInput.builder()
                .name(database.getDatabaseName())
                .parameters(database.getParameters())
                .applyMutation(builder -> database.getComment().ifPresent(builder::description))
                .applyMutation(builder -> database.getLocation().ifPresent(builder::locationUri))
                .build();
    }

    public static TableInput convertTable(Table table)
    {
        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE, VIRTUAL_VIEW).contains(table.getTableType()), "Invalid table type: %s", table.getTableType());

        return TableInput.builder()
                .name(table.getTableName())
                .owner(table.getOwner())
                .tableType(table.getTableType().toString())
                .storageDescriptor(convertStorage(table.getStorage(), table.getDataColumns()))
                .partitionKeys(table.getPartitionColumns().stream().map(GlueInputConverter::convertColumn).collect(toImmutableList()))
                .parameters(table.getParameters())
                .applyMutation(builder -> table.getViewOriginalText().ifPresent(builder::viewOriginalText))
                .applyMutation(builder -> table.getViewExpandedText().ifPresent(builder::viewExpandedText))
                .build();
    }

    public static TableInput toTableInput(software.amazon.awssdk.services.glue.model.Table table)
    {
        return TableInput.builder()
                .name(table.name())
                .owner(table.owner())
                .tableType(table.tableType())
                .storageDescriptor(table.storageDescriptor())
                .partitionKeys(table.partitionKeys())
                .parameters(table.parameters())
                .viewOriginalText(table.viewOriginalText())
                .viewExpandedText(table.viewExpandedText())
                .build();
    }

    public static PartitionInput convertPartition(PartitionWithStatistics partitionWithStatistics)
    {
        PartitionInput input = convertPartition(partitionWithStatistics.getPartition());
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();
        if (!statistics.getColumnStatistics().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore does not support column level statistics");
        }
        return input.toBuilder().parameters(updateStatisticsParameters(input.parameters(), statistics.getBasicStatistics()))
                .build();
    }

    public static PartitionInput convertPartition(Partition partition)
    {
        return PartitionInput.builder()
                .values(partition.getValues())
                .storageDescriptor(convertStorage(partition.getStorage(), partition.getColumns()))
                .parameters(partition.getParameters())
                .build();
    }

    private static StorageDescriptor convertStorage(Storage storage, List<Column> columns)
    {
        if (storage.isSkewed()) {
            throw new IllegalArgumentException("Writing to skewed table/partition is not supported");
        }
        SerDeInfo serDeInfo = SerDeInfo.builder()
                .serializationLibrary(storage.getStorageFormat().getSerDeNullable())
                .parameters(storage.getSerdeParameters())
                .build();

        StorageDescriptor.Builder sd = StorageDescriptor.builder()
                .location(storage.getLocation())
                .columns(columns.stream().map(GlueInputConverter::convertColumn).collect(toImmutableList()))
                .serdeInfo(serDeInfo)
                .inputFormat(storage.getStorageFormat().getInputFormatNullable())
                .outputFormat(storage.getStorageFormat().getOutputFormatNullable())
                .parameters(ImmutableMap.of());

        Optional<HiveBucketProperty> bucketProperty = storage.getBucketProperty();
        if (bucketProperty.isPresent()) {
            sd.numberOfBuckets(bucketProperty.get().getBucketCount());
            sd.bucketColumns(bucketProperty.get().getBucketedBy());
            if (!bucketProperty.get().getSortedBy().isEmpty()) {
                sd.sortColumns(bucketProperty.get().getSortedBy().stream()
                        .map(column -> Order.builder().column(column.getColumnName()).sortOrder(column.getOrder().getHiveOrder()).build())
                        .collect(toImmutableList()));
            }
        }

        return sd.build();
    }

    public static software.amazon.awssdk.services.glue.model.Column convertColumn(Column prestoColumn)
    {
        return software.amazon.awssdk.services.glue.model.Column.builder()
                .name(prestoColumn.getName())
                .type(prestoColumn.getType().toString())
                .comment(prestoColumn.getComment().orElse(null))
                .build();
    }
}
