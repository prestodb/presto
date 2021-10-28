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

import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.Order;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.SerDeInfo;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;
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
import static java.util.stream.Collectors.toList;

public final class GlueInputConverter
{
    private GlueInputConverter() {}

    public static DatabaseInput convertDatabase(Database database)
    {
        DatabaseInput input = new DatabaseInput();
        input.setName(database.getDatabaseName());
        input.setParameters(database.getParameters());
        database.getComment().ifPresent(input::setDescription);
        database.getLocation().ifPresent(input::setLocationUri);
        return input;
    }

    public static TableInput convertTable(Table table)
    {
        TableInput input = new TableInput();
        input.setName(table.getTableName());
        input.setOwner(table.getOwner());
        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE, VIRTUAL_VIEW).contains(table.getTableType()), "Invalid table type: %s", table.getTableType());
        input.setTableType(table.getTableType().toString());
        input.setStorageDescriptor(convertStorage(table.getStorage(), table.getDataColumns()));
        input.setPartitionKeys(table.getPartitionColumns().stream().map(GlueInputConverter::convertColumn).collect(toList()));
        input.setParameters(table.getParameters());
        table.getViewOriginalText().ifPresent(input::setViewOriginalText);
        table.getViewExpandedText().ifPresent(input::setViewExpandedText);
        return input;
    }

    public static TableInput toTableInput(com.amazonaws.services.glue.model.Table table)
    {
        TableInput input = new TableInput();
        input.setName(table.getName());
        input.setOwner(table.getOwner());
        input.setTableType(table.getTableType());
        input.setStorageDescriptor(table.getStorageDescriptor());
        input.setPartitionKeys(table.getPartitionKeys());
        input.setParameters(table.getParameters());
        input.setViewOriginalText(table.getViewOriginalText());
        input.setViewExpandedText(table.getViewExpandedText());
        return input;
    }

    public static PartitionInput convertPartition(PartitionWithStatistics partitionWithStatistics)
    {
        PartitionInput input = convertPartition(partitionWithStatistics.getPartition());
        PartitionStatistics statistics = partitionWithStatistics.getStatistics();
        if (!statistics.getColumnStatistics().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, "Glue metastore does not support column level statistics");
        }
        input.setParameters(updateStatisticsParameters(input.getParameters(), statistics.getBasicStatistics()));
        return input;
    }

    public static PartitionInput convertPartition(Partition partition)
    {
        PartitionInput input = new PartitionInput();
        input.setValues(partition.getValues());
        input.setStorageDescriptor(convertStorage(partition.getStorage(), partition.getColumns()));
        input.setParameters(partition.getParameters());
        return input;
    }

    private static StorageDescriptor convertStorage(Storage storage, List<Column> columns)
    {
        if (storage.isSkewed()) {
            throw new IllegalArgumentException("Writing to skewed table/partition is not supported");
        }
        SerDeInfo serdeInfo = new SerDeInfo()
                .withSerializationLibrary(storage.getStorageFormat().getSerDeNullable())
                .withParameters(storage.getSerdeParameters());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(storage.getLocation());
        sd.setColumns(columns.stream().map(GlueInputConverter::convertColumn).collect(toList()));
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(storage.getStorageFormat().getInputFormatNullable());
        sd.setOutputFormat(storage.getStorageFormat().getOutputFormatNullable());
        sd.setParameters(ImmutableMap.of());

        Optional<HiveBucketProperty> bucketProperty = storage.getBucketProperty();
        if (bucketProperty.isPresent()) {
            sd.setNumberOfBuckets(bucketProperty.get().getBucketCount());
            sd.setBucketColumns(bucketProperty.get().getBucketedBy());
            if (!bucketProperty.get().getSortedBy().isEmpty()) {
                sd.setSortColumns(bucketProperty.get().getSortedBy().stream()
                        .map(column -> new Order().withColumn(column.getColumnName()).withSortOrder(column.getOrder().getHiveOrder()))
                        .collect(toImmutableList()));
            }
        }

        return sd;
    }

    public static com.amazonaws.services.glue.model.Column convertColumn(Column prestoColumn)
    {
        return new com.amazonaws.services.glue.model.Column()
                .withName(prestoColumn.getName())
                .withType(prestoColumn.getType().toString())
                .withComment(prestoColumn.getComment().orElse(null));
    }
}
