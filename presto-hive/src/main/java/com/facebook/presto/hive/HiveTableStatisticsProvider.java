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

import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTableStatisticsProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveUtil.toPartitionValues;
import static com.facebook.presto.hive.util.Types.checkType;
import static com.facebook.presto.spi.statistics.TableStatistics.EMPTY_STATISTICS;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class HiveTableStatisticsProvider
        implements ConnectorTableStatisticsProvider
{
    private final ExtendedHiveMetastore hiveMetastore;

    @Inject
    public HiveTableStatisticsProvider(ExtendedHiveMetastore hiveMetastore)
    {
        this.hiveMetastore = hiveMetastore;
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableLayoutHandle layoutHandle)
    {
        HiveTableLayoutHandle layout = checkType(layoutHandle, HiveTableLayoutHandle.class, "layoutHandle");
        List<HivePartition> hivePartitions = layout.getPartitions().orElse(ImmutableList.of());
        List<Long> partitionRowNums = hivePartitions.stream()
                .map(this::getPartitionStatistics)
                .map(PartitionStatistics::getNumRows)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        long numRowsSum = partitionRowNums.stream().mapToLong(a -> a).sum();
        long partitionsWithStatsCount = partitionRowNums.size();
        long allPartitionsCount = hivePartitions.size();

        TableStatistics tableStatistics = EMPTY_STATISTICS;
        if (partitionsWithStatsCount > 0) {
            tableStatistics = TableStatistics.builder()
                    .setRowCount(new Estimate(1.0 * numRowsSum / partitionsWithStatsCount * allPartitionsCount))
                    .build();
            // TODO uncertainity level should be handled here
        }
        return tableStatistics;
    }

    private PartitionStatistics getPartitionStatistics(HivePartition hivePartition)
    {
        String databaseName = hivePartition.getTableName().getSchemaName();
        String tableName = hivePartition.getTableName().getTableName();
        String partitionId = hivePartition.getPartitionId();
        if (partitionId.equals(HivePartition.UNPARTITIONED_ID)) {
            Optional<Table> tableOptional = hiveMetastore.getTable(databaseName, tableName);
            Table table = tableOptional
                    .orElseThrow(() -> new IllegalArgumentException(format("Could not get metadata for table %s.%s", databaseName, tableName)));
            return readStatisticsFromParameters(table.getParameters());
        }
        else {
            Optional<Partition> partitionOptional = hiveMetastore.getPartition(databaseName, tableName, toPartitionValues(partitionId));
            Partition partition = partitionOptional
                    .orElseThrow(() -> new IllegalArgumentException(format("Could not get metadata for partition %s.%s.%s", databaseName, tableName, partitionId)));
            return readStatisticsFromParameters(partition.getParameters());
        }
    }

    private PartitionStatistics readStatisticsFromParameters(Map<String, String> parameters)
    {
        boolean columnStatsAcurate = Boolean.valueOf(Optional.ofNullable(parameters.get("COLUMN_STATS_ACCURATE")).orElse("false"));
        Optional<Long> numFiles = Optional.ofNullable(parameters.get("numFiles")).map(Long::valueOf);
        Optional<Long> numRows = Optional.ofNullable(parameters.get("numRows")).map(Long::valueOf);
        Optional<Long> rawDataSize = Optional.ofNullable(parameters.get("rawDataSize")).map(Long::valueOf);
        Optional<Long> totalSize = Optional.ofNullable(parameters.get("totalSize")).map(Long::valueOf);
        return new PartitionStatistics(columnStatsAcurate, numFiles, numRows, rawDataSize, totalSize);
    }
}
