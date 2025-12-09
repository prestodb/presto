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
package com.facebook.presto.hive.metastore.glue;

import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableResponse;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForTableRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_NOT_FOUND;
import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastore.awsSyncRequest;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

/**
 * Default implementation of GlueStatisticsFetcher that interacts with AWS Glue APIs
 * to fetch and update column statistics.
 * <p>
 * This implementation handles:
 * - Pagination for large numbers of columns
 * - Parallel execution for multiple partitions
 * - Batch operations for writes
 * - Error handling for AWS Glue exceptions
 */
public class DefaultGlueStatisticsFetcher
        implements GlueStatisticsFetcher
{
    // Read limit for AWS Glue API GetColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetColumnStatisticsForPartition
    private static final int GLUE_COLUMN_READ_STAT_PAGE_SIZE = 100;

    // Write limit for AWS Glue API UpdateColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-UpdateColumnStatisticsForPartition
    private static final int GLUE_COLUMN_WRITE_STAT_PAGE_SIZE = 25;

    private final GlueAsyncClient glueClient;
    private final String catalogId;
    private final Executor readExecutor;
    private final Executor writeExecutor;
    private final GlueMetastoreStats stats;

    public DefaultGlueStatisticsFetcher(
            GlueAsyncClient glueClient,
            String catalogId,
            Executor readExecutor,
            Executor writeExecutor,
            GlueMetastoreStats stats)
    {
        this.glueClient = glueClient;
        this.catalogId = catalogId;
        this.readExecutor = readExecutor;
        this.writeExecutor = writeExecutor;
        this.stats = stats;
    }

    @Override
    public List<ColumnStatistics> getTableColumnStatistics(Table table)
    {
        try {
            List<String> columnNames = getAllColumnNames(table);
            List<List<String>> columnChunks = Lists.partition(columnNames, GLUE_COLUMN_READ_STAT_PAGE_SIZE);

            List<CompletableFuture<GetColumnStatisticsForTableResponse>> getStatsFutures = columnChunks.stream()
                    .map(partialColumns -> supplyAsync(() -> {
                        GetColumnStatisticsForTableRequest request = GetColumnStatisticsForTableRequest.builder()
                                .catalogId(catalogId)
                                .databaseName(table.getDatabaseName())
                                .tableName(table.getTableName())
                                .columnNames(partialColumns)
                                .build();
                        return awsSyncRequest(glueClient::getColumnStatisticsForTable, request, stats.getGetColumnStatisticsForTable());
                    }, readExecutor)).collect(toImmutableList());

            ImmutableList.Builder<ColumnStatistics> allColumnStats = ImmutableList.builder();
            for (CompletableFuture<GetColumnStatisticsForTableResponse> future : getStatsFutures) {
                GetColumnStatisticsForTableResponse response = getFutureValue(future, PrestoException.class);
                allColumnStats.addAll(response.columnStatisticsList());
            }

            return allColumnStats.build();
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public Map<Partition, List<ColumnStatistics>> getPartitionColumnStatistics(Set<Partition> partitions)
    {
        Map<Partition, List<CompletableFuture<GetColumnStatisticsForPartitionResponse>>> futuresPerPartition = new HashMap<>();

        for (Partition partition : partitions) {
            ImmutableList.Builder<CompletableFuture<GetColumnStatisticsForPartitionResponse>> futures = ImmutableList.builder();
            List<List<Column>> columnChunks = Lists.partition(partition.getColumns(), GLUE_COLUMN_READ_STAT_PAGE_SIZE);

            for (List<Column> partialPartitionColumns : columnChunks) {
                List<String> columnNames = partialPartitionColumns.stream()
                        .map(Column::getName)
                        .collect(toImmutableList());

                GetColumnStatisticsForPartitionRequest request = GetColumnStatisticsForPartitionRequest.builder()
                        .catalogId(catalogId)
                        .databaseName(partition.getDatabaseName())
                        .tableName(partition.getTableName())
                        .columnNames(columnNames)
                        .partitionValues(partition.getValues())
                        .build();

                futures.add(supplyAsync(() ->
                        awsSyncRequest(glueClient::getColumnStatisticsForPartition, request, stats.getGetColumnStatisticsForPartition()),
                        readExecutor));
            }
            futuresPerPartition.put(partition, futures.build());
        }

        try {
            ImmutableMap.Builder<Partition, List<ColumnStatistics>> result = ImmutableMap.builder();

            futuresPerPartition.forEach((partition, futures) -> {
                ImmutableList.Builder<ColumnStatistics> columnStats = ImmutableList.builder();

                for (CompletableFuture<GetColumnStatisticsForPartitionResponse> future : futures) {
                    GetColumnStatisticsForPartitionResponse response = getFutureValue(future);
                    columnStats.addAll(response.columnStatisticsList());
                }

                result.put(partition, columnStats.build());
            });

            return result.build();
        }
        catch (EntityNotFoundException ex) {
            throw new PrestoException(HIVE_PARTITION_NOT_FOUND, ex);
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public void updateTableColumnStatistics(Table table, List<ColumnStatistics> columnStatistics)
    {
        try {
            // Get current statistics to determine which columns to delete
            List<ColumnStatistics> currentStats = getTableColumnStatistics(table);
            Set<String> updatedColumnNames = columnStatistics.stream()
                    .map(ColumnStatistics::columnName)
                    .collect(Collectors.toSet());

            List<String> columnsToDelete = currentStats.stream()
                    .map(ColumnStatistics::columnName)
                    .filter(name -> !updatedColumnNames.contains(name))
                    .collect(toImmutableList());

            // Update statistics in batches
            List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStatistics, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);
            List<CompletableFuture<Void>> updateFutures = columnChunks.stream()
                    .map(columnChunk -> runAsync(() ->
                            awsSyncRequest(
                                    glueClient::updateColumnStatisticsForTable,
                                    UpdateColumnStatisticsForTableRequest.builder()
                                            .catalogId(catalogId)
                                            .databaseName(table.getDatabaseName())
                                            .tableName(table.getTableName())
                                            .columnStatisticsList(columnChunk)
                                            .build(),
                                    stats.getUpdateColumnStatisticsForTable()),
                            writeExecutor))
                    .collect(toImmutableList());

            // Delete removed statistics
            List<CompletableFuture<Void>> deleteFutures = columnsToDelete.stream()
                    .map(columnName -> runAsync(() ->
                            awsSyncRequest(
                                    glueClient::deleteColumnStatisticsForTable,
                                    DeleteColumnStatisticsForTableRequest.builder()
                                            .catalogId(catalogId)
                                            .databaseName(table.getDatabaseName())
                                            .tableName(table.getTableName())
                                            .columnName(columnName)
                                            .build(),
                                    stats.getDeleteColumnStatisticsForTable()),
                            writeExecutor))
                    .collect(toImmutableList());

            // Wait for all operations to complete
            List<CompletableFuture<Void>> allFutures = ImmutableList.<CompletableFuture<Void>>builder()
                    .addAll(updateFutures)
                    .addAll(deleteFutures)
                    .build();

            getFutureValue(allOf(allFutures.toArray(new CompletableFuture[0])));
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public void updatePartitionColumnStatistics(Map<Partition, List<ColumnStatistics>> updates)
    {
        try {
            // Get current statistics for all partitions to determine which columns to delete
            Map<Partition, List<ColumnStatistics>> currentStats = getPartitionColumnStatistics(updates.keySet());

            List<CompletableFuture<Void>> allFutures = new ArrayList<>();

            for (Map.Entry<Partition, List<ColumnStatistics>> entry : updates.entrySet()) {
                Partition partition = entry.getKey();
                List<ColumnStatistics> newStats = entry.getValue();

                Set<String> updatedColumnNames = newStats.stream()
                        .map(ColumnStatistics::columnName)
                        .collect(Collectors.toSet());

                List<String> columnsToDelete = currentStats.getOrDefault(partition, ImmutableList.of()).stream()
                        .map(ColumnStatistics::columnName)
                        .filter(name -> !updatedColumnNames.contains(name))
                        .collect(toImmutableList());

                // Update statistics in batches
                List<List<ColumnStatistics>> columnChunks = Lists.partition(newStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);
                for (List<ColumnStatistics> columnChunk : columnChunks) {
                    allFutures.add(runAsync(() ->
                            awsSyncRequest(
                                    glueClient::updateColumnStatisticsForPartition,
                                    UpdateColumnStatisticsForPartitionRequest.builder()
                                            .catalogId(catalogId)
                                            .databaseName(partition.getDatabaseName())
                                            .tableName(partition.getTableName())
                                            .partitionValues(partition.getValues())
                                            .columnStatisticsList(columnChunk)
                                            .build(),
                                    stats.getUpdateColumnStatisticsForPartition()),
                            writeExecutor));
                }

                // Delete removed statistics
                for (String columnName : columnsToDelete) {
                    allFutures.add(runAsync(() ->
                            awsSyncRequest(
                                    glueClient::deleteColumnStatisticsForPartition,
                                    DeleteColumnStatisticsForPartitionRequest.builder()
                                            .catalogId(catalogId)
                                            .databaseName(partition.getDatabaseName())
                                            .tableName(partition.getTableName())
                                            .partitionValues(partition.getValues())
                                            .columnName(columnName)
                                            .build(),
                                    stats.getDeleteColumnStatisticsForPartition()),
                            writeExecutor));
                }
            }

            // Wait for all operations to complete
            getFutureValue(allOf(allFutures.toArray(new CompletableFuture[0])));
        }
        catch (EntityNotFoundException ex) {
            throw new PrestoException(HIVE_PARTITION_NOT_FOUND, ex);
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    private List<String> getAllColumnNames(Table table)
    {
        ImmutableList.Builder<String> allColumns = ImmutableList.builderWithExpectedSize(
                table.getDataColumns().size() + table.getPartitionColumns().size());
        table.getDataColumns().stream().map(Column::getName).forEach(allColumns::add);
        table.getPartitionColumns().stream().map(Column::getName).forEach(allColumns::add);
        return allColumns.build();
    }
}
