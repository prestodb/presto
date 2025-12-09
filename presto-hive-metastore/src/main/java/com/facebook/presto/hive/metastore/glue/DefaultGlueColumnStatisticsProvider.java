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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;
import software.amazon.awssdk.services.glue.model.DateColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.DoubleColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableRequest;
import software.amazon.awssdk.services.glue.model.GetColumnStatisticsForTableResponse;
import software.amazon.awssdk.services.glue.model.LongColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateColumnStatisticsForTableRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_NOT_FOUND;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.facebook.presto.hive.metastore.glue.GlueHiveMetastore.awsSyncRequest;
import static com.facebook.presto.hive.metastore.glue.converter.GlueStatConverter.fromGlueColumnStatistics;
import static com.facebook.presto.hive.metastore.glue.converter.GlueStatConverter.toGlueColumnStatistics;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static software.amazon.awssdk.services.glue.model.ColumnStatisticsType.DATE;
import static software.amazon.awssdk.services.glue.model.ColumnStatisticsType.DECIMAL;
import static software.amazon.awssdk.services.glue.model.ColumnStatisticsType.DOUBLE;
import static software.amazon.awssdk.services.glue.model.ColumnStatisticsType.LONG;

public class DefaultGlueColumnStatisticsProvider
        implements GlueColumnStatisticsProvider
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

    public DefaultGlueColumnStatisticsProvider(GlueAsyncClient glueClient,
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
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return MetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(Table table)
    {
        try {
            List<String> columnNames = getAllColumns(table);
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

            HiveBasicStatistics tableStatistics = getHiveBasicStatistics(table.getParameters());
            ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
            for (CompletableFuture<GetColumnStatisticsForTableResponse> future : getStatsFutures) {
                GetColumnStatisticsForTableResponse tableColumnsStats = getFutureValue(future, PrestoException.class);
                for (ColumnStatistics columnStatistics : tableColumnsStats.columnStatisticsList()) {
                    columnStatsMapBuilder.put(
                            columnStatistics.columnName(),
                            fromGlueColumnStatistics(columnStatistics.statisticsData(), tableStatistics.getRowCount()));
                }
            }
            return columnStatsMapBuilder.build();
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public Map<Partition, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(Collection<Partition> partitions)
    {
        Map<Partition, List<CompletableFuture<GetColumnStatisticsForPartitionResponse>>> resultsForPartition = new HashMap<>();
        for (Partition partition : partitions) {
            ImmutableList.Builder<CompletableFuture<GetColumnStatisticsForPartitionResponse>> futures = ImmutableList.builder();
            List<List<Column>> columnChunks = Lists.partition(partition.getColumns(), GLUE_COLUMN_READ_STAT_PAGE_SIZE);
            for (List<Column> partialPartitionColumns : columnChunks) {
                List<String> columnsNames = partialPartitionColumns.stream()
                        .map(Column::getName)
                        .collect(toImmutableList());

                GetColumnStatisticsForPartitionRequest request = GetColumnStatisticsForPartitionRequest.builder()
                        .catalogId(catalogId)
                        .databaseName(partition.getDatabaseName())
                        .tableName(partition.getTableName())
                        .columnNames(columnsNames)
                        .partitionValues(partition.getValues())
                        .build();

                futures.add(supplyAsync(() ->
                        awsSyncRequest(glueClient::getColumnStatisticsForPartition, request, stats.getGetColumnStatisticsForPartition()),
                        readExecutor));
            }
            resultsForPartition.put(partition, futures.build());
        }

        try {
            ImmutableMap.Builder<Partition, Map<String, HiveColumnStatistics>> partitionStatistics = ImmutableMap.builder();
            resultsForPartition.forEach((partition, futures) -> {
                HiveBasicStatistics tableStatistics = getHiveBasicStatistics(partition.getParameters());
                ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();

                for (CompletableFuture<GetColumnStatisticsForPartitionResponse> getColumnStatisticsResultFuture : futures) {
                    GetColumnStatisticsForPartitionResponse getColumnStatisticsResult = getFutureValue(getColumnStatisticsResultFuture);
                    getColumnStatisticsResult.columnStatisticsList().forEach(columnStatistics ->
                            columnStatsMapBuilder.put(
                                    columnStatistics.columnName(),
                                    fromGlueColumnStatistics(columnStatistics.statisticsData(), tableStatistics.getRowCount())));
                }

                partitionStatistics.put(partition, columnStatsMapBuilder.build());
            });

            return partitionStatistics.build();
        }
        catch (EntityNotFoundException ex) {
            throw new PrestoException(HIVE_PARTITION_NOT_FOUND, ex);
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    // Glue will accept null as min/max values but return 0 when reading
    // to avoid incorrect stats we skip writes for column statistics that have min/max null
    // this can be removed once glue fix this behaviour
    private boolean isGlueWritable(ColumnStatistics stats)
    {
        ColumnStatisticsData statisticsData = stats.statisticsData();
        ColumnStatisticsType columnType = stats.statisticsData().type();
        if (columnType.equals(DATE)) {
            DateColumnStatisticsData data = statisticsData.dateColumnStatisticsData();
            return data.maximumValue() != null && data.minimumValue() != null;
        }
        else if (columnType.equals(DECIMAL)) {
            DecimalColumnStatisticsData data = statisticsData.decimalColumnStatisticsData();
            return data.maximumValue() != null && data.minimumValue() != null;
        }
        else if (columnType.equals(DOUBLE)) {
            DoubleColumnStatisticsData data = statisticsData.doubleColumnStatisticsData();
            return data.maximumValue() != null && data.minimumValue() != null;
        }
        else if (columnType.equals(LONG)) {
            LongColumnStatisticsData data = statisticsData.longColumnStatisticsData();
            return data.maximumValue() != null && data.minimumValue() != null;
        }
        return true;
    }

    @Override
    public void updateTableColumnStatistics(Table table, Map<String, HiveColumnStatistics> updatedTableColumnStatistics)
    {
        try {
            HiveBasicStatistics tableStats = getHiveBasicStatistics(table.getParameters());
            List<ColumnStatistics> columnStats = toGlueColumnStatistics(table, updatedTableColumnStatistics, tableStats.getRowCount()).stream()
                    .filter(this::isGlueWritable)
                    .collect(toImmutableList());

            List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);

            List<CompletableFuture<Void>> updateFutures = columnChunks.stream().map(columnChunk -> runAsync(
                    () -> awsSyncRequest(
                            glueClient::updateColumnStatisticsForTable,
                            UpdateColumnStatisticsForTableRequest.builder()
                                    .catalogId(catalogId)
                                    .databaseName(table.getDatabaseName())
                                    .tableName(table.getTableName())
                                    .columnStatisticsList(columnChunk)
                                    .build(),
                            stats.getUpdateColumnStatisticsForTable()), this.writeExecutor))
                    .collect(toImmutableList());

            Map<String, HiveColumnStatistics> currentTableColumnStatistics = this.getTableColumnStatistics(table);
            Set<String> removedStatistics = difference(currentTableColumnStatistics.keySet(), updatedTableColumnStatistics.keySet());
            List<CompletableFuture<Void>> deleteFutures = removedStatistics.stream().map(column -> runAsync(
                    () -> awsSyncRequest(
                            glueClient::deleteColumnStatisticsForTable,
                            DeleteColumnStatisticsForTableRequest.builder()
                                    .catalogId(catalogId)
                                    .databaseName(table.getDatabaseName())
                                    .tableName(table.getTableName())
                                    .columnName(column)
                                    .build(),
                            stats.getDeleteColumnStatisticsForTable()), this.writeExecutor))
                    .collect(toImmutableList());

            ImmutableList<CompletableFuture<Void>> updateOperationsFutures = ImmutableList.<CompletableFuture<Void>>builder()
                    .addAll(updateFutures)
                    .addAll(deleteFutures)
                    .build();

            getFutureValue(CompletableFuture.allOf(updateOperationsFutures.toArray(new CompletableFuture[0])));
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    @Override
    public void updatePartitionStatistics(Set<PartitionStatisticsUpdate> partitionStatisticsUpdates)
    {
        Map<Partition, Map<String, HiveColumnStatistics>> currentStatistics = getPartitionColumnStatistics(
                partitionStatisticsUpdates.stream()
                        .map(PartitionStatisticsUpdate::getPartition).collect(toImmutableList()));

        List<CompletableFuture<Void>> updateFutures = new ArrayList<>();
        for (PartitionStatisticsUpdate update : partitionStatisticsUpdates) {
            Partition partition = update.getPartition();
            Map<String, HiveColumnStatistics> updatedColumnStatistics = update.getColumnStatistics();

            HiveBasicStatistics partitionStats = getHiveBasicStatistics(partition.getParameters());
            List<ColumnStatistics> columnStats = toGlueColumnStatistics(partition, updatedColumnStatistics, partitionStats.getRowCount()).stream()
                    .filter(this::isGlueWritable)
                    .collect(toImmutableList());

            List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);
            columnChunks.forEach(columnChunk ->
                    updateFutures.add(runAsync(() ->
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
                            writeExecutor)));

            Set<String> removedStatistics = difference(currentStatistics.get(partition).keySet(), updatedColumnStatistics.keySet());
            removedStatistics.forEach(column ->
                    updateFutures.add(runAsync(() ->
                                    awsSyncRequest(
                                            glueClient::deleteColumnStatisticsForPartition,
                                            DeleteColumnStatisticsForPartitionRequest.builder()
                                                    .catalogId(catalogId)
                                                    .databaseName(partition.getDatabaseName())
                                                    .tableName(partition.getTableName())
                                                    .partitionValues(partition.getValues())
                                                    .columnName(column)
                                                    .build(),
                                            stats.getDeleteColumnStatisticsForPartition()),
                            writeExecutor)));
        }
        try {
            getFutureValue(allOf(updateFutures.stream().toArray(CompletableFuture[]::new)));
        }
        catch (RuntimeException ex) {
            if (ex.getCause() != null && ex.getCause() instanceof EntityNotFoundException) {
                throw new PrestoException(HIVE_PARTITION_NOT_FOUND, ex.getCause());
            }
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    private List<String> getAllColumns(Table table)
    {
        ImmutableList.Builder<String> allColumns = ImmutableList.builderWithExpectedSize(table.getDataColumns().size() + table.getPartitionColumns().size());
        table.getDataColumns().stream().map(Column::getName).forEach(allColumns::add);
        table.getPartitionColumns().stream().map(Column::getName).forEach(allColumns::add);
        return allColumns.build();
    }
}
