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

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.model.ColumnStatistics;
import com.amazonaws.services.glue.model.ColumnStatisticsData;
import com.amazonaws.services.glue.model.ColumnStatisticsType;
import com.amazonaws.services.glue.model.DateColumnStatisticsData;
import com.amazonaws.services.glue.model.DecimalColumnStatisticsData;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.DeleteColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.DoubleColumnStatisticsData;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForPartitionResult;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableRequest;
import com.amazonaws.services.glue.model.GetColumnStatisticsForTableResult;
import com.amazonaws.services.glue.model.LongColumnStatisticsData;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForPartitionRequest;
import com.amazonaws.services.glue.model.UpdateColumnStatisticsForTableRequest;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.MetastoreUtil;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PARTITION_NOT_FOUND;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static com.facebook.presto.hive.metastore.glue.converter.GlueStatConverter.fromGlueColumnStatistics;
import static com.facebook.presto.hive.metastore.glue.converter.GlueStatConverter.toGlueColumnStatistics;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

public class DefaultGlueColumnStatisticsProvider
        implements GlueColumnStatisticsProvider
{
    // Read limit for AWS Glue API GetColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetColumnStatisticsForPartition
    private static final int GLUE_COLUMN_READ_STAT_PAGE_SIZE = 100;

    // Write limit for AWS Glue API UpdateColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-UpdateColumnStatisticsForPartition
    private static final int GLUE_COLUMN_WRITE_STAT_PAGE_SIZE = 25;

    private final AWSGlueAsync glueClient;
    private final String catalogId;
    private final Executor readExecutor;
    private final Executor writeExecutor;
    private final GlueMetastoreStats stats;

    public DefaultGlueColumnStatisticsProvider(AWSGlueAsync glueClient, String catalogId, Executor readExecutor, Executor writeExecutor, GlueHiveMetastoreConfig glueConfig,
            GlueMetastoreStats stats)
    {
        this.glueClient = glueClient;
        this.catalogId = catalogId;
        this.readExecutor = readExecutor;
        this.writeExecutor = writeExecutor;
        this.stats = stats;
    }

    private AmazonWebServiceRequest updateAWSRequest(MetastoreContext metastoreContext, AmazonWebServiceRequest request)
    {
        return request;
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return MetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(MetastoreContext metastoreContext, Table table)
    {
        try {
            List<String> columnNames = getAllColumns(table);
            List<List<String>> columnChunks = Lists.partition(columnNames, GLUE_COLUMN_READ_STAT_PAGE_SIZE);
            List<CompletableFuture<GetColumnStatisticsForTableResult>> getStatsFutures = columnChunks.stream()
                    .map(partialColumns -> supplyAsync(() -> {
                        GetColumnStatisticsForTableRequest request = (GetColumnStatisticsForTableRequest) updateAWSRequest(
                                metastoreContext,
                                new GetColumnStatisticsForTableRequest()
                                        .withCatalogId(catalogId)
                                        .withDatabaseName(table.getDatabaseName())
                                        .withTableName(table.getTableName())
                                        .withColumnNames(partialColumns));
                        return stats.getGetColumnStatisticsForTable().record(() -> glueClient.getColumnStatisticsForTable(request));
                    }, readExecutor)).collect(toImmutableList());

            HiveBasicStatistics tableStatistics = getHiveBasicStatistics(table.getParameters());
            ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();
            for (CompletableFuture<GetColumnStatisticsForTableResult> future : getStatsFutures) {
                GetColumnStatisticsForTableResult tableColumnsStats = getFutureValue(future, PrestoException.class);
                for (ColumnStatistics columnStatistics : tableColumnsStats.getColumnStatisticsList()) {
                    columnStatsMapBuilder.put(
                            columnStatistics.getColumnName(),
                            fromGlueColumnStatistics(columnStatistics.getStatisticsData(), tableStatistics.getRowCount()));
                }
            }
            return columnStatsMapBuilder.build();
        }
        catch (RuntimeException ex) {
            throw new PrestoException(HIVE_METASTORE_ERROR, ex);
        }
    }

    private Optional<Map<String, HiveColumnStatistics>> getPartitionColumnStatisticsIfPresent(MetastoreContext metastoreContext, Partition partition)
    {
        try {
            return Optional.of(getPartitionColumnStatistics(metastoreContext, partition));
        }
        catch (PrestoException ex) {
            if (ex.getErrorCode() == HIVE_PARTITION_NOT_FOUND.toErrorCode()) {
                return Optional.empty();
            }
            throw ex;
        }
    }

    @Override
    public Map<Partition, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(MetastoreContext metastoreContext, Collection<Partition> partitions)
    {
        Map<Partition, List<CompletableFuture<GetColumnStatisticsForPartitionResult>>> resultsForPartition = new HashMap<>();
        for (Partition partition : partitions) {
            ImmutableList.Builder<CompletableFuture<GetColumnStatisticsForPartitionResult>> futures = ImmutableList.builder();
            List<List<Column>> columnChunks = Lists.partition(partition.getColumns(), GLUE_COLUMN_READ_STAT_PAGE_SIZE);
            for (List<Column> partialPartitionColumns : columnChunks) {
                List<String> columnsNames = partialPartitionColumns.stream()
                        .map(Column::getName)
                        .collect(toImmutableList());
                GetColumnStatisticsForPartitionRequest request = (GetColumnStatisticsForPartitionRequest) updateAWSRequest(
                        metastoreContext,
                        new GetColumnStatisticsForPartitionRequest()
                                .withCatalogId(catalogId)
                                .withDatabaseName(partition.getDatabaseName())
                                .withTableName(partition.getTableName())
                                .withColumnNames(columnsNames)
                                .withPartitionValues(partition.getValues()));
                futures.add(supplyAsync(() -> stats.getGetColumnStatisticsForPartition().record(() ->
                        glueClient.getColumnStatisticsForPartition(request)), readExecutor));
            }
            resultsForPartition.put(partition, futures.build());
        }

        try {
            ImmutableMap.Builder<Partition, Map<String, HiveColumnStatistics>> partitionStatistics = ImmutableMap.builder();
            resultsForPartition.forEach((partition, futures) -> {
                HiveBasicStatistics tableStatistics = getHiveBasicStatistics(partition.getParameters());
                ImmutableMap.Builder<String, HiveColumnStatistics> columnStatsMapBuilder = ImmutableMap.builder();

                for (CompletableFuture<GetColumnStatisticsForPartitionResult> getColumnStatisticsResultFuture : futures) {
                    GetColumnStatisticsForPartitionResult getColumnStatisticsResult = getFutureValue(getColumnStatisticsResultFuture);
                    getColumnStatisticsResult.getColumnStatisticsList().forEach(columnStatistics ->
                            columnStatsMapBuilder.put(
                                    columnStatistics.getColumnName(),
                                    fromGlueColumnStatistics(columnStatistics.getStatisticsData(), tableStatistics.getRowCount())));
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
        ColumnStatisticsData statisticsData = stats.getStatisticsData();
        String columnType = stats.getStatisticsData().getType();
        if (columnType.equals(ColumnStatisticsType.DATE.toString())) {
            DateColumnStatisticsData data = statisticsData.getDateColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.DECIMAL.toString())) {
            DecimalColumnStatisticsData data = statisticsData.getDecimalColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.DOUBLE.toString())) {
            DoubleColumnStatisticsData data = statisticsData.getDoubleColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        else if (columnType.equals(ColumnStatisticsType.LONG.toString())) {
            LongColumnStatisticsData data = statisticsData.getLongColumnStatisticsData();
            return data.getMaximumValue() != null && data.getMinimumValue() != null;
        }
        return true;
    }

    @Override
    public void updateTableColumnStatistics(MetastoreContext metastoreContext, Table table, Map<String, HiveColumnStatistics> updatedTableColumnStatistics)
    {
        try {
            HiveBasicStatistics tableStats = getHiveBasicStatistics(table.getParameters());
            List<ColumnStatistics> columnStats = toGlueColumnStatistics(table, updatedTableColumnStatistics, tableStats.getRowCount()).stream()
                    .filter(this::isGlueWritable)
                    .collect(toImmutableList());

            List<List<ColumnStatistics>> columnChunks = Lists.partition(columnStats, GLUE_COLUMN_WRITE_STAT_PAGE_SIZE);

            List<CompletableFuture<Void>> updateFutures = columnChunks.stream().map(columnChunk -> runAsync(() -> {
                UpdateColumnStatisticsForTableRequest request = (UpdateColumnStatisticsForTableRequest) updateAWSRequest(
                                metastoreContext,
                                new UpdateColumnStatisticsForTableRequest()
                                        .withCatalogId(catalogId)
                                        .withDatabaseName(table.getDatabaseName())
                                        .withTableName(table.getTableName())
                                        .withColumnStatisticsList(columnChunk));
                stats.getUpdateColumnStatisticsForTable().record(() -> glueClient.updateColumnStatisticsForTable(request));
            }, this.writeExecutor)).collect(toImmutableList());

            Map<String, HiveColumnStatistics> currentTableColumnStatistics = this.getTableColumnStatistics(metastoreContext, table);
            Set<String> removedStatistics = difference(currentTableColumnStatistics.keySet(), updatedTableColumnStatistics.keySet());
            List<CompletableFuture<Void>> deleteFutures = removedStatistics.stream()
                    .map(column -> runAsync(
                            () -> {
                                DeleteColumnStatisticsForTableRequest request = (DeleteColumnStatisticsForTableRequest) updateAWSRequest(
                                        metastoreContext,
                                        new DeleteColumnStatisticsForTableRequest()
                                                .withCatalogId(catalogId)
                                                .withDatabaseName(table.getDatabaseName())
                                                .withTableName(table.getTableName())
                                                .withColumnName(column));
                                stats.getDeleteColumnStatisticsForTable().record(() -> glueClient.deleteColumnStatisticsForTable(request));
                            }, this.writeExecutor))
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
    public void updatePartitionStatistics(MetastoreContext metastoreContext, Set<PartitionStatisticsUpdate> partitionStatisticsUpdates)
    {
        Map<Partition, Map<String, HiveColumnStatistics>> currentStatistics = getPartitionColumnStatistics(
                metastoreContext,
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
                    updateFutures.add(runAsync(() -> stats.getUpdateColumnStatisticsForPartition().record(() ->
                                    glueClient.updateColumnStatisticsForPartition(
                                            (UpdateColumnStatisticsForPartitionRequest) updateAWSRequest(
                                                    metastoreContext,
                                                    new UpdateColumnStatisticsForPartitionRequest()
                                                            .withCatalogId(catalogId)
                                                            .withDatabaseName(partition.getDatabaseName())
                                                            .withTableName(partition.getTableName())
                                                            .withPartitionValues(partition.getValues())
                                                            .withColumnStatisticsList(columnChunk)))),
                            writeExecutor)));

            Set<String> removedStatistics = difference(currentStatistics.get(partition).keySet(), updatedColumnStatistics.keySet());
            removedStatistics.forEach(column ->
                    updateFutures.add(runAsync(() -> stats.getDeleteColumnStatisticsForPartition().record(() ->
                                    glueClient.deleteColumnStatisticsForPartition(
                                            (DeleteColumnStatisticsForPartitionRequest) updateAWSRequest(
                                                    metastoreContext,
                                                    new DeleteColumnStatisticsForPartitionRequest()
                                                            .withCatalogId(catalogId)
                                                            .withDatabaseName(partition.getDatabaseName())
                                                            .withTableName(partition.getTableName())
                                                            .withPartitionValues(partition.getValues())
                                                            .withColumnName(column)))),
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
