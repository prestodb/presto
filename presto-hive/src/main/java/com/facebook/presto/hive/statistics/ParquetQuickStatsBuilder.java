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

package com.facebook.presto.hive.statistics;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.concurrent.ThreadPoolExecutorMBean;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.stats.DistributionStat;
import com.facebook.airlift.stats.TimeStat;
import com.facebook.presto.common.RuntimeUnit;
import com.facebook.presto.hive.FileFormatDataSourceStats;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveFileContext;
import com.facebook.presto.hive.HiveFileInfo;
import com.facebook.presto.hive.PartitionNameWithVersion;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.parquet.ParquetDataSource;
import com.facebook.presto.parquet.cache.ParquetFileMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.hive.CacheQuota.NO_CACHE_CONSTRAINTS;
import static com.facebook.presto.hive.HiveCommonSessionProperties.getReadNullMaskedParquetEncryptedValue;
import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.PARQUET_SERDE_CLASS_NAMES;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.createDecryptor;
import static com.facebook.presto.parquet.cache.MetadataReader.readFooter;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ParquetQuickStatsBuilder
        implements QuickStatsBuilder
{
    public static final Logger log = Logger.get(ParquetQuickStatsBuilder.class);
    private final Executor footerFetchExecutor;
    private final ThreadPoolExecutorMBean footerFetchExecutorMBean;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final long footerFetchTimeoutMillis;
    private final TimeStat footerFetchDuration = new TimeStat(MILLISECONDS);
    private final DistributionStat fileCountPerPartition = new DistributionStat();
    private final DistributionStat footerByteSizeDistribution = new DistributionStat();

    public ParquetQuickStatsBuilder(FileFormatDataSourceStats stats, HdfsEnvironment hdfsEnvironment, HiveClientConfig hiveClientConfig)
    {
        this.stats = stats;
        this.hdfsEnvironment = hdfsEnvironment;
        this.footerFetchTimeoutMillis = hiveClientConfig.getParquetQuickStatsFileMetadataFetchTimeout().roundTo(MILLISECONDS);
        ExecutorService coreExecutor = newCachedThreadPool(daemonThreadsNamed("parquet-quick-stats-bg-fetch-%s"));
        this.footerFetchExecutor = new BoundedExecutor(coreExecutor, hiveClientConfig.getMaxConcurrentParquetQuickStatsCalls());
        this.footerFetchExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) coreExecutor);
    }

    private static void processColumnMetadata(ParquetMetadata parquetMetadata, Map<ColumnPath, ColumnQuickStats<?>> rolledUpColStats)
    {
        List<BlockMetaData> rowGroups = parquetMetadata.getBlocks();
        for (BlockMetaData rowGroup : rowGroups) {
            long rowCount = rowGroup.getRowCount();

            for (ColumnChunkMetaData columnChunkMetaData : rowGroup.getColumns()) {
                ColumnPath columnKey = columnChunkMetaData.getPath();
                if (columnKey.size() > 1) {
                    // We do not support reading/using stats for nested columns at the moment. These columns have a HiveColumnHandle#ColumnType == SYNTHESIZED
                    // TODO : When we do add this support, map the column handles to the parquet path to build stats for these nested columns
                    continue;
                }
                String columnName = columnKey.toArray()[0];
                PrimitiveType columnPrimitiveType = columnChunkMetaData.getPrimitiveType();

                Statistics colStats = columnChunkMetaData.getStatistics();
                long nullsCount = colStats.getNumNulls();

                // We set the default the mapped column type to SLICE since this treats the column as a collection of bytes
                // with no min/max stats. The only relevant stats are NULL count and ROW count
                ColumnType mappedType = ColumnType.SLICE;
                switch (columnPrimitiveType.getPrimitiveTypeName()) {
                    case INT64:
                        mappedType = ColumnType.LONG;
                        break;
                    case INT32:
                        mappedType = ColumnType.INTEGER;
                        break;
                    case BOOLEAN:
                        mappedType = ColumnType.BOOLEAN;
                        break;
                    case BINARY:
                        // BINARY primitive type should be mapped to SLICE since it won't have a min/max
                        mappedType = ColumnType.SLICE;
                        break;
                    case FLOAT:
                        mappedType = ColumnType.FLOAT;
                        break;
                    case DOUBLE:
                        mappedType = ColumnType.DOUBLE;
                        break;
                    default:
                    case INT96:
                    case FIXED_LEN_BYTE_ARRAY:
                        break;
                }

                if (columnPrimitiveType.getLogicalTypeAnnotation() != null) {
                    // Use logical information to decipher stats info for specific logical types
                    Optional<ColumnType> transformed = columnPrimitiveType.getLogicalTypeAnnotation().accept(new LogicalTypeAnnotationVisitor<ColumnType>()
                    {
                        @Override
                        public Optional<ColumnType> visit(DateLogicalTypeAnnotation dateLogicalType)
                        {
                            return Optional.of(ColumnType.DATE);
                        }

                        @Override
                        public Optional<ColumnType> visit(TimeLogicalTypeAnnotation timeLogicalType)
                        {
                            return Optional.of(ColumnType.TIME);
                        }
                    });

                    if (transformed.isPresent()) {
                        mappedType = transformed.get();
                    }
                }

                switch (mappedType) {
                    case INTEGER: {
                        ColumnQuickStats<Integer> toMerge = (ColumnQuickStats<Integer>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Integer.class));
                        IntStatistics asIntegerStats = ((IntStatistics) colStats);
                        toMerge.setMinValue(asIntegerStats.getMin());
                        toMerge.setMaxValue(asIntegerStats.getMax());
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                    case LONG: {
                        ColumnQuickStats<Long> toMerge = (ColumnQuickStats<Long>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Long.class));
                        LongStatistics asLongStats = ((LongStatistics) colStats);
                        toMerge.setMinValue(asLongStats.getMin());
                        toMerge.setMaxValue(asLongStats.getMax());
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }

                    case DOUBLE: {
                        ColumnQuickStats<Double> toMerge = (ColumnQuickStats<Double>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Double.class));
                        DoubleStatistics asDoubleStats = ((DoubleStatistics) colStats);
                        toMerge.setMinValue(asDoubleStats.getMin());
                        toMerge.setMaxValue(asDoubleStats.getMax());
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                    case FLOAT: {
                        ColumnQuickStats<Float> toMerge = (ColumnQuickStats<Float>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Float.class));
                        FloatStatistics asFloatStats = ((FloatStatistics) colStats);
                        toMerge.setMinValue(asFloatStats.getMin());
                        toMerge.setMaxValue(asFloatStats.getMax());
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                    case BOOLEAN: {
                        ColumnQuickStats<Boolean> toMerge = (ColumnQuickStats<Boolean>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Boolean.class));
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        // TODO : Boolean stats store trueCount and falseCount
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                    case DATE: {
                        ColumnQuickStats<ChronoLocalDate> toMerge = (ColumnQuickStats<ChronoLocalDate>) rolledUpColStats.getOrDefault(columnKey,
                                new ColumnQuickStats<>(columnName, ChronoLocalDate.class));
                        IntStatistics asIntStats = ((IntStatistics) colStats);
                        toMerge.setMinValue(LocalDate.ofEpochDay(asIntStats.getMin()));
                        toMerge.setMaxValue(LocalDate.ofEpochDay(asIntStats.getMax()));
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                    default:
                    case SLICE: {
                        ColumnQuickStats<Slice> toMerge = (ColumnQuickStats<Slice>) rolledUpColStats.getOrDefault(columnKey, new ColumnQuickStats<>(columnName, Slice.class));
                        toMerge.addToNullsCount(nullsCount);
                        toMerge.addToRowCount(rowCount);
                        rolledUpColStats.put(columnKey, toMerge);
                        break;
                    }
                }
            }
        }
    }

    @Managed
    @Nested
    public TimeStat getFooterFetchDuration()
    {
        return footerFetchDuration;
    }

    @Managed
    @Nested
    public DistributionStat getFooterByteSizeDistribution()
    {
        return footerByteSizeDistribution;
    }

    @Managed
    @Nested
    public DistributionStat getFileCountPerPartitionDistribution()
    {
        return fileCountPerPartition;
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return footerFetchExecutorMBean;
    }

    @Override
    public PartitionQuickStats buildQuickStats(ConnectorSession session, ExtendedHiveMetastore metastore,
            SchemaTableName table, MetastoreContext metastoreContext, String partitionId, Iterator<HiveFileInfo> files)
    {
        requireNonNull(session);
        requireNonNull(metastore);
        requireNonNull(table);
        requireNonNull(metastoreContext);
        requireNonNull(partitionId);
        requireNonNull(files);

        if (!files.hasNext()) {
            return PartitionQuickStats.EMPTY;
        }

        // TODO: Consider refactoring storage and/or table format to the interface when we implement an ORC/Iceberg quick stats builder
        StorageFormat storageFormat;
        if (UNPARTITIONED_ID.getPartitionName().equals(partitionId)) {
            Table resolvedTable = metastore.getTable(metastoreContext, table.getSchemaName(), table.getTableName()).get();
            storageFormat = resolvedTable.getStorage().getStorageFormat();
        }
        else {
            Partition partition = metastore.getPartitionsByNames(metastoreContext, table.getSchemaName(), table.getTableName(),
                    ImmutableList.of(new PartitionNameWithVersion(partitionId, Optional.empty()))).get(partitionId).get();
            storageFormat = partition.getStorage().getStorageFormat();
        }

        if (!PARQUET_SERDE_CLASS_NAMES.contains(storageFormat.getSerDe())) {
            // Not a parquet table/partition
            return PartitionQuickStats.EMPTY;
        }

        // We want to keep the number of files we use to build quick stats bounded, so that
        // 1. We can control total file IO overhead in a measurable way
        // 2. Planning time remains bounded
        // Future work here is to sample the file list, read their stats only and extrapolate the overall stats (TODO)
        List<CompletableFuture<ParquetMetadata>> footerFetchCompletableFutures = new ArrayList<>();
        int filesCount = 0;
        while (files.hasNext()) {
            HiveFileInfo file = files.next();
            filesCount++;
            Path path = new Path(file.getPath());
            long fileSize = file.getLength();

            HiveFileContext hiveFileContext = new HiveFileContext(
                    true,
                    NO_CACHE_CONSTRAINTS,
                    Optional.empty(),
                    OptionalLong.of(fileSize),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    file.getFileModifiedTime(),
                    false);

            HdfsContext hdfsContext = new HdfsContext(session, table.getSchemaName(), table.getTableName());
            Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);

            footerFetchCompletableFutures.add(supplyAsync(() -> {
                Stopwatch footerFetchDuration = Stopwatch.createStarted();
                try (FSDataInputStream inputStream = hdfsEnvironment.getFileSystem(hdfsContext, path).openFile(path, hiveFileContext);
                        ParquetDataSource parquetDataSource = buildHdfsParquetDataSource(inputStream, path, stats)) {
                    ParquetFileMetadata parquetFileMetadata = readFooter(parquetDataSource,
                            fileSize,
                            createDecryptor(configuration, path),
                            getReadNullMaskedParquetEncryptedValue(session));

                    footerByteSizeDistribution.add(parquetFileMetadata.getMetadataSize());
                    return parquetFileMetadata.getParquetMetadata();
                }
                catch (Exception e) {
                    log.error(e);
                    throw new RuntimeException(e);
                }
                finally {
                    this.footerFetchDuration.add(footerFetchDuration.elapsed(MILLISECONDS), MILLISECONDS);
                }
            }, footerFetchExecutor));
        }

        // Record a metric about how many files were seen
        session.getRuntimeStats().addMetricValue(String.format("ParquetQuickStatsBuilder/FileCount/%s/%s", table.getTableName(), partitionId), RuntimeUnit.NONE, filesCount);
        fileCountPerPartition.add(filesCount);

        HashMap<ColumnPath, ColumnQuickStats<?>> rolledUpColStats = new HashMap<>();
        try {
            // Wait for footer reads to finish
            CompletableFuture<Void> overallCompletableFuture = CompletableFuture.allOf(footerFetchCompletableFutures.toArray(new CompletableFuture[0]));
            overallCompletableFuture.get(footerFetchTimeoutMillis, MILLISECONDS);

            for (CompletableFuture<ParquetMetadata> future : footerFetchCompletableFutures) {
                ParquetMetadata parquetMetadata = future.get();
                processColumnMetadata(parquetMetadata, rolledUpColStats);
            }
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error(e, "Failed to read/build stats from parquet footer");
            throw new RuntimeException(e);
        }

        if (rolledUpColStats.isEmpty()) {
            return PartitionQuickStats.EMPTY;
        }
        return new PartitionQuickStats(partitionId, rolledUpColStats.values(), filesCount);
    }

    enum ColumnType
    {
        INTEGER,
        LONG,
        FLOAT,
        DOUBLE,
        SLICE,
        DATE,
        TIME,
        BOOLEAN
    }
}
