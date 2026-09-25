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
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;
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
import static com.facebook.presto.hive.HiveSessionProperties.isQuickStatsProvableEmptyEnabled;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.PARQUET_SERDE_CLASS_NAMES;
import static com.facebook.presto.hive.parquet.ParquetPageSourceFactory.createDecryptor;
import static com.facebook.presto.parquet.cache.MetadataReader.readFooter;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isTransactionalTable;

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

    // NDV (distinct-values-count) note: Parquet column-chunk statistics carry no distinct count
    // (no getNumDistinct()), so we deliberately do not attempt any distinct-value estimation here
    // (e.g. HLL) -- see ColumnQuickStats#getDistinctValuesCount() for the conservative NDV bound
    // that is derived, once, from the min/max/rowCount/nullsCount rolled up below across all row
    // groups and files for a given column+partition (NDV is not additive across row groups/files,
    // so it must be derived from the final merged state rather than accumulated incrementally).
    /**
     * @return the number of row groups seen in this footer. A file with zero row groups holds zero
     * rows, which is what lets {@link #buildQuickStats} tell provable emptiness apart from "the
     * schema had nothing we could use" (e.g. every column nested, skipped below).
     */
    private static long processColumnMetadata(ParquetMetadata parquetMetadata, Map<ColumnPath, ColumnQuickStats<?>> rolledUpColStats)
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
        return rowGroups.size();
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
            if (!isQuickStatsProvableEmptyEnabled(session)) {
                // Legacy behavior: report "unknown". Returning here also avoids the metadata lookups
                // below, which the pre-fix code never performed for a partition with no files.
                return PartitionQuickStats.EMPTY;
            }
            // No files means no rows. That is a proof, not an estimate -- but only claim it for
            // formats where a bare directory listing tells the whole story.
            //
            // Everything below must degrade to EMPTY rather than throw: pre-fix this branch returned
            // without touching the metastore, so any new exception here would turn a previously silent,
            // cached UNKNOWN into error-log noise plus an uncached result retried on every query.
            if (canProveNoFilesMeansNoRows(metastore, table, metastoreContext, partitionId)) {
                return PartitionQuickStats.PROVABLY_EMPTY;
            }
            return PartitionQuickStats.EMPTY;
        }

        // TODO: Consider refactoring storage and/or table format to the interface when we implement an ORC/Iceberg quick stats builder
        StorageFormat storageFormat = resolveStorageFormat(metastore, table, metastoreContext, partitionId);

        if (!isParquetSerDe(storageFormat)) {
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
        int footersRead = 0;
        long rowGroupsSeen = 0;
        try {
            // Wait for footer reads to finish
            CompletableFuture<Void> overallCompletableFuture = CompletableFuture.allOf(footerFetchCompletableFutures.toArray(new CompletableFuture[0]));
            overallCompletableFuture.get(footerFetchTimeoutMillis, MILLISECONDS);

            for (CompletableFuture<ParquetMetadata> future : footerFetchCompletableFutures) {
                ParquetMetadata parquetMetadata = future.get();
                footersRead++;
                rowGroupsSeen += processColumnMetadata(parquetMetadata, rolledUpColStats);
            }
        }
        catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error(e, "Failed to read/build stats from parquet footer");
            throw new RuntimeException(e);
        }

        if (rolledUpColStats.isEmpty()) {
            // Every footer read cleanly and reported no row groups: the files exist but hold no rows,
            // which is provable emptiness. An empty roll-up with row groups present means something
            // else (e.g. every column is nested and therefore skipped above), so stay UNKNOWN.
            // footersRead > 0 cannot actually be false here (files.hasNext() was true above and any
            // unresolved future would have thrown), and is kept only so the proof does not depend on
            // that reasoning holding after future edits.
            if (isQuickStatsProvableEmptyEnabled(session)
                    && footersRead > 0
                    && rowGroupsSeen == 0
                    && canProveEmptiness(metastore, table, metastoreContext, storageFormat)) {
                return PartitionQuickStats.PROVABLY_EMPTY;
            }
            return PartitionQuickStats.EMPTY;
        }
        return new PartitionQuickStats(partitionId, rolledUpColStats.values(), filesCount);
    }

    private static StorageFormat resolveStorageFormat(ExtendedHiveMetastore metastore, SchemaTableName table, MetastoreContext metastoreContext, String partitionId)
    {
        if (UNPARTITIONED_ID.getPartitionName().equals(partitionId)) {
            Table resolvedTable = metastore.getTable(metastoreContext, table.getSchemaName(), table.getTableName()).get();
            return resolvedTable.getStorage().getStorageFormat();
        }
        Partition partition = metastore.getPartitionsByNames(metastoreContext, table.getSchemaName(), table.getTableName(),
                ImmutableList.of(new PartitionNameWithVersion(partitionId, Optional.empty()))).get(partitionId).get();
        return partition.getStorage().getStorageFormat();
    }

    /**
     * The non-throwing form of {@link #resolveStorageFormat}, for the zero-files path: absent rather
     * than {@code NoSuchElementException} when the table or the partition has been dropped from under
     * us. Proving nothing is always a safe answer.
     */
    private static Optional<StorageFormat> findStorageFormat(ExtendedHiveMetastore metastore, SchemaTableName table, MetastoreContext metastoreContext, String partitionId)
    {
        if (UNPARTITIONED_ID.getPartitionName().equals(partitionId)) {
            return resolveTable(metastore, table, metastoreContext)
                    .map(resolvedTable -> resolvedTable.getStorage().getStorageFormat());
        }
        // Same degrade-rather-than-throw requirement as resolveTable: this runs on the no-files path,
        // which previously never contacted the metastore.
        try {
            return Optional.ofNullable(metastore.getPartitionsByNames(metastoreContext, table.getSchemaName(), table.getTableName(),
                            ImmutableList.of(new PartitionNameWithVersion(partitionId, Optional.empty())))
                            .get(partitionId))
                    .flatMap(partition -> partition)
                    .map(partition -> partition.getStorage().getStorageFormat());
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /**
     * {@link StorageFormat#getSerDe()} throws {@code HIVE_INVALID_METADATA} for a null serde, which is
     * reachable for view-like metadata; the emptiness proof must degrade to UNKNOWN there rather than
     * fail a statistics call that used to succeed.
     */
    private static boolean isParquetSerDe(StorageFormat storageFormat)
    {
        String serDe = storageFormat.getSerDeNullable();
        return serDe != null && PARQUET_SERDE_CLASS_NAMES.contains(serDe);
    }

    /**
     * "No files under the location" (or "files, but no row groups") only implies "no rows" for
     * formats where the data files are the whole story. Refuse to claim provable emptiness otherwise:
     * <ul>
     * <li>Transactional/ACID tables keep their data in nested {@code base_*}/{@code delta_*}
     * directories, and the default nested-directory policy is IGNORED
     * ({@code QuickStatsProvider#buildQuickStats}), so a non-empty ACID table can list as zero
     * files.</li>
     * <li>Hudi tables are read through a Hoodie input format whose visible file set is derived from
     * a commit timeline, not from a plain listing.</li>
     * </ul>
     * Two formats need no gate here, but only because something upstream already excludes them, so
     * both are worth naming:
     * <ul>
     * <li><b>Symlink tables</b> -- {@code QuickStatsProvider} replaces the manifest listing with the
     * resolved target files before any builder runs, so the iterator we see is the real data-file set.
     * Zero files then genuinely means no manifests and no targets.</li>
     * <li><b>Iceberg / Delta</b> -- unreachable through this connector: {@code HiveMetadata}
     * ({@code getTableMetadata}) throws {@code UnknownTableTypeException} during analysis. That is the
     * only thing protecting us, and it matters: an Iceberg table registered in HMS keeps its data
     * under {@code data/}, which the default IGNORED nested-directory policy skips, so it would list
     * as zero files. If that refusal ever moves, add an explicit gate here.</li>
     * </ul>
     * Ordinary nested-directory layouts are safe for a different and stronger reason: statistics and
     * split generation use the same policy ({@code QuickStatsProvider} and
     * {@code StoragePartitionLoader} both derive it from {@code recursiveDirWalkerEnabled}), so a zero
     * estimate matches what the scan will actually read.
     */
    private static boolean canProveEmptiness(ExtendedHiveMetastore metastore, SchemaTableName table, MetastoreContext metastoreContext, StorageFormat storageFormat)
    {
        if (isHudiFormat(storageFormat)) {
            return false;
        }
        // ACID-ness is a table-level property, so this needs the table even when the storage format
        // above came from a partition.
        Optional<Table> resolvedTable = resolveTable(metastore, table, metastoreContext);
        if (!resolvedTable.isPresent()) {
            // The table vanished from under us, or the metastore could not be reached; prove nothing.
            return false;
        }
        return canProveEmptiness(storageFormat, resolvedTable.get().getParameters());
    }

    /**
     * Pure predicate: given a storage format and the table-level parameters, may "no data files" be
     * treated as "no rows"? Split out from the metastore lookup so the decision is testable without a
     * metastore and so a caller that already holds the {@link Table} does not pay a second round-trip.
     */
    private static boolean canProveEmptiness(StorageFormat storageFormat, Map<String, String> tableParameters)
    {
        return !isHudiFormat(storageFormat) && !isTransactionalTable(tableParameters);
    }

    /**
     * Resolves the emptiness proof for a partition with no data files, using a single metastore
     * round-trip for an unpartitioned table: {@code getTable} yields both the storage format and the
     * table-level transactional flag, so there is no reason to fetch it twice.
     * <p>
     * A partitioned table needs two lookups, because the storage format is a partition-level property
     * while ACID-ness is table-level. That second lookup is per empty partition rather than per table;
     * threading an already-resolved table down from {@code QuickStatsProvider} would make it O(1) per
     * planning event, which is worth doing separately from this change.
     */
    private static boolean canProveNoFilesMeansNoRows(ExtendedHiveMetastore metastore, SchemaTableName table, MetastoreContext metastoreContext, String partitionId)
    {
        if (UNPARTITIONED_ID.getPartitionName().equals(partitionId)) {
            Optional<Table> resolvedTable = resolveTable(metastore, table, metastoreContext);
            if (!resolvedTable.isPresent()) {
                return false;
            }
            StorageFormat storageFormat = resolvedTable.get().getStorage().getStorageFormat();
            return isParquetSerDe(storageFormat) && canProveEmptiness(storageFormat, resolvedTable.get().getParameters());
        }

        Optional<StorageFormat> partitionFormat = findStorageFormat(metastore, table, metastoreContext, partitionId);
        return partitionFormat.isPresent()
                && isParquetSerDe(partitionFormat.get())
                && canProveEmptiness(metastore, table, metastoreContext, partitionFormat.get());
    }

    /**
     * A metastore lookup on the emptiness-proof path must degrade to "cannot prove" rather than throw.
     * Before this change the no-files branch returned without touching the metastore, so letting an
     * exception escape here would turn a previously silent, cached UNKNOWN into a failed statistics
     * call -- and, because the failure is not cached, one retried on every query.
     */
    private static Optional<Table> resolveTable(ExtendedHiveMetastore metastore, SchemaTableName table, MetastoreContext metastoreContext)
    {
        try {
            return metastore.getTable(metastoreContext, table.getSchemaName(), table.getTableName());
        }
        catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    /**
     * Mirrors {@code HiveSplitManager#isHudiFormat}: a Parquet serde combined with a Hoodie input
     * format.
     */
    private static boolean isHudiFormat(StorageFormat storageFormat)
    {
        String serDe = storageFormat.getSerDeNullable();
        String inputFormat = storageFormat.getInputFormatNullable();
        return serDe != null && serDe.equals(ParquetHiveSerDe.class.getName())
                && inputFormat != null
                && (inputFormat.equals(HoodieParquetInputFormat.class.getName())
                || inputFormat.equals(HoodieParquetRealtimeInputFormat.class.getName()));
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
