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
package com.facebook.presto.delta;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.StringRange;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import io.delta.kernel.utils.CloseableIterator;
import jakarta.inject.Inject;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.util.Objects.requireNonNull;

/**
 * Aggregates per-file {@link DeltaFileEntry} statistics into {@link TableStatistics} for the CBO.
 * Phase 1 only — NDV for regular columns requires Phase 2 (ANALYZE).
 *
 * Safety: any file missing stats/row-count → {@link TableStatistics#empty()}.
 * Missing null-count for a column → {@link Estimate#unknown()} for that column.
 * Zero-row table → explicit zero statistics.
 */
public class DeltaTableStatisticsProvider
        implements DeltaStatisticsProvider
{
    private static final Logger log = Logger.get(DeltaTableStatisticsProvider.class);
    private static final long WARN_INTERVAL_MS = 60_000;
    private static long lastWarnTime;

    @VisibleForTesting
    static final int MAX_PARTITION_NDV_THRESHOLD = 10_000;

    private static synchronized void logIncompleteStatsWarning(String tableName, String filePath)
    {
        long now = System.currentTimeMillis();
        if (now - lastWarnTime > WARN_INTERVAL_MS) {
            log.warn("Delta table '%s' statistics aggregation aborted: file '%s' has absent or malformed 'add.stats' JSON", tableName, filePath);
            lastWarnTime = now;
        }
    }

    @VisibleForTesting
    interface FileEntrySource
    {
        CloseableIterator<DeltaFileEntry> getFileEntries(
                ConnectorSession session,
                DeltaTable deltaTable,
                TupleDomain<DeltaColumnHandle> constraint,
                TypeManager typeManager);
    }

    private final FileEntrySource fileEntrySource;
    private final TypeManager typeManager;
    // A Delta snapshot is immutable, so statistics derived from it never go stale; the cache only bounds memory.
    private final Cache<StatsCacheKey, CachedStatistics> cache;

    @Inject
    public DeltaTableStatisticsProvider(DeltaClient deltaClient, TypeManager typeManager, DeltaConfig config)
    {
        this(requireNonNull(deltaClient, "deltaClient is null")::getFileEntries, typeManager, config);
    }

    @VisibleForTesting
    DeltaTableStatisticsProvider(FileEntrySource fileEntrySource, TypeManager typeManager, DeltaConfig config)
    {
        this.fileEntrySource = requireNonNull(fileEntrySource, "fileEntrySource is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        requireNonNull(config, "config is null");
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(config.getTableStatisticsCacheMaxEntries())
                .expireAfterAccess(config.getTableStatisticsCacheTtl().toMillis(), TimeUnit.MILLISECONDS)
                .build();
    }

    @VisibleForTesting
    DeltaTableStatisticsProvider(TypeManager typeManager)
    {
        this((session, table, constraint, types) -> {
            throw new UnsupportedOperationException("no file entry source");
        }, typeManager, new DeltaConfig());
    }

    /**
     * Statistics are best effort: any failure while reading the Delta log is logged and results in
     * {@link TableStatistics#empty()} rather than failing query planning. Failures are never cached.
     */
    @Override
    public TableStatistics getTableStatistics(
            ConnectorSession session,
            DeltaTableHandle tableHandle,
            List<ColumnHandle> columnHandles,
            Constraint<ColumnHandle> constraint)
    {
        DeltaTable deltaTable = tableHandle.getDeltaTable();
        try {
            TupleDomain<DeltaColumnHandle> transformedConstraint = constraint.getSummary()
                    .transform(DeltaColumnHandle.class::cast);
            if (!deltaTable.getSnapshotId().isPresent()) {
                // Without a pinned snapshot the result is not safe to cache or to compute
                return TableStatistics.empty();
            }
            // Both settings change how TIMESTAMP partition values are converted while pruning files, so
            // equal constraint values can select different files and must not share a cache entry
            boolean legacyTimestamp = session.getSqlFunctionProperties().isLegacyTimestamp();
            StatsCacheKey key = new StatsCacheKey(
                    deltaTable.getTableLocation(),
                    deltaTable.getSnapshotId().get(),
                    partitionDomainsOnly(transformedConstraint),
                    legacyTimestamp,
                    // the zone only affects partition pruning under legacy timestamp semantics
                    legacyTimestamp ? Optional.of(session.getSqlFunctionProperties().getTimeZoneKey()) : Optional.empty());
            // Concurrent callers for the same key share a single log scan
            CachedStatistics cached = cache.get(key, () -> loadStatistics(session, deltaTable, key.partitionConstraint));
            return cached.project(columnHandles);
        }
        catch (Exception e) {
            log.warn(e, "Failed to compute statistics for Delta table '%s.%s'; continuing without statistics",
                    deltaTable.getSchemaName(), deltaTable.getTableName());
            return TableStatistics.empty();
        }
    }

    private CachedStatistics loadStatistics(ConnectorSession session, DeltaTable deltaTable, TupleDomain<DeltaColumnHandle> partitionConstraint)
            throws IOException
    {
        // Always aggregate every column so one scan serves any later column subset requested for this key
        try (CloseableIterator<DeltaFileEntry> fileEntries = fileEntrySource.getFileEntries(session, deltaTable, partitionConstraint, typeManager)) {
            return new CachedStatistics(computeTableStatistics(fileEntries, deltaTable, null));
        }
    }

    // Only partition predicates prune files, so other domains must not fragment the cache
    private static TupleDomain<DeltaColumnHandle> partitionDomainsOnly(TupleDomain<DeltaColumnHandle> constraint)
    {
        if (constraint.isAll() || constraint.isNone()) {
            return constraint;
        }
        Map<DeltaColumnHandle, Domain> domains = new HashMap<>();
        constraint.getDomains().get().forEach((column, domain) -> {
            if (column.getColumnType() == PARTITION) {
                domains.put(column, domain);
            }
        });
        return TupleDomain.withColumnDomains(domains);
    }

    @VisibleForTesting
    long cacheSize()
    {
        return cache.size();
    }

    private static final class StatsCacheKey
    {
        private final String tableLocation;
        private final long snapshotId;
        private final TupleDomain<DeltaColumnHandle> partitionConstraint;
        private final boolean legacyTimestamp;
        private final Optional<TimeZoneKey> sessionTimeZone;

        StatsCacheKey(
                String tableLocation,
                long snapshotId,
                TupleDomain<DeltaColumnHandle> partitionConstraint,
                boolean legacyTimestamp,
                Optional<TimeZoneKey> sessionTimeZone)
        {
            this.tableLocation = requireNonNull(tableLocation, "tableLocation is null");
            this.snapshotId = snapshotId;
            this.partitionConstraint = requireNonNull(partitionConstraint, "partitionConstraint is null");
            this.legacyTimestamp = legacyTimestamp;
            this.sessionTimeZone = requireNonNull(sessionTimeZone, "sessionTimeZone is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StatsCacheKey that = (StatsCacheKey) o;
            return snapshotId == that.snapshotId
                    && tableLocation.equals(that.tableLocation)
                    && partitionConstraint.equals(that.partitionConstraint)
                    && legacyTimestamp == that.legacyTimestamp
                    && sessionTimeZone.equals(that.sessionTimeZone);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableLocation, snapshotId, partitionConstraint, legacyTimestamp, sessionTimeZone);
        }
    }

    /** Full-table statistics, re-keyable to whichever column handles the planner passes in. */
    private static final class CachedStatistics
    {
        private final TableStatistics statistics;
        private final Map<String, ColumnStatistics> byColumnName;

        CachedStatistics(TableStatistics statistics)
        {
            this.statistics = requireNonNull(statistics, "statistics is null");
            Map<String, ColumnStatistics> byName = new HashMap<>();
            statistics.getColumnStatistics().forEach((handle, columnStatistics) ->
                    byName.put(((DeltaColumnHandle) handle).getLogicalName(), columnStatistics));
            this.byColumnName = ImmutableMap.copyOf(byName);
        }

        TableStatistics project(List<ColumnHandle> requestedColumns)
        {
            if (statistics.getRowCount().isUnknown() && statistics.getColumnStatistics().isEmpty()) {
                return statistics;
            }
            TableStatistics.Builder builder = TableStatistics.builder()
                    .setRowCount(statistics.getRowCount())
                    .setTotalSize(statistics.getTotalSize());
            for (ColumnHandle handle : requestedColumns) {
                DeltaColumnHandle column = (DeltaColumnHandle) handle;
                if (column.getSubfield().isPresent()) {
                    continue;
                }
                ColumnStatistics columnStatistics = byColumnName.get(column.getLogicalName());
                if (columnStatistics != null) {
                    builder.setColumnStatistics(handle, columnStatistics);
                }
            }
            return builder.build();
        }
    }

    @VisibleForTesting
    TableStatistics computeTableStatistics(List<DeltaFileEntry> fileEntries, DeltaTable deltaTable)
    {
        return computeTableStatistics(fileEntries.iterator(), deltaTable, null);
    }

    @VisibleForTesting
    TableStatistics computeTableStatistics(
            List<DeltaFileEntry> fileEntries,
            DeltaTable deltaTable,
            List<ColumnHandle> requestedColumns)
    {
        return computeTableStatistics(fileEntries.iterator(), deltaTable, requestedColumns);
    }

    @VisibleForTesting
    TableStatistics computeTableStatistics(
            Iterator<DeltaFileEntry> fileEntries,
            DeltaTable deltaTable,
            List<ColumnHandle> requestedColumns)
    {
        List<ColumnStatsAccumulator> columns = resolveColumns(deltaTable, requestedColumns).stream()
                .map(column -> {
                    Type type = typeManager.getType(column.getDataType());
                    return column.getColumnType() == PARTITION
                            ? new PartitionColumnAccumulator(column, type)
                            : new RegularColumnAccumulator(column, type);
                })
                .collect(toImmutableList());

        long physicalRows = 0;
        long liveRows = 0;
        long totalBytes = 0;
        while (fileEntries.hasNext()) {
            DeltaFileEntry file = fileEntries.next();
            Optional<DeltaJsonFileStatistics> stats = file.getStats();
            Optional<Long> numRecords = stats.flatMap(DeltaFileStatistics::getNumRecords);
            if (!numRecords.isPresent()) {
                logIncompleteStatsWarning(deltaTable.getSchemaName() + "." + deltaTable.getTableName(), file.getPath());
                return TableStatistics.empty();
            }
            long fileRows = numRecords.get();
            physicalRows += fileRows;
            liveRows += fileRows - file.getDeletedRowCount();
            totalBytes += file.getSize();
            for (ColumnStatsAccumulator column : columns) {
                column.add(file, stats.get(), fileRows);
            }
        }

        if (liveRows <= 0) {
            return createZeroStatistics(columns);
        }

        TableStatistics.Builder builder = TableStatistics.builder()
                .setRowCount(Estimate.of(liveRows))
                .setTotalSize(Estimate.of(totalBytes));
        for (ColumnStatsAccumulator column : columns) {
            // null counts in add.stats are over physical rows, so normalize by physical rows
            builder.setColumnStatistics(column.handle, column.build(physicalRows));
        }
        return builder.build();
    }

    private static List<DeltaColumnHandle> resolveColumns(DeltaTable deltaTable, List<ColumnHandle> requestedColumns)
    {
        if (requestedColumns == null) {
            return buildColumnHandles(deltaTable.getColumns());
        }
        // Key the result by the planner's own handles; rebuilding and matching via equals() is fragile
        return requestedColumns.stream()
                .map(DeltaColumnHandle.class::cast)
                .filter(column -> !column.getSubfield().isPresent())
                .collect(toImmutableList());
    }

    private static List<DeltaColumnHandle> buildColumnHandles(List<DeltaColumn> columns)
    {
        return columns.stream()
                .map(col -> new DeltaColumnHandle(
                        col.getId(),
                        col.getPhysicalName(),
                        col.getLogicalName(),
                        col.getType(),
                        col.isPartition() ? PARTITION : REGULAR,
                        Optional.empty()))
                .collect(toImmutableList());
    }

    private static TableStatistics createZeroStatistics(List<ColumnStatsAccumulator> columns)
    {
        TableStatistics.Builder builder = TableStatistics.builder()
                .setRowCount(Estimate.zero())
                .setTotalSize(Estimate.zero());
        for (ColumnStatsAccumulator column : columns) {
            builder.setColumnStatistics(column.handle, ColumnStatistics.builder()
                    .setNullsFraction(Estimate.zero())
                    .setDistinctValuesCount(Estimate.zero())
                    .build());
        }
        return builder.build();
    }

    private enum RangeKind
    {
        DOUBLE, STRING, NONE;

        static RangeKind of(Type type)
        {
            if (type instanceof DecimalType || type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT)
                    || type.equals(TINYINT) || type.equals(DoubleType.DOUBLE) || type.equals(REAL) || type.equals(DATE)) {
                return DOUBLE;
            }
            if (type instanceof VarcharType) {
                return STRING;
            }
            // TIMESTAMP[ WITH TIME ZONE]: StatsUtil.toStatsRepresentation cannot map literals of these types,
            // so the CBO would never consult the range. BOOLEAN/VARBINARY/complex: no useful range.
            return NONE;
        }
    }

    private abstract static class ColumnStatsAccumulator
    {
        final DeltaColumnHandle handle;
        final Type type;
        final RangeKind rangeKind;
        final Bounds<Double> doubleRange = new Bounds<>();
        final Bounds<String> stringRange = new Bounds<>();
        long nullCount;
        boolean nullCountKnown = true;

        ColumnStatsAccumulator(DeltaColumnHandle handle, Type type)
        {
            this.handle = requireNonNull(handle, "handle is null");
            this.type = requireNonNull(type, "type is null");
            this.rangeKind = RangeKind.of(type);
        }

        abstract void add(DeltaFileEntry file, DeltaFileStatistics stats, long fileRows);

        Estimate distinctValuesCount()
        {
            return Estimate.unknown();
        }

        ColumnStatistics build(long physicalRows)
        {
            ColumnStatistics.Builder builder = ColumnStatistics.builder()
                    .setNullsFraction(nullCountKnown ? Estimate.of((double) nullCount / physicalRows) : Estimate.unknown())
                    .setDistinctValuesCount(distinctValuesCount());
            if (rangeKind == RangeKind.DOUBLE && doubleRange.hasAnyBound()) {
                builder.setRange(new DoubleRange(
                        doubleRange.min().orElse(NEGATIVE_INFINITY),
                        doubleRange.max().orElse(POSITIVE_INFINITY)));
            }
            else if (rangeKind == RangeKind.STRING && stringRange.hasBothBounds()) {
                builder.setStringRange(new StringRange(stringRange.min().get(), stringRange.max().get()));
            }
            return builder.build();
        }
    }

    private static final class RegularColumnAccumulator
            extends ColumnStatsAccumulator
    {
        private final String statsName;

        RegularColumnAccumulator(DeltaColumnHandle handle, Type type)
        {
            super(handle, type);
            // column-mapping tables key add.stats by physical name
            this.statsName = firstNonNull(handle.getPhysicalName(), handle.getLogicalName());
        }

        @Override
        void add(DeltaFileEntry file, DeltaFileStatistics stats, long fileRows)
        {
            Optional<Long> fileNulls = stats.getNullCount(statsName);
            if (fileNulls.isPresent()) {
                nullCount += fileNulls.get();
            }
            else {
                nullCountKnown = false;
            }

            // An empty or all-null file legitimately has no min/max and must not invalidate the bounds
            if (fileRows == 0 || (fileNulls.isPresent() && fileNulls.get() >= fileRows)) {
                return;
            }
            Optional<Object> min = stats.getMinColumnValue(handle);
            Optional<Object> max = stats.getMaxColumnValue(handle);
            switch (rangeKind) {
                case DOUBLE:
                    doubleRange.add(min.flatMap(value -> toStatsDouble(type, value)), max.flatMap(value -> toStatsDouble(type, value)));
                    break;
                case STRING:
                    // Delta truncates string stats; a truncated min/max is still a valid bound
                    stringRange.add(min.map(Object::toString), max.map(Object::toString));
                    break;
                case NONE:
                    break;
            }
        }
    }

    private static final class PartitionColumnAccumulator
            extends ColumnStatsAccumulator
    {
        private Set<String> distinctValues = new HashSet<>(); // null once the cap is exceeded

        PartitionColumnAccumulator(DeltaColumnHandle handle, Type type)
        {
            super(handle, type);
        }

        @Override
        void add(DeltaFileEntry file, DeltaFileStatistics stats, long fileRows)
        {
            if (fileRows == 0) {
                return;
            }
            String value = getPartitionValue(file.getPartitionValues(), handle);
            // Delta serializes a NULL partition value as an absent key or an empty string
            if (isNullOrEmpty(value)) {
                nullCount += fileRows;
                return;
            }
            if (distinctValues != null && distinctValues.add(value) && distinctValues.size() > MAX_PARTITION_NDV_THRESHOLD) {
                distinctValues = null;
            }
            switch (rangeKind) {
                case DOUBLE:
                    Optional<Double> parsed = toStatsDouble(type, value);
                    doubleRange.add(parsed, parsed);
                    break;
                case STRING:
                    stringRange.add(Optional.of(value), Optional.of(value));
                    break;
                case NONE:
                    break;
            }
        }

        @Override
        Estimate distinctValuesCount()
        {
            return distinctValues == null ? Estimate.unknown() : Estimate.of(distinctValues.size());
        }
    }

    /** Running min/max where each bound becomes permanently unknown once any contributing file lacks it. */
    private static final class Bounds<T extends Comparable<? super T>>
    {
        private T min;
        private T max;
        private boolean minValid = true;
        private boolean maxValid = true;

        void add(Optional<T> fileMin, Optional<T> fileMax)
        {
            if (minValid) {
                if (fileMin.isPresent()) {
                    min = (min == null || fileMin.get().compareTo(min) < 0) ? fileMin.get() : min;
                }
                else {
                    minValid = false;
                    min = null;
                }
            }
            if (maxValid) {
                if (fileMax.isPresent()) {
                    max = (max == null || fileMax.get().compareTo(max) > 0) ? fileMax.get() : max;
                }
                else {
                    maxValid = false;
                    max = null;
                }
            }
        }

        Optional<T> min()
        {
            return Optional.ofNullable(min);
        }

        Optional<T> max()
        {
            return Optional.ofNullable(max);
        }

        boolean hasAnyBound()
        {
            return (min != null || max != null) && (min == null || max == null || min.compareTo(max) <= 0);
        }

        boolean hasBothBounds()
        {
            return min != null && max != null && min.compareTo(max) <= 0;
        }
    }

    /**
     * Converts a stats JSON value or a partition-value string into Presto's double stats representation
     * (the same one StatsUtil.toStatsRepresentation uses): numeric value for numbers/decimals, epoch day for DATE.
     */
    @VisibleForTesting
    static Optional<Double> toStatsDouble(Type type, Object value)
    {
        if (value == null) {
            return Optional.empty();
        }
        try {
            if (type.equals(DATE)) {
                return Optional.of(value instanceof Number
                        ? ((Number) value).doubleValue()
                        : (double) LocalDate.parse(value.toString()).toEpochDay());
            }
            double result = value instanceof Number ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
            return Double.isNaN(result) ? Optional.empty() : Optional.of(result);
        }
        catch (NumberFormatException | DateTimeParseException e) {
            return Optional.empty();
        }
    }

    private static String getPartitionValue(Map<String, String> partitionValues, DeltaColumnHandle column)
    {
        if (partitionValues == null) {
            return null;
        }
        // With column mapping, Delta keys partitionValues by physical name
        for (String name : Arrays.asList(column.getPhysicalName(), column.getLogicalName())) {
            if (name != null && partitionValues.containsKey(name)) {
                return partitionValues.get(name);
            }
        }
        return partitionValues.entrySet().stream()
                .filter(entry -> (column.getPhysicalName() != null && entry.getKey().equalsIgnoreCase(column.getPhysicalName()))
                        || (column.getLogicalName() != null && entry.getKey().equalsIgnoreCase(column.getLogicalName())))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
}
