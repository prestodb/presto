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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.StringRange;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.delta.kernel.utils.CloseableIterator;
import jakarta.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
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

    private final DeltaClient deltaClient;
    private final TypeManager typeManager;

    @Inject
    public DeltaTableStatisticsProvider(DeltaClient deltaClient, TypeManager typeManager)
    {
        this.deltaClient = deltaClient;
        this.typeManager = typeManager;
    }

    @Override
    public TableStatistics getTableStatistics(
            ConnectorSession session,
            DeltaTableHandle tableHandle,
            List<ColumnHandle> columnHandles,
            Constraint<ColumnHandle> constraint)
    {
        requireNonNull(deltaClient, "deltaClient is null — DeltaTableStatisticsProvider was not injected correctly");
        TupleDomain<DeltaColumnHandle> transformedConstraint = constraint.getSummary()
                .transform(DeltaColumnHandle.class::cast);
        try (CloseableIterator<DeltaFileEntry> fileEntriesIter = deltaClient.getFileEntries(
                session,
                tableHandle.getDeltaTable(),
                transformedConstraint,
                typeManager)) {
            return computeTableStatistics(fileEntriesIter, tableHandle.getDeltaTable(), columnHandles);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Could not close scan file iterator", e);
        }
    }

    @VisibleForTesting
    TableStatistics computeTableStatistics(List<DeltaFileEntry> fileEntries, DeltaTable deltaTable)
    {
        return computeTableStatistics(fileEntries.iterator(), deltaTable, null);
    }

    @VisibleForTesting
    TableStatistics computeTableStatistics(
            Iterator<DeltaFileEntry> fileEntries,
            DeltaTable deltaTable,
            List<ColumnHandle> requestedColumnHandles)
    {
        List<DeltaColumnHandle> columnHandles = buildColumnHandles(deltaTable.getColumns());
        if (requestedColumnHandles != null) {
            Set<ColumnHandle> requestedSet = ImmutableSet.copyOf(requestedColumnHandles);
            columnHandles = columnHandles.stream()
                    .filter(requestedSet::contains)
                    .collect(Collectors.toList());
        }

        double totalNumRecords = 0;

        // NaN = unknown null count (at least one file lacked it for this column)
        Map<DeltaColumnHandle, Double> nullCounts = new HashMap<>();
        columnHandles.forEach(col -> nullCounts.put(col, 0.0));

        Map<DeltaColumnHandle, Double> minValues = new HashMap<>();
        Map<DeltaColumnHandle, Double> maxValues = new HashMap<>();
        Map<DeltaColumnHandle, Boolean> minReliable = new HashMap<>();
        Map<DeltaColumnHandle, Boolean> maxReliable = new HashMap<>();
        columnHandles.forEach(col -> {
            minReliable.put(col, true);
            maxReliable.put(col, true);
        });

        // String ranges for varchar
        Map<DeltaColumnHandle, String> minStringValues = new HashMap<>();
        Map<DeltaColumnHandle, String> maxStringValues = new HashMap<>();
        Map<DeltaColumnHandle, Boolean> stringMinReliable = new HashMap<>();
        Map<DeltaColumnHandle, Boolean> stringMaxReliable = new HashMap<>();
        columnHandles.forEach(col -> {
            stringMinReliable.put(col, true);
            stringMaxReliable.put(col, true);
        });

        // Distinct partition values per partition column for exact NDV
        Map<DeltaColumnHandle, Set<String>> partitionDistinctValues = new HashMap<>();
        Map<DeltaColumnHandle, Boolean> partitionNdvExceeded = new HashMap<>();
        columnHandles.stream()
                .filter(col -> col.getColumnType() == PARTITION)
                .forEach(col -> {
                    partitionDistinctValues.put(col, new HashSet<>());
                    partitionNdvExceeded.put(col, false);
                });

        boolean hasFiles = false;
        while (fileEntries.hasNext()) {
            DeltaFileEntry entry = fileEntries.next();
            hasFiles = true;

            // Any file without stats → cannot produce a reliable aggregate
            if (!entry.getStats().isPresent()) {
                logIncompleteStatsWarning(deltaTable.getSchemaName() + "." + deltaTable.getTableName(), entry.getPath());
                return TableStatistics.empty();
            }
            DeltaFileStatistics stats = entry.getStats().get();

            // Any file without row count → cannot produce a reliable aggregate
            if (!stats.getNumRecords().isPresent()) {
                logIncompleteStatsWarning(deltaTable.getSchemaName() + "." + deltaTable.getTableName(), entry.getPath());
                return TableStatistics.empty();
            }

            long fileNumRecords = stats.getNumRecords().get();
            totalNumRecords += fileNumRecords;

            for (DeltaColumnHandle column : columnHandles) {
                if (column.getColumnType() == PARTITION) {
                    // Case-insensitive lookup — partition map uses original case, column name may be lowercased
                    String partitionValue = getPartitionValue(entry.getPartitionValues(), column.getLogicalName());
                    if (partitionValue == null) {
                        nullCounts.merge(column, (double) fileNumRecords, Double::sum);
                    }
                    else if (!partitionNdvExceeded.get(column)) {
                        Set<String> distincts = partitionDistinctValues.get(column);
                        distincts.add(partitionValue);
                        if (distincts.size() > MAX_PARTITION_NDV_THRESHOLD) {
                            partitionNdvExceeded.put(column, true);
                            distincts.clear(); // Free up memory immediately
                        }
                    }
                }
                else {
                    // Use physical name for stats lookup (column-mapping tables store stats by physical name)
                    String physicalName = column.getPhysicalName() != null
                            ? column.getPhysicalName()
                            : column.getLogicalName();

                    Optional<Long> maybeNullCount = stats.getNullCount(physicalName);
                    if (maybeNullCount.isPresent()) {
                        if (!Double.isNaN(nullCounts.get(column))) {
                            nullCounts.put(column, nullCounts.get(column) + maybeNullCount.get());
                        }
                    }
                    else {
                        nullCounts.put(column, NaN); // mark unknown for this column
                    }

                    boolean hasNonNullValues = fileNumRecords > maybeNullCount.orElse(0L);

                    // Track Double Ranges (min)
                    Optional<Object> minValOpt = stats.getMinColumnValue(column);
                    OptionalDouble minDoubleOpt = minValOpt.isPresent()
                            ? toDoubleStatistic(minValOpt.get(), column.getDataType())
                            : OptionalDouble.empty();
                    if (minDoubleOpt.isPresent()) {
                        minValues.merge(column, minDoubleOpt.getAsDouble(), Math::min);
                    }
                    else if (hasNonNullValues) {
                        minReliable.put(column, false);
                    }

                    // Track Double Ranges (max)
                    Optional<Object> maxValOpt = stats.getMaxColumnValue(column);
                    OptionalDouble maxDoubleOpt = maxValOpt.isPresent()
                            ? toDoubleStatistic(maxValOpt.get(), column.getDataType())
                            : OptionalDouble.empty();
                    if (maxDoubleOpt.isPresent()) {
                        maxValues.merge(column, maxDoubleOpt.getAsDouble(), Math::max);
                    }
                    else if (hasNonNullValues) {
                        maxReliable.put(column, false);
                    }

                    // Track String Ranges (varchar)
                    boolean isVarchar = column.getDataType().getBase().equalsIgnoreCase("varchar");
                    if (isVarchar) {
                        if (minValOpt.isPresent()) {
                            String valueStr = minValOpt.get().toString();
                            minStringValues.merge(column, valueStr, (s1, s2) -> s1.compareTo(s2) < 0 ? s1 : s2);
                        }
                        else if (hasNonNullValues) {
                            stringMinReliable.put(column, false);
                        }

                        if (maxValOpt.isPresent()) {
                            String valueStr = maxValOpt.get().toString();
                            maxStringValues.merge(column, valueStr, (s1, s2) -> s1.compareTo(s2) > 0 ? s1 : s2);
                        }
                        else if (hasNonNullValues) {
                            stringMaxReliable.put(column, false);
                        }
                    }
                }
            }
        }

        if (!hasFiles || totalNumRecords == 0) {
            return createZeroStatistics(columnHandles);
        }

        final double finalNumRecords = totalNumRecords;
        TableStatistics.Builder statsBuilder = TableStatistics.builder()
                .setRowCount(Estimate.of(finalNumRecords));

        for (DeltaColumnHandle column : columnHandles) {
            ColumnStatistics.Builder colBuilder = ColumnStatistics.builder();

            Double nullCount = nullCounts.get(column);
            colBuilder.setNullsFraction(
                    Double.isNaN(nullCount)
                            ? Estimate.unknown()
                            : Estimate.of(nullCount / finalNumRecords));

            Double minValue = minValues.get(column);
            Double maxValue = maxValues.get(column);
            boolean isMinReliable = minReliable.getOrDefault(column, false) && minValue != null;
            boolean isMaxReliable = maxReliable.getOrDefault(column, false) && maxValue != null;

            if (isMinReliable && isMaxReliable) {
                colBuilder.setRange(new DoubleRange(minValue, maxValue));
            }
            else if (isMaxReliable) {
                colBuilder.setRange(new DoubleRange(NEGATIVE_INFINITY, maxValue));
            }
            else if (isMinReliable) {
                colBuilder.setRange(new DoubleRange(minValue, POSITIVE_INFINITY));
            }

            // String range estimation
            boolean isVarchar = column.getDataType().getBase().equalsIgnoreCase("varchar");
            if (isVarchar) {
                String minStr = minStringValues.get(column);
                String maxStr = maxStringValues.get(column);
                boolean isMinStrReliable = stringMinReliable.getOrDefault(column, false) && minStr != null;
                boolean isMaxStrReliable = stringMaxReliable.getOrDefault(column, false) && maxStr != null;
                if (isMinStrReliable && isMaxStrReliable && minStr.compareTo(maxStr) <= 0) {
                    colBuilder.setStringRange(new StringRange(minStr, maxStr));
                }
            }

            if (column.getColumnType() == PARTITION) {
                // Exact NDV from observed distinct partition values
                if (partitionNdvExceeded.get(column)) {
                    colBuilder.setDistinctValuesCount(Estimate.unknown());
                }
                else {
                    colBuilder.setDistinctValuesCount(
                            Estimate.of(partitionDistinctValues.get(column).size()));
                }
            }
            statsBuilder.setColumnStatistics(column, colBuilder.build());
        }

        return statsBuilder.build();
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
                .collect(Collectors.toList());
    }

    private static TableStatistics createZeroStatistics(List<DeltaColumnHandle> columnHandles)
    {
        TableStatistics.Builder statsBuilder = TableStatistics.builder()
                .setRowCount(Estimate.of(0));
        for (DeltaColumnHandle column : columnHandles) {
            ColumnStatistics.Builder colBuilder = ColumnStatistics.builder();
            colBuilder.setNullsFraction(Estimate.of(0));
            colBuilder.setDistinctValuesCount(Estimate.of(0));
            statsBuilder.setColumnStatistics(column, colBuilder.build());
        }
        return statsBuilder.build();
    }

    private static boolean isValidInRange(Double d)
    {
        return d != null && !Double.isNaN(d);
    }

    /**
     * Looks up a partition value case-insensitively. Returns {@code null} if absent
     * (absent key = NULL partition value in Delta).
     */
    private static String getPartitionValue(Map<String, String> partitionValues, String columnLogicalName)
    {
        if (partitionValues == null || columnLogicalName == null) {
            return null;
        }
        if (partitionValues.containsKey(columnLogicalName)) {
            return partitionValues.get(columnLogicalName);
        }
        String lowerKey = columnLogicalName.toLowerCase(Locale.ENGLISH);
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            if (entry.getKey().toLowerCase(Locale.ENGLISH).equals(lowerKey)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /** Converts a JSON stat value to double for range statistics. Returns empty for non-numeric types. */
    @VisibleForTesting
    static OptionalDouble toDoubleStatistic(Object value, TypeSignature type)
    {
        if (value == null) {
            return OptionalDouble.empty();
        }
        String baseType = type.getBase().toLowerCase(Locale.ENGLISH);
        switch (baseType) {
            case "bigint":
            case "integer":
            case "int":
            case "smallint":
            case "tinyint":
                if (value instanceof Number) {
                    return OptionalDouble.of(((Number) value).doubleValue());
                }
                return OptionalDouble.empty();
            case "double":
                if (value instanceof Number) {
                    return OptionalDouble.of(((Number) value).doubleValue());
                }
                return OptionalDouble.empty();
            case "real":
            case "float":
                // doubleValue() avoids float→double widening noise (e.g. 1.1f → 1.100000023841858)
                if (value instanceof Number) {
                    return OptionalDouble.of(((Number) value).doubleValue());
                }
                return OptionalDouble.empty();
            case "decimal":
                if (value instanceof Number) {
                    return OptionalDouble.of(((Number) value).doubleValue());
                }
                // Delta may encode decimal as a string (e.g. "123.45")
                if (value instanceof String) {
                    try {
                        return OptionalDouble.of(Double.parseDouble((String) value));
                    }
                    catch (NumberFormatException e) {
                        return OptionalDouble.empty();
                    }
                }
                return OptionalDouble.empty();
            case "date":
                // Delta stores dates as epoch-day integers, but some tools store them as ISO-8601 string dates
                if (value instanceof Number) {
                    return OptionalDouble.of(((Number) value).doubleValue());
                }
                if (value instanceof String) {
                    try {
                        LocalDate date = LocalDate.parse((String) value);
                        return OptionalDouble.of(date.toEpochDay());
                    }
                    catch (Exception e) {
                        return OptionalDouble.empty();
                    }
                }
                return OptionalDouble.empty();
            case "timestamp":
            case "timestamp with time zone":
            case "timestamp_ntz":
                if (value instanceof Number) {
                    return OptionalDouble.of(((Number) value).doubleValue());
                }
                if (value instanceof String) {
                    try {
                        String str = ((String) value).trim().replace(' ', 'T');
                        Instant instant;
                        if (str.endsWith("Z") || str.endsWith("z") || str.contains("+") || (str.lastIndexOf('-') > 10)) {
                            try {
                                instant = Instant.parse(str);
                            }
                            catch (DateTimeParseException e) {
                                instant = OffsetDateTime.parse(str).toInstant();
                            }
                        }
                        else {
                            instant = LocalDateTime.parse(str).toInstant(ZoneOffset.UTC);
                        }
                        long epochMicros = instant.getEpochSecond() * 1_000_000L + (instant.getNano() / 1_000L);
                        return OptionalDouble.of((double) epochMicros);
                    }
                    catch (Exception e) {
                        return OptionalDouble.empty();
                    }
                }
                return OptionalDouble.empty();
            default:
                return OptionalDouble.empty(); // varchar, boolean etc. — no range stats
        }
    }
}
