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

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import jakarta.inject.Inject;

import java.util.HashMap;
import java.util.HashSet;
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
    private final DeltaClient deltaClient;

    @Inject
    public DeltaTableStatisticsProvider(DeltaClient deltaClient)
    {
        this.deltaClient = deltaClient;
    }

    @Override
    public TableStatistics getTableStatistics(
            ConnectorSession session,
            DeltaTableHandle tableHandle,
            List<ColumnHandle> columnHandles,
            Constraint<ColumnHandle> constraint)
    {
        requireNonNull(deltaClient, "deltaClient is null — DeltaTableStatisticsProvider was not injected correctly");
        List<DeltaFileEntry> fileEntries = deltaClient.listFileEntries(session, tableHandle.getDeltaTable());
        return computeTableStatistics(fileEntries, tableHandle.getDeltaTable(), columnHandles);
    }

    @VisibleForTesting
    TableStatistics computeTableStatistics(List<DeltaFileEntry> fileEntries, DeltaTable deltaTable)
    {
        return computeTableStatistics(fileEntries, deltaTable, null);
    }

    @VisibleForTesting
    TableStatistics computeTableStatistics(
            List<DeltaFileEntry> fileEntries,
            DeltaTable deltaTable,
            List<ColumnHandle> requestedColumnHandles)
    {
        List<DeltaColumnHandle> columnHandles = buildColumnHandles(deltaTable.getColumns());
        if (requestedColumnHandles != null && !requestedColumnHandles.isEmpty()) {
            Set<ColumnHandle> requestedSet = ImmutableSet.copyOf(requestedColumnHandles);
            columnHandles = columnHandles.stream()
                    .filter(requestedSet::contains)
                    .collect(Collectors.toList());
        }

        if (fileEntries.isEmpty()) {
            return createZeroStatistics(columnHandles);
        }

        double totalNumRecords = 0;

        // NaN = unknown null count (at least one file lacked it for this column)
        Map<DeltaColumnHandle, Double> nullCounts = new HashMap<>();
        columnHandles.forEach(col -> nullCounts.put(col, 0.0));

        Map<DeltaColumnHandle, Double> minValues = new HashMap<>();
        Map<DeltaColumnHandle, Double> maxValues = new HashMap<>();

        // Distinct partition values per partition column for exact NDV
        Map<DeltaColumnHandle, Set<String>> partitionDistinctValues = new HashMap<>();
        columnHandles.stream()
                .filter(col -> col.getColumnType() == PARTITION)
                .forEach(col -> partitionDistinctValues.put(col, new HashSet<>()));

        for (DeltaFileEntry entry : fileEntries) {
            // Any file without stats → cannot produce a reliable aggregate
            if (!entry.getStats().isPresent()) {
                return TableStatistics.empty();
            }
            DeltaFileStatistics stats = entry.getStats().get();

            // Any file without row count → cannot produce a reliable aggregate
            if (!stats.getNumRecords().isPresent()) {
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
                    else {
                        partitionDistinctValues.get(column).add(partitionValue);
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

                    stats.getMinColumnValue(column).ifPresent(value -> {
                        OptionalDouble asDouble = toDoubleStatistic(value, column.getDataType());
                        if (asDouble.isPresent()) {
                            minValues.merge(column, asDouble.getAsDouble(), Math::min);
                        }
                    });

                    stats.getMaxColumnValue(column).ifPresent(value -> {
                        OptionalDouble asDouble = toDoubleStatistic(value, column.getDataType());
                        if (asDouble.isPresent()) {
                            maxValues.merge(column, asDouble.getAsDouble(), Math::max);
                        }
                    });
                }
            }
        }

        if (totalNumRecords == 0) {
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
            if (isValidInRange(minValue) && isValidInRange(maxValue)) {
                colBuilder.setRange(new DoubleRange(minValue, maxValue));
            }
            else if (isValidInRange(maxValue)) {
                colBuilder.setRange(new DoubleRange(NEGATIVE_INFINITY, maxValue));
            }
            else if (isValidInRange(minValue)) {
                colBuilder.setRange(new DoubleRange(minValue, POSITIVE_INFINITY));
            }

            if (column.getColumnType() == PARTITION) {
                // Exact NDV from observed distinct partition values
                colBuilder.setDistinctValuesCount(
                        Estimate.of(partitionDistinctValues.get(column).size()));
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
                    // parameters: [precision, scale]
                    int scale = type.getParameters().size() >= 2
                            ? (int) (long) type.getParameters().get(1).getLongLiteral()
                            : 0;
                    double divisor = Math.pow(10, scale);
                    return OptionalDouble.of(((Number) value).doubleValue() / divisor);
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
                // Delta stores dates as epoch-day integers
                if (value instanceof Number) {
                    return OptionalDouble.of(((Number) value).doubleValue());
                }
                return OptionalDouble.empty();
            default:
                return OptionalDouble.empty(); // varchar, boolean, timestamp etc. — no range stats
        }
    }
}
