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
package com.facebook.presto.ducklake.statistics;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.DuckLakeTableHandle;
import com.facebook.presto.ducklake.DuckLakeTableLayoutHandle;
import com.facebook.presto.ducklake.catalog.DuckLakeCatalog;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakeDeleteFile;
import com.facebook.presto.ducklake.catalog.DuckLakeFileColumnStats;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.facebook.presto.ducklake.split.pruning.DataFilePruner;
import com.facebook.presto.ducklake.split.pruning.StatsValueParser;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import jakarta.inject.Inject;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import static java.util.Objects.requireNonNull;

/**
 * Builds {@link TableStatistics} for the cost-based optimizer straight from the data files visible
 * at a table handle's snapshot, the way {@code IcebergAbstractMetadata.getTableStatistics} delegates
 * to Iceberg's own {@code TableStatisticsMaker}. Row count and column statistics are computed here
 * rather than read from {@code ducklake_table_stats}/{@code ducklake_table_column_stats}, because
 * neither of those catalog tables is scoped to a snapshot and {@code ducklake_table_stats.record_count}
 * counts every row ever written rather than the rows currently visible (it does not account for
 * deletes). Nested types have no {@code ducklake_file_column_stats} row at all (spec &sect;4.5), so
 * their column statistics are always {@link ColumnStatistics#empty() unknown}.
 *
 * <p><b>Row count undercounts inlined rows.</b> Small inserts DuckLake keeps inlined in ordinary
 * catalog tables (spec &sect;3) rather than as a Parquet data file are not visited here at all, so a
 * table with pending inlined rows reports a row count lower than its true row count by however many
 * rows are inlined. This is bounded by the producer's inlining row limit (a small constant), so the
 * undercount can never be large, but it is real: it is the only known source of underestimate in
 * this class.
 */
public class TableStatisticsMaker
{
    private final DuckLakeCatalog catalog;

    @Inject
    public TableStatisticsMaker(DuckLakeCatalog catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    public TableStatistics makeTableStatistics(
            DuckLakeTableHandle tableHandle,
            Optional<DuckLakeTableLayoutHandle> tableLayoutHandle,
            List<ColumnHandle> columnHandles,
            Constraint<ColumnHandle> constraint)
    {
        requireNonNull(tableHandle, "tableHandle is null");
        requireNonNull(tableLayoutHandle, "tableLayoutHandle is null");
        requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(constraint, "constraint is null");

        long snapshotId = tableHandle.getSnapshotId();
        TupleDomain<ColumnHandle> predicate = tableLayoutHandle
                .map(DuckLakeTableLayoutHandle::getDomainPredicate)
                .orElseGet(constraint::getSummary);

        List<DuckLakeDataFile> allFiles = catalog.listDataFiles(snapshotId, tableHandle.toDuckLakeTable());
        List<DuckLakePartitionField> partitionFields = tableHandle.getSchema().getPartitionFields();
        List<DuckLakeDataFile> survivingFiles = DataFilePruner.prune(allFiles, partitionFields, predicate);

        long totalRecordCount = 0;
        long totalDeleteCount = 0;
        long totalFileSize = 0;
        for (DuckLakeDataFile file : survivingFiles) {
            totalRecordCount += file.getRecordCount();
            totalDeleteCount += file.getDeleteFile().map(DuckLakeDeleteFile::getDeleteCount).orElse(0L);
            totalFileSize += file.getFileSizeBytes();
        }
        long rowCount = Math.max(totalRecordCount - totalDeleteCount, 0);

        TableStatistics.Builder statistics = TableStatistics.builder()
                .setRowCount(Estimate.of(rowCount))
                .setTotalSize(Estimate.of(totalFileSize));

        for (ColumnHandle columnHandle : columnHandles) {
            DuckLakeColumnHandle column = (DuckLakeColumnHandle) columnHandle;
            if (column.isPathColumn() || column.isRowIdColumn() || column.isRowPositionColumn()) {
                continue;
            }
            statistics.setColumnStatistics(columnHandle, columnStatistics(column, survivingFiles));
        }
        return statistics.build();
    }

    private static ColumnStatistics columnStatistics(DuckLakeColumnHandle column, List<DuckLakeDataFile> files)
    {
        return ColumnStatistics.builder()
                .setNullsFraction(nullsFraction(column, files))
                .setDataSize(dataSize(column, files))
                .setRange(range(column, files))
                .build();
    }

    /**
     * Null fraction over the files that report a {@code null_count} for this column: files that
     * do not (an absent stats row, or a stats row with no {@code null_count}) are excluded from
     * both the numerator and the denominator rather than treated as "zero nulls", since {@code
     * value_count} (which does not include nulls) must never be substituted for {@code
     * record_count} here.
     */
    private static Estimate nullsFraction(DuckLakeColumnHandle column, List<DuckLakeDataFile> files)
    {
        long recordCount = 0;
        long nullCount = 0;
        boolean anyReported = false;
        for (DuckLakeDataFile file : files) {
            DuckLakeFileColumnStats stats = file.getColumnStats().get(column.getId());
            if (stats == null || !stats.getNullCount().isPresent()) {
                continue;
            }
            anyReported = true;
            recordCount += file.getRecordCount();
            nullCount += stats.getNullCount().getAsLong();
        }
        if (!anyReported) {
            return Estimate.unknown();
        }
        if (recordCount <= 0) {
            return Estimate.of(0);
        }
        return Estimate.of((double) nullCount / recordCount);
    }

    private static Estimate dataSize(DuckLakeColumnHandle column, List<DuckLakeDataFile> files)
    {
        long size = 0;
        boolean anyReported = false;
        for (DuckLakeDataFile file : files) {
            DuckLakeFileColumnStats stats = file.getColumnStats().get(column.getId());
            if (stats == null || !stats.getColumnSizeBytes().isPresent()) {
                continue;
            }
            anyReported = true;
            size += stats.getColumnSizeBytes().getAsLong();
        }
        return anyReported ? Estimate.of(size) : Estimate.unknown();
    }

    /**
     * A range for the types the engine's {@code StatsUtil.toStatsRepresentation} can convert a
     * literal into a stats double for, minus {@code BOOLEAN} (whose two values carry no useful
     * range) and every timestamp type (the engine ignores a connector's timestamp range for
     * filtering, and {@code SHOW STATS} would misrender millis as micros): the integer types,
     * {@code REAL}/{@code DOUBLE}/{@code DECIMAL} (parsed as a plain decimal, not the type's
     * on-disk encoding) and {@code DATE} (parsed through {@link StatsValueParser}, whose result is
     * already epoch days). Any surviving file that lacks a usable bound for the column -- no stats
     * row, no {@code min_value}/{@code max_value}, an unparseable value, or (for {@code
     * REAL}/{@code DOUBLE}) a {@code contains_nan} that is not known to be {@code false} -- makes
     * the whole column's range unknown, since an unbounded file makes the table's range unknown.
     */
    private static Optional<DoubleRange> range(DuckLakeColumnHandle column, List<DuckLakeDataFile> files)
    {
        Type type = column.getType();
        boolean numeric = isNumericRangeType(type);
        boolean date = type instanceof DateType;
        if (!numeric && !date) {
            return Optional.empty();
        }
        if (files.isEmpty()) {
            return Optional.empty();
        }

        String duckLakeType = column.getColumnIdentity().getDuckLakeType();
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;
        for (DuckLakeDataFile file : files) {
            DuckLakeFileColumnStats stats = file.getColumnStats().get(column.getId());
            if (stats == null || !stats.getMinValue().isPresent() || !stats.getMaxValue().isPresent()) {
                return Optional.empty();
            }
            if (isFloatingPoint(type) && !isKnownNanFree(stats)) {
                return Optional.empty();
            }

            OptionalDouble fileMin = numeric
                    ? parseNumericBound(stats.getMinValue().get())
                    : parseDateBound(duckLakeType, stats.getMinValue().get(), false);
            OptionalDouble fileMax = numeric
                    ? parseNumericBound(stats.getMaxValue().get())
                    : parseDateBound(duckLakeType, stats.getMaxValue().get(), true);
            if (!fileMin.isPresent() || !fileMax.isPresent()) {
                return Optional.empty();
            }
            min = Math.min(min, fileMin.getAsDouble());
            max = Math.max(max, fileMax.getAsDouble());
        }
        return Optional.of(new DoubleRange(min, max));
    }

    private static boolean isNumericRangeType(Type type)
    {
        return type instanceof TinyintType
                || type instanceof SmallintType
                || type instanceof IntegerType
                || type instanceof BigintType
                || type instanceof RealType
                || type instanceof DoubleType
                || type instanceof DecimalType;
    }

    private static boolean isFloatingPoint(Type type)
    {
        return type instanceof RealType || type instanceof DoubleType;
    }

    /**
     * DuckLake only records FLOAT/DOUBLE min/max when {@code contains_nan} is known to be false
     * ({@code DuckLakeColumnStats::ToStats}); an absent {@code contains_nan} therefore does not
     * mean "no NaN" for these two types the way it does for every other stats-bearing type.
     */
    private static boolean isKnownNanFree(DuckLakeFileColumnStats stats)
    {
        return stats.getContainsNan().isPresent() && !stats.getContainsNan().get();
    }

    private static OptionalDouble parseNumericBound(String raw)
    {
        try {
            return OptionalDouble.of(new BigDecimal(raw.trim()).doubleValue());
        }
        catch (NumberFormatException e) {
            // Covers DuckDB's inf/-inf/nan tokens along with any other unparseable value: treat
            // the bound as unknown rather than failing the query.
            return OptionalDouble.empty();
        }
    }

    private static OptionalDouble parseDateBound(String duckLakeType, String raw, boolean isMax)
    {
        Optional<Object> parsed = isMax
                ? StatsValueParser.parseMax(duckLakeType, DateType.DATE, raw)
                : StatsValueParser.parseMin(duckLakeType, DateType.DATE, raw);
        return parsed.map(value -> OptionalDouble.of((Long) value)).orElseGet(OptionalDouble::empty);
    }
}
