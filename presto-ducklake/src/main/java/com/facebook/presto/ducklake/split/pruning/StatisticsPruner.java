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
package com.facebook.presto.ducklake.split.pruning;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakeFileColumnStats;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Drops {@link DuckLakeDataFile}s whose per-file {@code ducklake_file_column_stats} prove a
 * predicate cannot match any row in the file (spec &sect;4.5). Unlike {@link PartitionPruner},
 * this runs over every predicate column that has a stats row, not just partition columns, and is
 * meant to run immediately after it. As with {@link PartitionPruner}, everything here errs toward
 * keeping a file: a missing stats row, an unparseable or absent bound (see {@link
 * StatsValueParser}), an unsupported column type, or {@code contains_nan} being true (or, for
 * FLOAT/DOUBLE, absent) all leave the corresponding side of the comparison unknown rather than
 * excluding the file.
 */
public final class StatisticsPruner
{
    private StatisticsPruner() {}

    public static List<DuckLakeDataFile> prune(
            List<DuckLakeDataFile> files,
            Map<Long, DuckLakeColumnHandle> columnsById,
            TupleDomain<ColumnHandle> predicate)
    {
        requireNonNull(files, "files is null");
        requireNonNull(columnsById, "columnsById is null");
        requireNonNull(predicate, "predicate is null");

        if (predicate.isAll()) {
            return files;
        }
        Optional<Map<ColumnHandle, Domain>> domains = predicate.getDomains();
        if (!domains.isPresent()) {
            return files;
        }

        List<PrunableColumn> prunableColumns = new ArrayList<>();
        for (DuckLakeColumnHandle column : columnsById.values()) {
            Domain domain = domains.get().get(column);
            if (domain == null || domain.isAll()) {
                continue;
            }
            if (!(domain.getValues() instanceof SortedRangeSet)) {
                // Not expected for the orderable types stats pruning applies to, but if it happens
                // there is no comparable range to build a stats domain against.
                continue;
            }
            prunableColumns.add(new PrunableColumn(column, domain));
        }
        if (prunableColumns.isEmpty()) {
            return files;
        }

        ImmutableList.Builder<DuckLakeDataFile> kept = ImmutableList.builder();
        for (DuckLakeDataFile file : files) {
            if (keep(file, prunableColumns)) {
                kept.add(file);
            }
        }
        return kept.build();
    }

    private static boolean keep(DuckLakeDataFile file, List<PrunableColumn> prunableColumns)
    {
        for (PrunableColumn column : prunableColumns) {
            if (!column.keep(file)) {
                return false;
            }
        }
        return true;
    }

    /** One predicate column with a constraining, range-shaped domain. */
    private static final class PrunableColumn
    {
        private final DuckLakeColumnHandle column;
        private final Domain predicateDomain;

        PrunableColumn(DuckLakeColumnHandle column, Domain predicateDomain)
        {
            this.column = requireNonNull(column, "column is null");
            this.predicateDomain = requireNonNull(predicateDomain, "predicateDomain is null");
        }

        boolean keep(DuckLakeDataFile file)
        {
            DuckLakeFileColumnStats stats = file.getColumnStats().get(column.getId());
            if (stats == null) {
                return true;
            }
            try {
                return buildStatsDomain(file, stats)
                        .map(predicateDomain::overlaps)
                        .orElse(true);
            }
            catch (RuntimeException e) {
                // An unexpected stats shape (or anything else that slipped past the parser): never
                // skip the file over it.
                return true;
            }
        }

        /**
         * Builds the domain of values a file's stats say this column could hold, or {@link
         * Optional#empty()} when nothing constraining can be said (equivalent to {@code
         * Domain.all}, but avoids forcing every caller to special-case it).
         */
        private Optional<Domain> buildStatsDomain(DuckLakeDataFile file, DuckLakeFileColumnStats stats)
        {
            Type type = column.getType();

            if (isAllNull(file, stats)) {
                return Optional.of(Domain.onlyNull(type));
            }
            boolean nullAllowed = !stats.getNullCount().isPresent() || stats.getNullCount().getAsLong() > 0;

            if (boundsUnknownDueToNan(type, stats)) {
                // A NaN in the file compares unordered with everything: the min/max bounds cannot
                // be trusted to exclude any value.
                return Optional.of(Domain.create(ValueSet.ofRanges(Range.all(type)), nullAllowed));
            }

            String duckLakeType = column.getColumnIdentity().getDuckLakeType();
            Optional<Object> min = stats.getMinValue().flatMap(value -> StatsValueParser.parseMin(duckLakeType, type, value));
            Optional<Object> max = stats.getMaxValue().flatMap(value -> StatsValueParser.parseMax(duckLakeType, type, value));
            return Optional.of(Domain.create(ValueSet.ofRanges(buildRange(type, min, max)), nullAllowed));
        }

        /**
         * The DuckLake extension only records FLOAT/DOUBLE min/max when {@code contains_nan} is
         * known to be false ({@code DuckLakeColumnStats::ToStats}); Parquet min/max statistics
         * themselves exclude NaN, so a float/double file with {@code contains_nan} absent may
         * still hold NaN rows the bounds say nothing about. For those two types, absence is
         * therefore treated the same as {@code true}. Every other type's {@code contains_nan} is
         * always NULL in practice, so absence there keeps meaning "no NaN" ({@code false}).
         */
        private static boolean boundsUnknownDueToNan(Type type, DuckLakeFileColumnStats stats)
        {
            if (type instanceof RealType || type instanceof DoubleType) {
                return stats.getContainsNan().orElse(true);
            }
            return stats.getContainsNan().orElse(false);
        }

        private static boolean isAllNull(DuckLakeDataFile file, DuckLakeFileColumnStats stats)
        {
            return stats.getNullCount().isPresent()
                    && file.getRecordCount() > 0
                    && stats.getNullCount().getAsLong() == file.getRecordCount();
        }

        private static Range buildRange(Type type, Optional<Object> min, Optional<Object> max)
        {
            if (min.isPresent() && max.isPresent()) {
                try {
                    return Range.range(type, min.get(), true, max.get(), true);
                }
                catch (IllegalArgumentException e) {
                    // A parsed max below the parsed min (e.g. a garbage max next to a good min):
                    // the pair is not trustworthy as a bound, so treat the range as unknown rather
                    // than picking one side to believe.
                    return Range.all(type);
                }
            }
            if (min.isPresent()) {
                return Range.greaterThanOrEqual(type, min.get());
            }
            if (max.isPresent()) {
                return Range.lessThanOrEqual(type, max.get());
            }
            return Range.all(type);
        }
    }
}
