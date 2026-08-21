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

import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.metastore.BooleanStatistics;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static com.facebook.presto.hive.metastore.PartitionStatistics.empty;
import static java.util.Collections.emptyList;

public class PartitionQuickStats
{
    /**
     * Sentinel for "quick stats are unavailable for this partition" -- a non-Parquet serde, a footer
     * that could not be read, or a format we deliberately refuse to reason about. Converts to
     * {@link PartitionStatistics#empty()}, i.e. UNKNOWN.
     */
    public static final PartitionQuickStats EMPTY = new PartitionQuickStats("emptyPartition", emptyList(), 0);

    /**
     * Sentinel for "this partition provably contains zero rows" -- either the directory listing found
     * no files at all, or every file that was read reported zero row groups. Unlike {@link #EMPTY}
     * this carries information, and converts to a row count of 0 rather than UNKNOWN.
     * <p>
     * Kept as a separate instance rather than a flag because callers compare against {@link #EMPTY}
     * by identity (there is no {@code equals} override), notably the strategy loop in
     * {@code QuickStatsProvider}, which must stop exploring further strategies once emptiness is
     * proven.
     */
    public static final PartitionQuickStats PROVABLY_EMPTY = new PartitionQuickStats("provablyEmptyPartition", emptyList(), 0);
    private final String partitionId;
    private final List<ColumnQuickStats<?>> stats;
    private final int fileCount;

    public PartitionQuickStats(String partitionId, Collection<ColumnQuickStats<?>> stats, int fileCount)
    {
        this.partitionId = partitionId;
        this.stats = ImmutableList.copyOf(stats);
        this.fileCount = fileCount;
    }

    /**
     * @deprecated use {@link #convertToPartitionStatistics(PartitionQuickStats, boolean)}. This
     * overload emits the conservative NDV bound (i.e. behaves as if the
     * {@code hive.quick-stats.ndv-enabled} kill-switch were enabled), for callers/tests that
     * pre-date that flag.
     */
    @Deprecated
    public static PartitionStatistics convertToPartitionStatistics(PartitionQuickStats partitionQuickStats)
    {
        return convertToPartitionStatistics(partitionQuickStats, true);
    }

    /**
     * @param ndvEnabled kill-switch for the conservative distinctValuesCount (NDV) bound (see
     * {@link ColumnQuickStats#getDistinctValuesCount()}). When {@code false}, this method emits
     * byte-identical output to the pre-fix behavior (distinctValuesCount always
     * {@code OptionalLong.empty()}), for safe fleet rollback without a rebuild.
     */
    public static PartitionStatistics convertToPartitionStatistics(PartitionQuickStats partitionQuickStats, boolean ndvEnabled)
    {
        if (partitionQuickStats == PROVABLY_EMPTY) {
            // Zero rows is a fact here, not an estimate: either the listing found no files, or every
            // file read reported no row groups. Emit it as such so the CBO can act on it, instead of
            // discarding it into the same UNKNOWN bucket as "stats could not be read".
            return new PartitionStatistics(
                    new HiveBasicStatistics(OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(0)),
                    ImmutableMap.of());
        }

        if (partitionQuickStats.equals(EMPTY) || partitionQuickStats.getStats().isEmpty()) {
            return empty();
        }

        long rowCount = partitionQuickStats.getStats().get(0).getRowCount();

        ImmutableMap.Builder<String, HiveColumnStatistics> hiveColumnStatisticsBuilder = ImmutableMap.builder();
        partitionQuickStats.getStats().forEach(columnQuickStats -> {
            long nullsCount = columnQuickStats.getNullsCount();
            Object minValue = columnQuickStats.getMinValue();
            Object maxValue = columnQuickStats.getMaxValue();

            OptionalLong distinctValuesCount = OptionalLong.empty();
            if (ndvEnabled) {
                // Defensively re-clamp the per-column NDV against the PARTITION-level rowCount and
                // THIS column's own nullsCount, i.e. to max(0, partitionRowCount - columnNullsCount).
                // MetastoreHiveStatisticsProvider#validateColumnStatistics enforces both
                // distinctValuesCount <= rowCount AND distinctValuesCount <= nonNullsCount, where
                // nonNullsCount = partitionRowCount - columnNullsCount (NOT the column's own
                // rowCount). Clamping to nonNullsCount alone satisfies both checks (nonNullsCount
                // <= rowCount always). This matters because partitionQuickStats.getStats() is
                // backed by a HashMap (see ParquetQuickStatsBuilder#buildQuickStats), so the
                // partition-level "rowCount" above (taken from an arbitrary column) can diverge
                // from any individual column's own rowCount/nullsCount under schema evolution (a
                // column absent from some files in the partition) -- clamping by rowCount alone is
                // NOT sufficient to guarantee distinctValuesCount <= nonNullsCount in that case,
                // which would otherwise throw HIVE_CORRUPTED_COLUMN_STATISTICS
                // (ignore-corrupted-statistics defaults to false).
                distinctValuesCount = clampToCeiling(columnQuickStats.getDistinctValuesCount(), Math.max(0, rowCount - nullsCount));
            }

            HiveColumnStatistics hiveColumnStatistics;
            if (columnQuickStats.getStatType().equals(Integer.class)) {
                hiveColumnStatistics = createIntegerColumnStatistics(OptionalLong.of((int) minValue), OptionalLong.of((int) maxValue),
                        OptionalLong.of(nullsCount), distinctValuesCount);
            }
            else if (columnQuickStats.getStatType().equals(Long.class)) {
                hiveColumnStatistics = createIntegerColumnStatistics(OptionalLong.of((long) minValue), OptionalLong.of((long) maxValue),
                        OptionalLong.of(nullsCount), distinctValuesCount);
            }
            else if (columnQuickStats.getStatType().equals(Double.class)) {
                hiveColumnStatistics = createDoubleColumnStatistics(OptionalDouble.of((double) minValue), OptionalDouble.of((double) maxValue),
                        OptionalLong.of(nullsCount), distinctValuesCount);
            }
            else if (columnQuickStats.getStatType().equals(Slice.class)) {
                // VARCHAR/VARBINARY: no min/max is collected, so no NDV bound is available either;
                // behavior here is unchanged from before this fix.
                hiveColumnStatistics = createBinaryColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(nullsCount));
            }
            else if (columnQuickStats.getStatType().equals(Boolean.class)) {
                hiveColumnStatistics = ndvEnabled
                        ? HiveColumnStatistics.builder()
                                .setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty()))
                                .setNullsCount(OptionalLong.of(nullsCount))
                                .setDistinctValuesCount(distinctValuesCount)
                                .build()
                        // Byte-identical to the pre-fix code path when the kill-switch is off.
                        : createBooleanColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(nullsCount));
            }
            else if (columnQuickStats.getStatType().equals(ChronoLocalDate.class)) {
                hiveColumnStatistics = createDateColumnStatistics(Optional.of((LocalDate) minValue), Optional.of((LocalDate) maxValue),
                        OptionalLong.of(nullsCount), distinctValuesCount);
            }
            else {
                hiveColumnStatistics = new HiveColumnStatistics(Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        OptionalLong.empty(),
                        OptionalLong.empty(),
                        OptionalLong.of(nullsCount),
                        OptionalLong.empty());
            }

            hiveColumnStatisticsBuilder.put(columnQuickStats.getColumnName(), hiveColumnStatistics);
        });
        HiveBasicStatistics hiveBasicStatistics = new HiveBasicStatistics(
                OptionalLong.of(partitionQuickStats.getFileCount()),
                OptionalLong.of(rowCount),
                OptionalLong.empty(),
                OptionalLong.empty());

        return new PartitionStatistics(hiveBasicStatistics, hiveColumnStatisticsBuilder.build());
    }

    private static OptionalLong clampToCeiling(OptionalLong distinctValuesCount, long ceiling)
    {
        if (!distinctValuesCount.isPresent()) {
            return distinctValuesCount;
        }
        return OptionalLong.of(Math.max(0, Math.min(distinctValuesCount.getAsLong(), ceiling)));
    }

    public List<ColumnQuickStats<?>> getStats()
    {
        return stats;
    }

    public int getFileCount()
    {
        return fileCount;
    }

    @Override
    public String toString()
    {
        return "PartitionQuickStats{" +
                "partitionId='" + partitionId + '\'' +
                ", stats=" + stats +
                ", fileCount=" + fileCount +
                '}';
    }
}
