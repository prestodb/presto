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
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static com.facebook.presto.hive.metastore.PartitionStatistics.empty;
import static java.util.Collections.emptyList;

public class PartitionQuickStats
{
    /**
     * Quick stats are unavailable for this partition; converts to {@link PartitionStatistics#empty()}.
     */
    public static final PartitionQuickStats UNKNOWN_STATS = new PartitionQuickStats("unknownStats", emptyList(), 0, 0);

    /**
     * The partition's data files hold no rows; converts to a row count of 0.
     */
    public static final PartitionQuickStats ZERO_INPUT_ROWS = new PartitionQuickStats("zeroInputRows", emptyList(), 0, 0);

    private final String partitionId;
    private final List<ColumnQuickStats<?>> stats;
    private final int fileCount;
    private final long rowCount;

    public PartitionQuickStats(String partitionId, Collection<ColumnQuickStats<?>> stats, int fileCount, long rowCount)
    {
        this.partitionId = partitionId;
        this.stats = ImmutableList.copyOf(stats);
        this.fileCount = fileCount;
        this.rowCount = rowCount;
    }

    public static PartitionStatistics convertToPartitionStatistics(PartitionQuickStats partitionQuickStats)
    {
        if (partitionQuickStats == ZERO_INPUT_ROWS) {
            return new PartitionStatistics(
                    new HiveBasicStatistics(OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(0), OptionalLong.of(0)),
                    ImmutableMap.of());
        }

        if (partitionQuickStats.equals(UNKNOWN_STATS)) {
            return empty();
        }

        long rowCount = partitionQuickStats.getRowCount();

        ImmutableMap.Builder<String, HiveColumnStatistics> hiveColumnStatisticsBuilder = ImmutableMap.builder();
        partitionQuickStats.getStats().forEach(columnQuickStats -> {
            long nullsCount = columnQuickStats.getNullsCount();
            Object minValue = columnQuickStats.getMinValue();
            Object maxValue = columnQuickStats.getMaxValue();

            OptionalLong distinctValuesCount = columnQuickStats.getDistinctValuesCount(rowCount - nullsCount);

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
                hiveColumnStatistics = createBinaryColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(nullsCount));
            }
            else if (columnQuickStats.getStatType().equals(Boolean.class)) {
                hiveColumnStatistics = HiveColumnStatistics.builder()
                        .setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty()))
                        .setNullsCount(OptionalLong.of(nullsCount))
                        .setDistinctValuesCount(distinctValuesCount)
                        .build();
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
                OptionalLong.of(partitionQuickStats.getRowCount()),
                OptionalLong.empty(),
                OptionalLong.empty());

        return new PartitionStatistics(hiveBasicStatistics, hiveColumnStatisticsBuilder.build());
    }

    public long getRowCount()
    {
        return rowCount;
    }

    /**
     * Drops the distinct-values count from every column.
     */
    public static PartitionStatistics withoutDistinctValuesCount(PartitionStatistics statistics)
    {
        if (statistics.getColumnStatistics().isEmpty()) {
            return statistics;
        }
        ImmutableMap.Builder<String, HiveColumnStatistics> columns = ImmutableMap.builder();
        statistics.getColumnStatistics().forEach((name, column) -> columns.put(name,
                HiveColumnStatistics.builder(column).setDistinctValuesCount(OptionalLong.empty()).build()));
        return new PartitionStatistics(statistics.getBasicStatistics(), columns.build());
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
                ", rowCount=" + rowCount +
                '}';
    }
}
