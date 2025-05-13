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
    public static final PartitionQuickStats EMPTY = new PartitionQuickStats("emptyPartition", emptyList(), 0);
    private final String partitionId;
    private final List<ColumnQuickStats<?>> stats;
    private final int fileCount;

    public PartitionQuickStats(String partitionId, Collection<ColumnQuickStats<?>> stats, int fileCount)
    {
        this.partitionId = partitionId;
        this.stats = ImmutableList.copyOf(stats);
        this.fileCount = fileCount;
    }

    public static PartitionStatistics convertToPartitionStatistics(PartitionQuickStats partitionQuickStats)
    {
        if (partitionQuickStats.equals(EMPTY) || partitionQuickStats.getStats().isEmpty()) {
            return empty();
        }

        ImmutableMap.Builder<String, HiveColumnStatistics> hiveColumnStatisticsBuilder = ImmutableMap.builder();
        partitionQuickStats.getStats().forEach(columnQuickStats -> {
            long nullsCount = columnQuickStats.getNullsCount();
            Object minValue = columnQuickStats.getMinValue();
            Object maxValue = columnQuickStats.getMaxValue();

            HiveColumnStatistics hiveColumnStatistics;
            if (columnQuickStats.getStatType().equals(Integer.class)) {
                hiveColumnStatistics = createIntegerColumnStatistics(OptionalLong.of((int) minValue), OptionalLong.of((int) maxValue),
                        OptionalLong.of(nullsCount), OptionalLong.empty());
            }
            else if (columnQuickStats.getStatType().equals(Long.class)) {
                hiveColumnStatistics = createIntegerColumnStatistics(OptionalLong.of((long) minValue), OptionalLong.of((long) maxValue),
                        OptionalLong.of(nullsCount), OptionalLong.empty());
            }
            else if (columnQuickStats.getStatType().equals(Double.class)) {
                hiveColumnStatistics = createDoubleColumnStatistics(OptionalDouble.of((double) minValue), OptionalDouble.of((double) maxValue),
                        OptionalLong.of(nullsCount), OptionalLong.empty());
            }
            else if (columnQuickStats.getStatType().equals(Slice.class)) {
                hiveColumnStatistics = createBinaryColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(nullsCount));
            }
            else if (columnQuickStats.getStatType().equals(Boolean.class)) {
                hiveColumnStatistics = createBooleanColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(nullsCount));
            }
            else if (columnQuickStats.getStatType().equals(ChronoLocalDate.class)) {
                hiveColumnStatistics = createDateColumnStatistics(Optional.of((LocalDate) minValue), Optional.of((LocalDate) maxValue),
                        OptionalLong.of(nullsCount), OptionalLong.empty());
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

        long rowCount = partitionQuickStats.getStats().get(0).getRowCount();
        HiveBasicStatistics hiveBasicStatistics = new HiveBasicStatistics(
                OptionalLong.of(partitionQuickStats.getFileCount()),
                OptionalLong.of(rowCount),
                OptionalLong.empty(),
                OptionalLong.empty());

        return new PartitionStatistics(hiveBasicStatistics, hiveColumnStatisticsBuilder.build());
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
