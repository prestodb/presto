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
package com.facebook.presto.iceberg.util;

import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import org.apache.iceberg.PartitionSpec;

import java.util.EnumSet;
import java.util.Map;

import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;

public final class StatisticsUtil
{
    private StatisticsUtil()
    {
    }

    /**
     * Attempts to merge statistics from Iceberg and hive tables.
     * <br>
     * If a statistic is unknown on the Iceberg table, but known in Hive, the Hive statistic
     * will always be used. Otherwise, hive statistics are only used if specified in the
     * hive-statistic-merge-strategy
     */
    public static TableStatistics mergeHiveStatistics(TableStatistics icebergStatistics, PartitionStatistics hiveStatistics, EnumSet<ColumnStatisticType> mergeFlags, PartitionSpec spec)
    {
        if (spec.isPartitioned()) {
            return icebergStatistics;
        }
        // We really only need to merge in NDVs and null fractions from the column statistics in hive's stats
        //
        // take iceberg data and column size, and row count statistics over hive's as they're more likely
        // to be up to date since they are computed on the fly
        Map<String, HiveColumnStatistics> columnStats = hiveStatistics.getColumnStatistics();
        TableStatistics.Builder statsBuilder = TableStatistics.builder();
        statsBuilder.setTotalSize(icebergStatistics.getTotalSize());
        statsBuilder.setRowCount(icebergStatistics.getRowCount());
        icebergStatistics.getColumnStatistics().forEach((columnHandle, icebergColumnStats) -> {
            HiveColumnStatistics hiveColumnStats = columnStats.get(((IcebergColumnHandle) columnHandle).getName());
            ColumnStatistics.Builder mergedStats = ColumnStatistics.builder()
                    .setDataSize(icebergColumnStats.getDataSize())
                    .setDistinctValuesCount(icebergColumnStats.getDistinctValuesCount())
                    .setRange(icebergColumnStats.getRange())
                    .setNullsFraction(icebergColumnStats.getNullsFraction())
                    .setDistinctValuesCount(icebergColumnStats.getDistinctValuesCount())
                    .setRange(icebergColumnStats.getRange());
            if (hiveColumnStats != null) {
                // NDVs
                if (mergeFlags.contains(NUMBER_OF_DISTINCT_VALUES)) {
                    hiveColumnStats.getDistinctValuesCount().ifPresent(ndvs -> mergedStats.setDistinctValuesCount(Estimate.of(ndvs)));
                }
                // Null count
                if (mergeFlags.contains(NUMBER_OF_NON_NULL_VALUES)) {
                    hiveColumnStats.getNullsCount().ifPresent(nullCount -> {
                        Estimate nullsFraction;
                        if (!hiveStatistics.getBasicStatistics().getRowCount().isPresent()) {
                            if (icebergStatistics.getRowCount().isUnknown()) {
                                nullsFraction = Estimate.unknown();
                            }
                            else {
                                nullsFraction = Estimate.of((double) nullCount / icebergStatistics.getRowCount().getValue());
                            }
                        }
                        else {
                            nullsFraction = Estimate.of((double) nullCount / hiveStatistics.getBasicStatistics().getRowCount().getAsLong());
                        }
                        mergedStats.setNullsFraction(nullsFraction);
                    });
                }
                // data size
                if (mergeFlags.contains(ColumnStatisticType.TOTAL_SIZE_IN_BYTES)) {
                    hiveColumnStats.getTotalSizeInBytes().ifPresent(size -> mergedStats.setDataSize(Estimate.of(size)));
                }
            }
            statsBuilder.setColumnStatistics(columnHandle, mergedStats.build());
        });
        return statsBuilder.build();
    }
}
