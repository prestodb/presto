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
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import org.apache.iceberg.PartitionSpec;

import java.util.Map;

import static com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy.NONE;
import static com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy.USE_NDV;
import static com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy.USE_NULLS_FRACTIONS;
import static com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy.USE_NULLS_FRACTION_AND_NDV;

public final class StatisticsUtil
{
    private StatisticsUtil()
    {
    }

    public static TableStatistics mergeHiveStatistics(TableStatistics icebergStatistics, PartitionStatistics hiveStatistics, HiveStatisticsMergeStrategy mergeStrategy, PartitionSpec spec)
    {
        if (mergeStrategy.equals(NONE) || spec.isPartitioned()) {
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
                if (mergeStrategy.equals(USE_NDV) || mergeStrategy.equals(USE_NULLS_FRACTION_AND_NDV)) {
                    hiveColumnStats.getDistinctValuesCount().ifPresent(ndvs -> mergedStats.setDistinctValuesCount(Estimate.of(ndvs)));
                }
                if (mergeStrategy.equals(USE_NULLS_FRACTIONS) || mergeStrategy.equals(USE_NULLS_FRACTION_AND_NDV)) {
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
            }
            statsBuilder.setColumnStatistics(columnHandle, mergedStats.build());
        });
        return statsBuilder.build();
    }
}
