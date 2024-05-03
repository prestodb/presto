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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.iceberg.ColumnIdentity.TypeCategory;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.OptionalLong;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy.NONE;
import static com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy.USE_NDV;
import static com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy.USE_NULLS_FRACTIONS;
import static com.facebook.presto.iceberg.util.HiveStatisticsMergeStrategy.USE_NULLS_FRACTION_AND_NDV;
import static com.facebook.presto.iceberg.util.StatisticsUtil.mergeHiveStatistics;
import static org.testng.Assert.assertEquals;

public class TestStatisticsUtil
{
    @Test
    public void testMergeStrategyNone()
    {
        TableStatistics merged = mergeHiveStatistics(generateSingleColumnIcebergStats(), generateSingleColumnHiveStatistics(), NONE, PartitionSpec.unpartitioned());
        assertEquals(Estimate.of(1), merged.getRowCount());
        assertEquals(Estimate.unknown(), merged.getTotalSize());
        assertEquals(1, merged.getColumnStatistics().size());
        ColumnStatistics stats = merged.getColumnStatistics().values().stream().findFirst().get();
        assertEquals(Estimate.of(8), stats.getDataSize());
        assertEquals(Estimate.of(1), stats.getDistinctValuesCount());
        assertEquals(Estimate.of(0.1), stats.getNullsFraction());
        assertEquals(new DoubleRange(0.0, 1.0), stats.getRange().get());
    }

    @Test
    public void testMergeStrategyWithPartitioned()
    {
        TableStatistics merged = mergeHiveStatistics(generateSingleColumnIcebergStats(), generateSingleColumnHiveStatistics(), USE_NULLS_FRACTION_AND_NDV,
                PartitionSpec.builderFor(new Schema(Types.NestedField.required(0, "test", Types.IntegerType.get()))).bucket("test", 100).build());
        assertEquals(Estimate.of(1), merged.getRowCount());
        assertEquals(Estimate.unknown(), merged.getTotalSize());
        assertEquals(1, merged.getColumnStatistics().size());
        ColumnStatistics stats = merged.getColumnStatistics().values().stream().findFirst().get();
        assertEquals(Estimate.of(8), stats.getDataSize());
        assertEquals(Estimate.of(1), stats.getDistinctValuesCount());
        assertEquals(Estimate.of(0.1), stats.getNullsFraction());
        assertEquals(new DoubleRange(0.0, 1.0), stats.getRange().get());
    }

    @Test
    public void testMergeStrategyNDVs()
    {
        TableStatistics merged = mergeHiveStatistics(generateSingleColumnIcebergStats(), generateSingleColumnHiveStatistics(), USE_NDV, PartitionSpec.unpartitioned());
        assertEquals(Estimate.of(1), merged.getRowCount());
        assertEquals(Estimate.unknown(), merged.getTotalSize());
        assertEquals(1, merged.getColumnStatistics().size());
        ColumnStatistics stats = merged.getColumnStatistics().values().stream().findFirst().get();
        assertEquals(Estimate.of(8), stats.getDataSize());
        assertEquals(Estimate.of(2), stats.getDistinctValuesCount());
        assertEquals(Estimate.of(0.1), stats.getNullsFraction());
        assertEquals(new DoubleRange(0.0, 1.0), stats.getRange().get());
    }

    @Test
    public void testMergeStrategyNulls()
    {
        TableStatistics merged = mergeHiveStatistics(generateSingleColumnIcebergStats(), generateSingleColumnHiveStatistics(), USE_NULLS_FRACTIONS, PartitionSpec.unpartitioned());
        assertEquals(Estimate.of(1), merged.getRowCount());
        assertEquals(Estimate.unknown(), merged.getTotalSize());
        assertEquals(1, merged.getColumnStatistics().size());
        ColumnStatistics stats = merged.getColumnStatistics().values().stream().findFirst().get();
        assertEquals(Estimate.of(8), stats.getDataSize());
        assertEquals(Estimate.of(1), stats.getDistinctValuesCount());
        assertEquals(Estimate.of(1.0), stats.getNullsFraction());
        assertEquals(new DoubleRange(0.0, 1.0), stats.getRange().get());
    }

    @Test
    public void testMergeStrategyNDVsAndNulls()
    {
        TableStatistics merged = mergeHiveStatistics(generateSingleColumnIcebergStats(), generateSingleColumnHiveStatistics(), USE_NULLS_FRACTION_AND_NDV, PartitionSpec.unpartitioned());
        assertEquals(Estimate.of(1), merged.getRowCount());
        assertEquals(Estimate.unknown(), merged.getTotalSize());
        assertEquals(1, merged.getColumnStatistics().size());
        ColumnStatistics stats = merged.getColumnStatistics().values().stream().findFirst().get();
        assertEquals(Estimate.of(8), stats.getDataSize());
        assertEquals(Estimate.of(2), stats.getDistinctValuesCount());
        assertEquals(Estimate.of(1.0), stats.getNullsFraction());
        assertEquals(new DoubleRange(0.0, 1.0), stats.getRange().get());
    }

    private static TableStatistics generateSingleColumnIcebergStats()
    {
        return TableStatistics.builder()
                .setRowCount(Estimate.of(1))
                .setTotalSize(Estimate.unknown())
                .setColumnStatistics(new IcebergColumnHandle(
                                new ColumnIdentity(1, "test", TypeCategory.PRIMITIVE, Collections.emptyList()),
                                INTEGER,
                                Optional.empty(),
                                REGULAR),
                        ColumnStatistics.builder()
                                .setNullsFraction(Estimate.of(0.1))
                                .setRange(new DoubleRange(0.0, 1.0))
                                .setDataSize(Estimate.of(8))
                                .setDistinctValuesCount(Estimate.of(1))
                                .build())
                .build();
    }

    private static PartitionStatistics generateSingleColumnHiveStatistics()
    {
        return new PartitionStatistics(
                new HiveBasicStatistics(1, 2, 16, 16),
                ImmutableMap.of("test",
                        HiveColumnStatistics.createIntegerColumnStatistics(
                                OptionalLong.of(1),
                                OptionalLong.of(2),
                                OptionalLong.of(2),
                                OptionalLong.of(2))));
    }
}
