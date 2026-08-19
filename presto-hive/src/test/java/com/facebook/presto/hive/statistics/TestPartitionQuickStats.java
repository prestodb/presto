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

import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.OptionalLong;

import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static com.facebook.presto.hive.statistics.PartitionQuickStats.convertToPartitionStatistics;
import static com.facebook.presto.hive.statistics.PartitionQuickStats.withoutDistinctValuesCount;
import static org.testng.Assert.assertEquals;

public class TestPartitionQuickStats
{
    /**
     * A column added later is missing from older files, so the NDV estimate must use the partition's row
     * count, not the column's.
     */
    @Test
    public void testDistinctValuesCountUsesPartitionNonNullCount()
    {
        ColumnQuickStats<Long> added = new ColumnQuickStats<>("added", Long.class);
        added.setMinValue(1L);
        added.setMaxValue(100_000L);
        added.addToNullsCount(50L);

        PartitionStatistics statistics = convertToPartitionStatistics(new PartitionQuickStats("p1", ImmutableList.of(added), 2, 200));

        assertEquals(statistics.getBasicStatistics().getRowCount(), OptionalLong.of(200L));
        // 200 partition rows minus 50 nulls
        assertEquals(statistics.getColumnStatistics().get("added").getDistinctValuesCount(), OptionalLong.of(150L));
        MetastoreHiveStatisticsProvider.validatePartitionStatistics(new SchemaTableName("schema", "table"), ImmutableMap.of("p1", statistics));
    }

    @Test
    public void testWithoutDistinctValuesCountMatchesStatisticsWithoutNdv()
    {
        ColumnQuickStats<Boolean> flag = new ColumnQuickStats<>("flag", Boolean.class);
        flag.addToNullsCount(2L);

        PartitionStatistics withNdv = convertToPartitionStatistics(new PartitionQuickStats("p1", ImmutableList.of(flag), 1, 10));
        assertEquals(withNdv.getColumnStatistics().get("flag").getDistinctValuesCount(), OptionalLong.of(2L));

        HiveColumnStatistics withoutNdv = withoutDistinctValuesCount(withNdv).getColumnStatistics().get("flag");
        assertEquals(withoutNdv, createBooleanColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(2L)));
    }
}
