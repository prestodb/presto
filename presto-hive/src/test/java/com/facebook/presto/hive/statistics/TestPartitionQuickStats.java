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
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.OptionalLong;

import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link PartitionQuickStats#convertToPartitionStatistics(PartitionQuickStats, boolean)}:
 * the {@code distinctValuesCount <= nonNullsCount} clamping invariant (M1) and the
 * {@code hive.quick-stats.ndv-enabled} kill-switch (M2).
 */
public class TestPartitionQuickStats
{
    /**
     * Reproduces the schema-evolution divergence scenario from the review: the partition-level
     * rowCount (taken from an arbitrary column, since {@code PartitionQuickStats#getStats()} is
     * ultimately backed by a {@code HashMap} in {@link ParquetQuickStatsBuilder}) is smaller than
     * another column's own rowCount. That other column is dense-with-nulls and has a wide value
     * range, so its raw NDV bound (from {@link ColumnQuickStats#getDistinctValuesCount()}) exceeds
     * {@code partitionRowCount - itsOwnNullsCount} (nonNullsCount, as used by
     * MetastoreHiveStatisticsProvider#validateColumnStatistics). The emitted distinctValuesCount
     * must never exceed that nonNullsCount, or validation throws HIVE_CORRUPTED_COLUMN_STATISTICS.
     */
    @Test
    public void testDistinctValuesCountNeverExceedsNonNullsCountUnderRowCountDivergence()
    {
        // Column A: becomes stats.get(0) -- i.e. the partition-level rowCount (200) is taken from
        // this column. Narrow range, no nulls: NDV = 5 (unaffected by the divergence).
        ColumnQuickStats<Long> columnA = new ColumnQuickStats<>("sparse_col", Long.class);
        columnA.setMinValue(1L);
        columnA.setMaxValue(5L);
        columnA.addToRowCount(200L);
        columnA.addToNullsCount(0L);

        // Column B: its OWN rowCount (1000) diverges from the partition-level rowCount (200) --
        // e.g. present in more files than column A under schema evolution. Dense with nulls (50)
        // and a wide range (100001), so its raw NDV bound is nonNullCount = 950.
        ColumnQuickStats<Long> columnB = new ColumnQuickStats<>("dense_col_with_nulls", Long.class);
        columnB.setMinValue(0L);
        columnB.setMaxValue(100_000L);
        columnB.addToRowCount(1000L);
        columnB.addToNullsCount(50L);
        // Sanity-check the precondition this test relies on: the raw (unclamped) bound must indeed
        // exceed the partition-level nonNullsCount below, otherwise this test would not actually
        // exercise the M1 divergence clamp.
        assertEquals(columnB.getDistinctValuesCount(), OptionalLong.of(950L));

        PartitionQuickStats partitionQuickStats = new PartitionQuickStats("p1", ImmutableList.of(columnA, columnB), 5);
        PartitionStatistics partitionStatistics = PartitionQuickStats.convertToPartitionStatistics(partitionQuickStats, true);

        long partitionRowCount = partitionStatistics.getBasicStatistics().getRowCount().getAsLong();
        assertEquals(partitionRowCount, 200L, "partition rowCount should come from columnA (stats.get(0))");

        HiveColumnStatistics columnBStats = partitionStatistics.getColumnStatistics().get("dense_col_with_nulls");
        assertTrue(columnBStats.getDistinctValuesCount().isPresent());
        long dvc = columnBStats.getDistinctValuesCount().getAsLong();
        long columnBNullsCount = columnBStats.getNullsCount().getAsLong();
        long nonNullsCount = partitionRowCount - columnBNullsCount;

        // This is exactly the invariant MetastoreHiveStatisticsProvider#validateColumnStatistics
        // enforces (distinctValuesCount <= nonNullsCount, using the PARTITION rowCount and this
        // column's own nullsCount); it must hold, or the query would hard-fail with
        // HIVE_CORRUPTED_COLUMN_STATISTICS.
        assertEquals(nonNullsCount, 150L);
        assertTrue(dvc <= nonNullsCount, "distinctValuesCount (" + dvc + ") must be <= nonNullsCount (" + nonNullsCount + ")");
        assertTrue(dvc <= partitionRowCount, "distinctValuesCount (" + dvc + ") must be <= partition rowCount (" + partitionRowCount + ")");
        assertTrue(dvc >= 0);
        assertEquals(dvc, 150L);
    }

    @Test
    public void testNdvEnabledEmitsConservativeDistinctValuesCount()
    {
        ColumnQuickStats<Integer> status = new ColumnQuickStats<>("status", Integer.class);
        status.setMinValue(0);
        status.setMaxValue(4);
        status.addToRowCount(1000L);
        status.addToNullsCount(0L);

        PartitionQuickStats partitionQuickStats = new PartitionQuickStats("p1", ImmutableList.of(status), 1);
        PartitionStatistics partitionStatistics = PartitionQuickStats.convertToPartitionStatistics(partitionQuickStats, true);

        HiveColumnStatistics statusStats = partitionStatistics.getColumnStatistics().get("status");
        assertEquals(statusStats.getDistinctValuesCount(), OptionalLong.of(5L));
    }

    @Test
    public void testNdvDisabledIsByteIdenticalToPreFixBehaviorForIntegerColumn()
    {
        ColumnQuickStats<Integer> status = new ColumnQuickStats<>("status", Integer.class);
        status.setMinValue(0);
        status.setMaxValue(4);
        status.addToRowCount(1000L);
        status.addToNullsCount(3L);

        PartitionQuickStats partitionQuickStats = new PartitionQuickStats("p1", ImmutableList.of(status), 1);
        PartitionStatistics actual = PartitionQuickStats.convertToPartitionStatistics(partitionQuickStats, false);

        HiveColumnStatistics statusStats = actual.getColumnStatistics().get("status");
        assertFalse(statusStats.getDistinctValuesCount().isPresent(), "Kill-switch disabled: distinctValuesCount must remain unset");

        // Byte-identical to the exact pre-fix construction (OptionalLong.empty() in the NDV slot).
        HiveColumnStatistics expected = createIntegerColumnStatistics(OptionalLong.of(0), OptionalLong.of(4), OptionalLong.of(3L), OptionalLong.empty());
        assertEquals(statusStats, expected);
    }

    @Test
    public void testNdvDisabledIsByteIdenticalToPreFixBehaviorForBooleanColumn()
    {
        ColumnQuickStats<Boolean> flag = new ColumnQuickStats<>("flag", Boolean.class);
        flag.addToRowCount(10L);
        flag.addToNullsCount(2L);

        PartitionQuickStats partitionQuickStats = new PartitionQuickStats("p1", ImmutableList.of(flag), 1);
        PartitionStatistics disabled = PartitionQuickStats.convertToPartitionStatistics(partitionQuickStats, false);
        PartitionStatistics enabled = PartitionQuickStats.convertToPartitionStatistics(partitionQuickStats, true);

        HiveColumnStatistics disabledStats = disabled.getColumnStatistics().get("flag");
        HiveColumnStatistics enabledStats = enabled.getColumnStatistics().get("flag");

        // Disabled: byte-identical to the exact pre-fix construction for booleans (which used the
        // 3-arg factory with no distinctValuesCount parameter at all).
        HiveColumnStatistics expected = createBooleanColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(2L));
        assertEquals(disabledStats, expected);
        assertFalse(disabledStats.getDistinctValuesCount().isPresent());

        // Enabled: distinctValuesCount is populated (min(nonNullCount=8, 2) = 2), everything else
        // about the boolean stats (nulls count, absent true/false counts) is unchanged.
        assertEquals(enabledStats.getDistinctValuesCount(), OptionalLong.of(2L));
        assertEquals(enabledStats.getNullsCount(), disabledStats.getNullsCount());
        assertEquals(enabledStats.getBooleanStatistics(), disabledStats.getBooleanStatistics());
    }

    /**
     * The PROVABLY_EMPTY sentinel must convert to an explicit all-zero
     * {@link com.facebook.presto.hive.HiveBasicStatistics}, not to {@link PartitionStatistics#empty()}
     * (which is UNKNOWN, i.e. NaN by the time the CBO sees it).
     */
    @Test
    public void testProvablyEmptyProducesZeroBasicStatistics()
    {
        for (boolean ndvEnabled : new boolean[] {true, false}) {
            PartitionStatistics statistics = PartitionQuickStats.convertToPartitionStatistics(PartitionQuickStats.PROVABLY_EMPTY, ndvEnabled);

            HiveBasicStatistics basicStatistics = statistics.getBasicStatistics();
            assertEquals(basicStatistics.getRowCount(), OptionalLong.of(0));
            assertEquals(basicStatistics.getFileCount(), OptionalLong.of(0));
            assertEquals(basicStatistics.getInMemoryDataSizeInBytes(), OptionalLong.of(0));
            assertEquals(basicStatistics.getOnDiskDataSizeInBytes(), OptionalLong.of(0));
            // A provably empty partition carries no column statistics: there are no rows to describe,
            // and StatsNormalizer derives the per-variable zeros from the row count.
            assertTrue(statistics.getColumnStatistics().isEmpty());
            assertNotEquals(statistics, PartitionStatistics.empty());
        }
    }

    /**
     * The EMPTY sentinel (and any stats-empty instance) keeps meaning UNKNOWN. This is
     * the invariant that makes the two sentinels safe to distinguish by identity.
     */
    @Test
    public void testEmptySentinelStillMeansUnknown()
    {
        for (boolean ndvEnabled : new boolean[] {true, false}) {
            assertEquals(PartitionQuickStats.convertToPartitionStatistics(PartitionQuickStats.EMPTY, ndvEnabled), PartitionStatistics.empty());
            // Any instance with no column stats also converts to UNKNOWN, unchanged from before.
            assertEquals(
                    PartitionQuickStats.convertToPartitionStatistics(new PartitionQuickStats("p1", ImmutableList.of(), 7), ndvEnabled),
                    PartitionStatistics.empty());
        }
        assertNotSame(PartitionQuickStats.EMPTY, PartitionQuickStats.PROVABLY_EMPTY);
    }

    /**
     * API test: both {@code convertToPartitionStatistics} overloads -- including the
     * {@code @Deprecated} single-argument one -- must agree on the new third state.
     */
    @Test
    public void testBothOverloadsAgreeOnProvablyEmpty()
    {
        @SuppressWarnings("deprecation")
        PartitionStatistics viaDeprecatedOverload = PartitionQuickStats.convertToPartitionStatistics(PartitionQuickStats.PROVABLY_EMPTY);

        assertEquals(viaDeprecatedOverload, PartitionQuickStats.convertToPartitionStatistics(PartitionQuickStats.PROVABLY_EMPTY, true));
        assertEquals(viaDeprecatedOverload, PartitionQuickStats.convertToPartitionStatistics(PartitionQuickStats.PROVABLY_EMPTY, false));

        @SuppressWarnings("deprecation")
        PartitionStatistics emptyViaDeprecatedOverload = PartitionQuickStats.convertToPartitionStatistics(PartitionQuickStats.EMPTY);
        assertEquals(emptyViaDeprecatedOverload, PartitionStatistics.empty());
    }

    /**
     * Validation: the zero statistics the provable-zero path emits must pass
     * {@code MetastoreHiveStatisticsProvider#validatePartitionStatistics}, which hard-fails a query
     * with HIVE_CORRUPTED_COLUMN_STATISTICS on any inconsistency.
     */
    @Test
    public void testProvablyEmptyStatisticsPassValidation()
    {
        MetastoreHiveStatisticsProvider.validatePartitionStatistics(
                new SchemaTableName("schema", "table"),
                ImmutableMap.of("p1", PartitionQuickStats.convertToPartitionStatistics(PartitionQuickStats.PROVABLY_EMPTY, true)));
    }

    @Test
    public void testDeprecatedSingleArgOverloadDefaultsToNdvEnabled()
    {
        ColumnQuickStats<Integer> status = new ColumnQuickStats<>("status", Integer.class);
        status.setMinValue(0);
        status.setMaxValue(4);
        status.addToRowCount(1000L);
        status.addToNullsCount(0L);

        PartitionQuickStats partitionQuickStats = new PartitionQuickStats("p1", ImmutableList.of(status), 1);

        @SuppressWarnings("deprecation")
        PartitionStatistics viaDeprecatedOverload = PartitionQuickStats.convertToPartitionStatistics(partitionQuickStats);
        PartitionStatistics viaExplicitEnabled = PartitionQuickStats.convertToPartitionStatistics(partitionQuickStats, true);

        assertEquals(viaDeprecatedOverload, viaExplicitEnabled);
    }
}
