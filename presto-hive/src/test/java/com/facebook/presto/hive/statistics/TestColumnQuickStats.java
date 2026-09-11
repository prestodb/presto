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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.util.OptionalLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link ColumnQuickStats#getDistinctValuesCount()} -- the conservative NDV bound
 * introduced to stop join-key NDV from surfacing as NaN to the cost-based optimizer (see
 * PartitionQuickStats#convertToPartitionStatistics and MetastoreHiveStatisticsProvider).
 */
public class TestColumnQuickStats
{
    @Test
    public void testBigintJoinKeyIsBoundedByRange()
    {
        // Mirrors a real-world bigint join key (e.g. order_id): min/max are known and the value
        // range is much smaller than the row count, so the range bound should be the tighter one.
        ColumnQuickStats<Long> orderId = new ColumnQuickStats<>("order_id", Long.class);
        orderId.setMinValue(314L);
        orderId.setMaxValue(98319154L);
        orderId.addToRowCount(559_000_000L);
        orderId.addToNullsCount(0L);

        OptionalLong ndv = orderId.getDistinctValuesCount();
        assertTrue(ndv.isPresent(), "Expected a conservative NDV bound instead of empty/NaN");
        assertEquals(ndv.getAsLong(), 98319154L - 314L + 1L);
        assertTrue(ndv.getAsLong() >= 0);
        assertTrue(ndv.getAsLong() <= orderId.getRowCount() - orderId.getNullsCount());
        assertTrue(ndv.getAsLong() <= orderId.getRowCount());
    }

    @Test
    public void testBigintColumnIsBoundedByNonNullCountWhenRangeIsLarger()
    {
        // When the [min, max] range is wider than the number of non-null rows seen, the row-count
        // bound is the tighter (and only sound) one.
        ColumnQuickStats<Long> id = new ColumnQuickStats<>("id", Long.class);
        id.setMinValue(1L);
        id.setMaxValue(1_000_000_000L);
        id.addToRowCount(100L);
        id.addToNullsCount(10L);

        OptionalLong ndv = id.getDistinctValuesCount();
        assertTrue(ndv.isPresent());
        assertEquals(ndv.getAsLong(), 90L);
    }

    @Test
    public void testIntegerColumnUsesRangeBound()
    {
        ColumnQuickStats<Integer> status = new ColumnQuickStats<>("status", Integer.class);
        status.setMinValue(0);
        status.setMaxValue(4);
        status.addToRowCount(1000L);
        status.addToNullsCount(0L);

        assertEquals(status.getDistinctValuesCount(), OptionalLong.of(5L));
    }

    @Test
    public void testDateColumnUsesEpochDayRangeBound()
    {
        ColumnQuickStats<ChronoLocalDate> orderDate = new ColumnQuickStats<>("orderdate", ChronoLocalDate.class);
        orderDate.setMinValue(LocalDate.parse("2020-01-01"));
        orderDate.setMaxValue(LocalDate.parse("2020-01-05"));
        orderDate.addToRowCount(1_000_000L);
        orderDate.addToNullsCount(0L);

        // 5-day range: 2020-01-01 .. 2020-01-05 inclusive
        assertEquals(orderDate.getDistinctValuesCount(), OptionalLong.of(5L));
    }

    @Test
    public void testBooleanColumnIsBoundedByTwo()
    {
        ColumnQuickStats<Boolean> flag = new ColumnQuickStats<>("flag", Boolean.class);
        flag.addToRowCount(10L);
        flag.addToNullsCount(2L);

        assertEquals(flag.getDistinctValuesCount(), OptionalLong.of(2L));
    }

    @Test
    public void testBooleanColumnWithFewNonNullRowsIsBoundedByNonNullCount()
    {
        ColumnQuickStats<Boolean> flag = new ColumnQuickStats<>("flag", Boolean.class);
        flag.addToRowCount(10L);
        flag.addToNullsCount(9L);

        assertEquals(flag.getDistinctValuesCount(), OptionalLong.of(1L));
    }

    @Test
    public void testDoubleColumnHasNoRangeBoundOnlyRowCountBound()
    {
        ColumnQuickStats<Double> price = new ColumnQuickStats<>("price", Double.class);
        price.setMinValue(1.0);
        price.setMaxValue(1000.0);
        price.addToRowCount(50L);
        price.addToNullsCount(10L);

        assertEquals(price.getDistinctValuesCount(), OptionalLong.of(40L));
    }

    @Test
    public void testFloatColumnHasNoBoundAndRemainsEmpty()
    {
        // FLOAT is not wired through to HiveColumnStatistics at the partition level
        // (PartitionQuickStats#convertToPartitionStatistics has no FLOAT branch), so computing a
        // bound here would be dead code. Kept consistent with that: NDV stays unset for FLOAT even
        // though min/max are known and there are non-null rows.
        ColumnQuickStats<Float> ratio = new ColumnQuickStats<>("ratio", Float.class);
        ratio.setMinValue(0.1f);
        ratio.setMaxValue(0.9f);
        ratio.addToRowCount(20L);
        ratio.addToNullsCount(0L);

        assertFalse(ratio.getDistinctValuesCount().isPresent());
    }

    @Test
    public void testFloatColumnWithZeroNonNullRowsStillReportsZero()
    {
        // The zero-non-null-rows short-circuit is an exact fact, so it applies even to FLOAT.
        ColumnQuickStats<Float> ratio = new ColumnQuickStats<>("ratio", Float.class);
        ratio.addToRowCount(5L);
        ratio.addToNullsCount(5L);

        assertEquals(ratio.getDistinctValuesCount(), OptionalLong.of(0L));
    }

    @Test
    public void testSliceColumnHasNoBoundAndRemainsEmpty()
    {
        // VARCHAR/VARBINARY: no min/max is collected by the quick-stats builder, so behavior is
        // unchanged from before this fix -- NDV stays unset rather than fabricating a bound.
        ColumnQuickStats<Slice> comment = new ColumnQuickStats<>("comment", Slice.class);
        comment.addToRowCount(100L);
        comment.addToNullsCount(0L);
        // Slice type never gets min/max set by the parquet builder, but even if it did, SLICE is
        // explicitly excluded from range-bound computation.
        comment.setMinValue(Slices.utf8Slice("a"));
        comment.setMaxValue(Slices.utf8Slice("z"));

        assertFalse(comment.getDistinctValuesCount().isPresent());
    }

    @Test
    public void testAllNullColumnHasZeroDistinctValues()
    {
        ColumnQuickStats<Long> allNull = new ColumnQuickStats<>("allnull", Long.class);
        allNull.setMinValue(1L);
        allNull.setMaxValue(1L);
        allNull.addToRowCount(10L);
        allNull.addToNullsCount(10L);

        assertEquals(allNull.getDistinctValuesCount(), OptionalLong.of(0L));
    }

    @Test
    public void testZeroRowColumnHasZeroDistinctValues()
    {
        ColumnQuickStats<Long> empty = new ColumnQuickStats<>("empty", Long.class);

        assertEquals(empty.getDistinctValuesCount(), OptionalLong.of(0L));
    }

    @Test
    public void testExtremeLongRangeFallsBackToNonNullCountOnOverflow()
    {
        // min/max span almost the full long range: maxValue - minValue + 1 would overflow a long.
        // The implementation must not throw, and must conservatively fall back to the
        // non-null-count bound rather than propagating an incorrect/overflowed value.
        ColumnQuickStats<Long> extreme = new ColumnQuickStats<>("extreme", Long.class);
        extreme.setMinValue(Long.MIN_VALUE + 5);
        extreme.setMaxValue(Long.MAX_VALUE - 5);
        extreme.addToRowCount(1000L);
        extreme.addToNullsCount(0L);

        assertEquals(extreme.getDistinctValuesCount(), OptionalLong.of(1000L));
    }

    @Test
    public void testMinEqualsMaxYieldsExactlyOneDistinctValue()
    {
        ColumnQuickStats<Long> constant = new ColumnQuickStats<>("constant", Long.class);
        constant.setMinValue(42L);
        constant.setMaxValue(42L);
        constant.addToRowCount(1000L);
        constant.addToNullsCount(0L);

        assertEquals(constant.getDistinctValuesCount(), OptionalLong.of(1L));
    }

    @Test
    public void testDateMinEqualsMaxYieldsExactlyOneDistinctValue()
    {
        ColumnQuickStats<ChronoLocalDate> constant = new ColumnQuickStats<>("constant_date", ChronoLocalDate.class);
        LocalDate day = LocalDate.parse("2020-06-15");
        constant.setMinValue(day);
        constant.setMaxValue(day);
        constant.addToRowCount(500L);
        constant.addToNullsCount(0L);

        assertEquals(constant.getDistinctValuesCount(), OptionalLong.of(1L));
    }

    @Test
    public void testIntegerFullRangeFallsBackToNonNullCountOnOverflow()
    {
        // Integer.MAX_VALUE - Integer.MIN_VALUE + 1, widened to long, does not overflow a long, so
        // this exercises the "range is wider than nonNullCount" path for INTEGER rather than the
        // ArithmeticException path (which is Long/Date-only in practice) -- included for INTEGER
        // overflow-adjacent coverage alongside the dedicated LONG overflow test.
        ColumnQuickStats<Integer> full = new ColumnQuickStats<>("full", Integer.class);
        full.setMinValue(Integer.MIN_VALUE);
        full.setMaxValue(Integer.MAX_VALUE);
        full.addToRowCount(4242L);
        full.addToNullsCount(0L);

        assertEquals(full.getDistinctValuesCount(), OptionalLong.of(4242L));
    }

    @Test
    public void testDateFullRangeFallsBackToNonNullCount()
    {
        // LocalDate.MIN/MAX epoch-day values (~+/-3.65e11) are nowhere near Long.MIN/MAX_VALUE, so
        // maxEpochDay - minEpochDay + 1 (~7.3e11) does not overflow a long here -- this exercises
        // the "range is (vastly) wider than nonNullCount" fallback for the DATE branch specifically
        // (the dedicated overflow test only covers LONG; a genuine long-overflow is not
        // constructible for DATE via real LocalDate values).
        ColumnQuickStats<ChronoLocalDate> extreme = new ColumnQuickStats<>("extreme_date", ChronoLocalDate.class);
        extreme.setMinValue(LocalDate.MIN);
        extreme.setMaxValue(LocalDate.MAX);
        extreme.addToRowCount(1000L);
        extreme.addToNullsCount(0L);

        assertEquals(extreme.getDistinctValuesCount(), OptionalLong.of(1000L));
    }

    @Test
    public void testNonNullRowsWithoutObservedMinMaxReturnsEmptyDefensively()
    {
        // Defensive guard: a typed column (Long) that has non-null rows but for which
        // setMinValue/setMaxValue were never called (should not happen via the parquet builder in
        // practice, since min/max are always set together with rowCount/nullsCount for these
        // types, but the getter must not throw / must not fabricate a bound in that case).
        ColumnQuickStats<Long> noRange = new ColumnQuickStats<>("no_range", Long.class);
        noRange.addToRowCount(100L);
        noRange.addToNullsCount(0L);

        assertFalse(noRange.getDistinctValuesCount().isPresent());
    }

    @Test
    public void testMergeAcrossRowGroupsProducesBoundFromFinalMergedState()
    {
        // Simulate merging stats from two row groups/files for the same column, as
        // ParquetQuickStatsBuilder#processColumnMetadata does: the NDV bound must be derived once
        // from the fully-merged rowCount/nullsCount/min/max, not accumulated incrementally.
        ColumnQuickStats<Long> merged = new ColumnQuickStats<>("id", Long.class);
        // Row group 1
        merged.setMinValue(10L);
        merged.setMaxValue(20L);
        merged.addToRowCount(11L);
        merged.addToNullsCount(0L);
        // Row group 2 (disjoint, wider range; overlapping row count)
        merged.setMinValue(15L);
        merged.setMaxValue(30L);
        merged.addToRowCount(16L);
        merged.addToNullsCount(0L);

        assertEquals(merged.getMinValue(), (Long) 10L);
        assertEquals(merged.getMaxValue(), (Long) 30L);
        assertEquals(merged.getRowCount(), 27L);
        // range = 30 - 10 + 1 = 21, nonNullCount = 27; bound = min(27, 21) = 21
        assertEquals(merged.getDistinctValuesCount(), OptionalLong.of(21L));
    }
}
