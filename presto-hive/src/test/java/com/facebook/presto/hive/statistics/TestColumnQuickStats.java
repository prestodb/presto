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

public class TestColumnQuickStats
{
    @Test
    public void testBigintColumnIsBoundedByNonNullCountWhenRangeIsLarger()
    {
        ColumnQuickStats<Long> id = new ColumnQuickStats<>("id", Long.class);
        id.setMinValue(1L);
        id.setMaxValue(1_000_000_000L);

        assertEquals(id.getDistinctValuesCount(90L), OptionalLong.of(90L));
    }

    @Test
    public void testIntegerColumnUsesRangeBound()
    {
        ColumnQuickStats<Integer> status = new ColumnQuickStats<>("status", Integer.class);
        status.setMinValue(0);
        status.setMaxValue(4);

        assertEquals(status.getDistinctValuesCount(1000L), OptionalLong.of(5L));
    }

    @Test
    public void testDateColumnUsesEpochDayRangeBound()
    {
        ColumnQuickStats<ChronoLocalDate> orderDate = new ColumnQuickStats<>("orderdate", ChronoLocalDate.class);
        orderDate.setMinValue(LocalDate.parse("2020-01-01"));
        orderDate.setMaxValue(LocalDate.parse("2020-01-05"));

        assertEquals(orderDate.getDistinctValuesCount(1_000_000L), OptionalLong.of(5L));
    }

    @Test
    public void testBooleanColumnIsBoundedByTwo()
    {
        ColumnQuickStats<Boolean> flag = new ColumnQuickStats<>("flag", Boolean.class);

        assertEquals(flag.getDistinctValuesCount(8L), OptionalLong.of(2L));
    }

    @Test
    public void testSliceColumnHasNoBoundAndRemainsEmpty()
    {
        ColumnQuickStats<Slice> comment = new ColumnQuickStats<>("comment", Slice.class);
        comment.setMinValue(Slices.utf8Slice("a"));
        comment.setMaxValue(Slices.utf8Slice("z"));

        assertFalse(comment.getDistinctValuesCount(100L).isPresent());
    }

    @Test
    public void testAllNullColumnHasZeroDistinctValues()
    {
        ColumnQuickStats<Long> allNull = new ColumnQuickStats<>("allnull", Long.class);
        allNull.setMinValue(1L);
        allNull.setMaxValue(1L);

        assertEquals(allNull.getDistinctValuesCount(0L), OptionalLong.of(0L));
    }

    @Test
    public void testExtremeLongRangeFallsBackToNonNullCountOnOverflow()
    {
        // maxValue - minValue + 1 overflows a long, so only the non-null count bounds NDV.
        ColumnQuickStats<Long> extreme = new ColumnQuickStats<>("extreme", Long.class);
        extreme.setMinValue(Long.MIN_VALUE + 5);
        extreme.setMaxValue(Long.MAX_VALUE - 5);

        assertEquals(extreme.getDistinctValuesCount(1000L), OptionalLong.of(1000L));
    }
}
