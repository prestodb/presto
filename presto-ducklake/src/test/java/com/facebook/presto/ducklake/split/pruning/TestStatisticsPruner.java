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
package com.facebook.presto.ducklake.split.pruning;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.TimeType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakeFileColumnStats;
import com.facebook.presto.spi.ColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

public class TestStatisticsPruner
{
    private static final long ORDERKEY_COLUMN_ID = 1L;
    private static final long PRICE_COLUMN_ID = 2L;
    private static final long NAME_COLUMN_ID = 3L;
    private static final long ORDERDATE_COLUMN_ID = 4L;
    private static final long TS_COLUMN_ID = 5L;
    private static final long TIME_COLUMN_ID = 6L;

    private static final DuckLakeColumnHandle ORDERKEY_COLUMN =
            DuckLakeColumnHandle.primitiveColumnHandle(ORDERKEY_COLUMN_ID, "o_orderkey", "int64", BigintType.BIGINT);
    private static final DuckLakeColumnHandle PRICE_COLUMN =
            DuckLakeColumnHandle.primitiveColumnHandle(PRICE_COLUMN_ID, "price", "float64", DoubleType.DOUBLE);
    private static final DuckLakeColumnHandle NAME_COLUMN =
            DuckLakeColumnHandle.primitiveColumnHandle(NAME_COLUMN_ID, "name", "varchar", VarcharType.createUnboundedVarcharType());
    private static final DuckLakeColumnHandle ORDERDATE_COLUMN =
            DuckLakeColumnHandle.primitiveColumnHandle(ORDERDATE_COLUMN_ID, "o_orderdate", "date", DateType.DATE);
    private static final DuckLakeColumnHandle TS_COLUMN =
            DuckLakeColumnHandle.primitiveColumnHandle(TS_COLUMN_ID, "ts", "timestamp", TimestampType.TIMESTAMP);
    private static final DuckLakeColumnHandle TIME_COLUMN =
            DuckLakeColumnHandle.primitiveColumnHandle(TIME_COLUMN_ID, "t", "time", TimeType.TIME);

    @Test
    public void testOrderkeyLessThanKeepsOnlyOverlappingFile()
    {
        List<DuckLakeDataFile> files = ordersFixtureFiles();
        TupleDomain<ColumnHandle> predicate = orderkeyDomain(Range.lessThan(BigintType.BIGINT, 10L));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(files, columnsById(ORDERKEY_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testOrderkeyBetweenKeepsBothFiles()
    {
        List<DuckLakeDataFile> files = ordersFixtureFiles();
        TupleDomain<ColumnHandle> predicate = orderkeyDomain(Range.range(BigintType.BIGINT, 100L, true, 200L, true));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(files, columnsById(ORDERKEY_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L, 2L));
    }

    @Test
    public void testOrderkeyGreaterThanKeepsNoFiles()
    {
        List<DuckLakeDataFile> files = ordersFixtureFiles();
        TupleDomain<ColumnHandle> predicate = orderkeyDomain(Range.greaterThan(BigintType.BIGINT, 70000L));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(files, columnsById(ORDERKEY_COLUMN), predicate);

        assertEquals(kept.size(), 0);
    }

    @Test
    public void testGarbageMinKeepsFile()
    {
        DuckLakeDataFile file = dataFile(1, 100L, ImmutableMap.of(
                ORDERKEY_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("not-a-number"), Optional.of("60000"), Optional.empty())));
        TupleDomain<ColumnHandle> predicate = orderkeyDomain(Range.lessThan(BigintType.BIGINT, 10L));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(ImmutableList.of(file), columnsById(ORDERKEY_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testGarbageMaxStillPrunesOnMinSide()
    {
        DuckLakeDataFile file = dataFile(1, 100L, ImmutableMap.of(
                ORDERKEY_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("100"), Optional.of("garbage"), Optional.empty())));

        List<DuckLakeDataFile> droppedFor = StatisticsPruner.prune(
                ImmutableList.of(file), columnsById(ORDERKEY_COLUMN), orderkeyDomain(Range.lessThan(BigintType.BIGINT, 10L)));
        assertEquals(droppedFor.size(), 0);

        List<DuckLakeDataFile> keptFor = StatisticsPruner.prune(
                ImmutableList.of(file), columnsById(ORDERKEY_COLUMN), orderkeyDomain(Range.greaterThan(BigintType.BIGINT, 50L)));
        assertEquals(idsOf(keptFor), ImmutableList.of(1L));
    }

    @Test
    public void testContainsNanColumnNeverPruned()
    {
        DuckLakeDataFile file = dataFile(1, 100L, ImmutableMap.of(
                PRICE_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("1.0"), Optional.of("2.0"), Optional.of(true))));
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(PRICE_COLUMN, Domain.create(ValueSet.ofRanges(Range.greaterThan(DoubleType.DOUBLE, 100.0)), false)));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(ImmutableList.of(file), columnsById(PRICE_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testContainsNanAbsentIsTreatedAsUnknownForDouble()
    {
        // The extension only records float/double min/max when contains_nan is known to be
        // false; an absent flag means a NaN row could still be in the file, so it must not be
        // pruned even though the recorded bounds exclude the predicate.
        DuckLakeDataFile file = dataFile(1, 100L, ImmutableMap.of(
                PRICE_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("1.0"), Optional.of("2.0"), Optional.empty())));
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(PRICE_COLUMN, Domain.create(ValueSet.ofRanges(Range.greaterThan(DoubleType.DOUBLE, 100.0)), false)));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(ImmutableList.of(file), columnsById(PRICE_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testContainsNanFalseIsPruned()
    {
        DuckLakeDataFile file = dataFile(1, 100L, ImmutableMap.of(
                PRICE_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("1.0"), Optional.of("2.0"), Optional.of(false))));
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(PRICE_COLUMN, Domain.create(ValueSet.ofRanges(Range.greaterThan(DoubleType.DOUBLE, 100.0)), false)));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(ImmutableList.of(file), columnsById(PRICE_COLUMN), predicate);

        assertEquals(kept.size(), 0);
    }

    @Test
    public void testAllNullColumnDroppedWhenNullNotAllowed()
    {
        DuckLakeDataFile file = dataFile(1, 5L, ImmutableMap.of(
                ORDERKEY_COLUMN_ID, stats(OptionalLong.of(5), Optional.empty(), Optional.empty(), Optional.empty())));
        TupleDomain<ColumnHandle> predicate = orderkeyDomain(Range.equal(BigintType.BIGINT, 7L));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(ImmutableList.of(file), columnsById(ORDERKEY_COLUMN), predicate);

        assertEquals(kept.size(), 0);
    }

    @Test
    public void testAllNullColumnKeptWhenNullAllowed()
    {
        DuckLakeDataFile file = dataFile(1, 5L, ImmutableMap.of(
                ORDERKEY_COLUMN_ID, stats(OptionalLong.of(5), Optional.empty(), Optional.empty(), Optional.empty())));
        Domain domain = Domain.create(ValueSet.ofRanges(Range.equal(BigintType.BIGINT, 7L)), true);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(ORDERKEY_COLUMN, domain));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(ImmutableList.of(file), columnsById(ORDERKEY_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testHalfNullColumnNotDroppedForNullExcludingDomain()
    {
        DuckLakeDataFile file = dataFile(1, 10L, ImmutableMap.of(
                ORDERKEY_COLUMN_ID, stats(OptionalLong.of(5), Optional.empty(), Optional.empty(), Optional.empty())));
        TupleDomain<ColumnHandle> predicate = orderkeyDomain(Range.equal(BigintType.BIGINT, 7L));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(ImmutableList.of(file), columnsById(ORDERKEY_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testVarcharTruncatedMaxKeepsPrefixedValue()
    {
        DuckLakeDataFile file = dataFile(1, 100L, ImmutableMap.of(
                NAME_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("aaa"), Optional.of("abc"), Optional.empty())));

        assertEquals(idsOf(pruneOnName(file, "abcz")), ImmutableList.of(1L));
        assertEquals(pruneOnName(file, "b").size(), 0);
        assertEquals(idsOf(pruneOnName(file, "abc")), ImmutableList.of(1L));
    }

    @Test
    public void testDateRangeAgainstFixtureStats()
    {
        DuckLakeDataFile file = dataFile(1, 100L, ImmutableMap.of(
                ORDERDATE_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("1992-01-01"), Optional.of("1998-08-02"), Optional.empty())));

        Domain lessThan = Domain.create(ValueSet.ofRanges(Range.lessThan(DateType.DATE, LocalDate.of(1992, 1, 1).toEpochDay())), false);
        List<DuckLakeDataFile> droppedFor = StatisticsPruner.prune(
                ImmutableList.of(file), columnsById(ORDERDATE_COLUMN), TupleDomain.withColumnDomains(ImmutableMap.of(ORDERDATE_COLUMN, lessThan)));
        assertEquals(droppedFor.size(), 0);

        Domain lessThanOrEqual = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(DateType.DATE, LocalDate.of(1992, 1, 1).toEpochDay())), false);
        List<DuckLakeDataFile> keptFor = StatisticsPruner.prune(
                ImmutableList.of(file), columnsById(ORDERDATE_COLUMN), TupleDomain.withColumnDomains(ImmutableMap.of(ORDERDATE_COLUMN, lessThanOrEqual)));
        assertEquals(idsOf(keptFor), ImmutableList.of(1L));
    }

    @Test
    public void testTimestampMaxCeilingKeepsBoundaryValue()
    {
        DuckLakeDataFile file = dataFile(1, 100L, ImmutableMap.of(
                TS_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("2024-06-15 00:00:00"), Optional.of("2024-06-15 13:45:30.123456"), Optional.empty())));
        long predicateMillis = toEpochMillis(LocalDateTime.of(2024, 6, 15, 13, 45, 30)) + 124;
        Domain domain = Domain.singleValue(TimestampType.TIMESTAMP, predicateMillis);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(TS_COLUMN, domain));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(ImmutableList.of(file), columnsById(TS_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testColumnWithNoStatsRowKeepsFile()
    {
        DuckLakeDataFile file = dataFile(1, 100L, ImmutableMap.of());
        TupleDomain<ColumnHandle> predicate = orderkeyDomain(Range.equal(BigintType.BIGINT, 7L));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(ImmutableList.of(file), columnsById(ORDERKEY_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testTimeColumnNeverPruned()
    {
        DuckLakeDataFile file = dataFile(1, 100L, ImmutableMap.of(
                TIME_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("13:45:30"), Optional.of("13:45:30"), Optional.empty())));
        Domain domain = Domain.singleValue(TimeType.TIME, 1000L);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(TIME_COLUMN, domain));

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(ImmutableList.of(file), columnsById(TIME_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testAllPredicateReturnsSameInstance()
    {
        List<DuckLakeDataFile> files = ordersFixtureFiles();

        List<DuckLakeDataFile> kept = StatisticsPruner.prune(files, columnsById(ORDERKEY_COLUMN), TupleDomain.all());

        assertSame(kept, files);
    }

    private static List<DuckLakeDataFile> pruneOnName(DuckLakeDataFile file, String value)
    {
        Domain domain = Domain.singleValue(VarcharType.createUnboundedVarcharType(), Slices.utf8Slice(value));
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(NAME_COLUMN, domain));
        return StatisticsPruner.prune(ImmutableList.of(file), columnsById(NAME_COLUMN), predicate);
    }

    /** Two files shaped like the fixture's {@code tpch.orders} table (see spec fixture notes). */
    private static List<DuckLakeDataFile> ordersFixtureFiles()
    {
        DuckLakeDataFile fileA = dataFile(1, 100L, ImmutableMap.of(
                ORDERKEY_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("1"), Optional.of("60000"), Optional.empty())));
        DuckLakeDataFile fileB = dataFile(2, 100L, ImmutableMap.of(
                ORDERKEY_COLUMN_ID, stats(OptionalLong.empty(), Optional.of("100"), Optional.of("60000"), Optional.empty())));
        return ImmutableList.of(fileA, fileB);
    }

    private static TupleDomain<ColumnHandle> orderkeyDomain(Range range)
    {
        Domain domain = Domain.create(ValueSet.ofRanges(range), false);
        return TupleDomain.withColumnDomains(ImmutableMap.of(ORDERKEY_COLUMN, domain));
    }

    private static DuckLakeFileColumnStats stats(OptionalLong nullCount, Optional<String> min, Optional<String> max, Optional<Boolean> containsNan)
    {
        return new DuckLakeFileColumnStats(0L, OptionalLong.empty(), OptionalLong.empty(), nullCount, min, max, containsNan);
    }

    private static DuckLakeDataFile dataFile(long id, long recordCount, Map<Long, DuckLakeFileColumnStats> columnStats)
    {
        return new DuckLakeDataFile(
                id,
                "/data/f" + id + ".parquet",
                "parquet",
                recordCount,
                1024L,
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                columnStats);
    }

    private static Map<Long, DuckLakeColumnHandle> columnsById(DuckLakeColumnHandle... columns)
    {
        ImmutableMap.Builder<Long, DuckLakeColumnHandle> builder = ImmutableMap.builder();
        for (DuckLakeColumnHandle column : columns) {
            builder.put(column.getId(), column);
        }
        return builder.build();
    }

    private static long toEpochMillis(LocalDateTime value)
    {
        return value.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    private static List<Long> idsOf(List<DuckLakeDataFile> files)
    {
        return files.stream().map(DuckLakeDataFile::getDataFileId).collect(toImmutableList());
    }
}
