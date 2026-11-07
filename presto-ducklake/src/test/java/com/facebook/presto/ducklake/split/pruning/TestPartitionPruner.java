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
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TimestampWithTimeZoneType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
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

public class TestPartitionPruner
{
    private static final long TS_COLUMN_ID = 10L;
    private static final long ID_COLUMN_ID = 20L;
    private static final long VARCHAR_COLUMN_ID = 30L;
    private static final long DATE_COLUMN_ID = 40L;
    private static final long TIMESTAMPTZ_COLUMN_ID = 50L;

    private static final DuckLakeColumnHandle TS_COLUMN =
            DuckLakeColumnHandle.primitiveColumnHandle(TS_COLUMN_ID, "ts", "timestamp", TimestampType.TIMESTAMP);
    private static final DuckLakeColumnHandle ID_COLUMN =
            DuckLakeColumnHandle.primitiveColumnHandle(ID_COLUMN_ID, "id", "int32", IntegerType.INTEGER);
    private static final DuckLakeColumnHandle VARCHAR_COLUMN =
            DuckLakeColumnHandle.primitiveColumnHandle(VARCHAR_COLUMN_ID, "s", "varchar", VarcharType.createUnboundedVarcharType());
    private static final DuckLakeColumnHandle DATE_COLUMN =
            DuckLakeColumnHandle.primitiveColumnHandle(DATE_COLUMN_ID, "d", "date", DateType.DATE);
    private static final DuckLakeColumnHandle TIMESTAMPTZ_COLUMN = DuckLakeColumnHandle.primitiveColumnHandle(
            TIMESTAMPTZ_COLUMN_ID, "tstz", "timestamptz", TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);

    private static final List<DuckLakePartitionField> BY_MONTH_FIELDS = ImmutableList.of(
            new DuckLakePartitionField(0, TS_COLUMN_ID, "month"),
            new DuckLakePartitionField(1, TS_COLUMN_ID, "year"));

    @Test
    public void testMonthRangeWithinOneYear()
    {
        List<DuckLakeDataFile> files = byMonthFiles();
        TupleDomain<ColumnHandle> predicate = tsBetween(LocalDateTime.of(2024, 3, 1, 0, 0), LocalDateTime.of(2024, 4, 15, 0, 0));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, BY_MONTH_FIELDS, columnsById(TS_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(3L, 4L));
    }

    @Test
    public void testMonthWrapAcrossYearBoundary()
    {
        List<DuckLakeDataFile> files = byMonthFiles();
        TupleDomain<ColumnHandle> predicate = tsBetween(LocalDateTime.of(2024, 11, 15, 0, 0), LocalDateTime.of(2025, 1, 10, 0, 0));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, BY_MONTH_FIELDS, columnsById(TS_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(11L, 12L, 13L));
    }

    @Test
    public void testUnboundedHighKeepsAllFiles()
    {
        List<DuckLakeDataFile> files = byMonthFiles();
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(TimestampType.TIMESTAMP, toEpochMillis(LocalDateTime.of(2023, 1, 1, 0, 0)))), false);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(TS_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, BY_MONTH_FIELDS, columnsById(TS_COLUMN), predicate);

        assertEquals(kept.size(), 14);
    }

    @Test
    public void testSpanOfAtLeastTwelveMonthsKeepsAllFiles()
    {
        List<DuckLakeDataFile> files = byMonthFiles();
        TupleDomain<ColumnHandle> predicate = tsBetween(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2025, 2, 1, 0, 0));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, BY_MONTH_FIELDS, columnsById(TS_COLUMN), predicate);

        assertEquals(kept.size(), 14);
    }

    @Test
    public void testSingleValueKeepsOneMonth()
    {
        List<DuckLakeDataFile> files = byMonthFiles();
        long value = toEpochMillis(LocalDateTime.of(2025, 2, 3, 10, 0));
        Domain domain = Domain.singleValue(TimestampType.TIMESTAMP, value);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(TS_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, BY_MONTH_FIELDS, columnsById(TS_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(14L));
    }

    @Test
    public void testUnboundedLowPrunesByYear()
    {
        List<DuckLakeDataFile> files = byMonthFiles();
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(TimestampType.TIMESTAMP, toEpochMillis(LocalDateTime.of(2025, 1, 1, 0, 0)))), false);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(TS_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, BY_MONTH_FIELDS, columnsById(TS_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(13L, 14L));
    }

    @Test
    public void testUnboundedHighPrunesByYear()
    {
        List<DuckLakeDataFile> files = byMonthFiles();
        Domain domain = Domain.create(ValueSet.ofRanges(Range.lessThan(TimestampType.TIMESTAMP, toEpochMillis(LocalDateTime.of(2024, 2, 1, 0, 0)))), false);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(TS_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, BY_MONTH_FIELDS, columnsById(TS_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L));
    }

    @Test
    public void testUnboundedHighPrunesByYearWithLoneYearField()
    {
        List<DuckLakeDataFile> files = yearOnlyFiles();
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, TS_COLUMN_ID, "year"));
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(TimestampType.TIMESTAMP, toEpochMillis(LocalDateTime.of(2024, 6, 15, 0, 0)))), false);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(TS_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(TS_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(2024L, 2025L));
    }

    @Test
    public void testWideBoundedRangePrunesByYear()
    {
        List<DuckLakeDataFile> files = byMonthFilesWithExtra2023Dec();
        TupleDomain<ColumnHandle> predicate = tsBetween(LocalDateTime.of(2024, 3, 1, 0, 0), LocalDateTime.of(2025, 6, 30, 0, 0));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, BY_MONTH_FIELDS, columnsById(TS_COLUMN), predicate);

        assertEquals(kept.size(), 14);
        assertEquals(idsOf(kept).contains(0L), false);
    }

    @Test
    public void testUnboundedRangeWithoutYearMemberStillAllowsAll()
    {
        List<DuckLakeDataFile> files = byMonthFiles();
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, TS_COLUMN_ID, "month"));
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(TimestampType.TIMESTAMP, toEpochMillis(LocalDateTime.of(2025, 1, 1, 0, 0)))), false);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(TS_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(TS_COLUMN), predicate);

        assertEquals(kept.size(), 14);
    }

    @Test
    public void testIdentityEquality()
    {
        List<DuckLakeDataFile> files = identityIntFiles();
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, ID_COLUMN_ID, "identity"));
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 7L);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(ID_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(ID_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(7L));
    }

    @Test
    public void testIdentityInList()
    {
        List<DuckLakeDataFile> files = identityIntFiles();
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, ID_COLUMN_ID, "identity"));
        Domain domain = Domain.multipleValues(IntegerType.INTEGER, ImmutableList.of(1L, 3L));
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(ID_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(ID_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L, 3L));
    }

    @Test
    public void testIdentityGreaterThan()
    {
        List<DuckLakeDataFile> files = identityIntFiles();
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, ID_COLUMN_ID, "identity"));
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(IntegerType.INTEGER, 7L)), false);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(ID_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(ID_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(8L, 9L));
    }

    @Test
    public void testIdentityVarchar()
    {
        List<DuckLakeDataFile> files = ImmutableList.of(
                dataFile(1, ImmutableMap.of(0, Optional.of("apple"))),
                dataFile(2, ImmutableMap.of(0, Optional.of("banana"))),
                dataFile(3, ImmutableMap.of(0, Optional.of("cherry"))));
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, VARCHAR_COLUMN_ID, "identity"));
        Domain domain = Domain.singleValue(VarcharType.createUnboundedVarcharType(), Slices.utf8Slice("banana"));
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(VARCHAR_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(VARCHAR_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(2L));
    }

    @Test
    public void testIdentityDate()
    {
        List<DuckLakeDataFile> files = ImmutableList.of(
                dataFile(1, ImmutableMap.of(0, Optional.of("2024-01-01"))),
                dataFile(2, ImmutableMap.of(0, Optional.of("2024-06-15"))),
                dataFile(3, ImmutableMap.of(0, Optional.of("2024-12-31"))));
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, DATE_COLUMN_ID, "identity"));
        long low = LocalDate.of(2024, 3, 1).toEpochDay();
        long high = LocalDate.of(2024, 7, 1).toEpochDay();
        Domain domain = Domain.create(ValueSet.ofRanges(Range.range(DateType.DATE, low, true, high, true)), false);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(DATE_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(DATE_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(2L));
    }

    @Test
    public void testUnparseableIdentityValueIsKept()
    {
        List<DuckLakeDataFile> files = ImmutableList.of(dataFile(1, ImmutableMap.of(0, Optional.of("not-a-number"))));
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, ID_COLUMN_ID, "identity"));
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 7L);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(ID_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(ID_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testUnparseableCalendarValueIsKept()
    {
        List<DuckLakeDataFile> files = ImmutableList.of(dataFile(1, ImmutableMap.of(0, Optional.of("not-a-number"))));
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, TS_COLUMN_ID, "month"));
        TupleDomain<ColumnHandle> predicate = tsBetween(LocalDateTime.of(2024, 3, 1, 0, 0), LocalDateTime.of(2024, 4, 15, 0, 0));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(TS_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testNullPartitionValueKeptWhenNullAllowed()
    {
        List<DuckLakeDataFile> files = ImmutableList.of(dataFile(1, ImmutableMap.of(0, Optional.empty())));
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, ID_COLUMN_ID, "identity"));
        Domain domain = Domain.create(ValueSet.of(IntegerType.INTEGER, 7L), true);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(ID_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(ID_COLUMN), predicate);

        assertEquals(idsOf(kept), ImmutableList.of(1L));
    }

    @Test
    public void testNullPartitionValueDroppedWhenNullNotAllowed()
    {
        List<DuckLakeDataFile> files = ImmutableList.of(dataFile(1, ImmutableMap.of(0, Optional.empty())));
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, ID_COLUMN_ID, "identity"));
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 7L);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(ID_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(ID_COLUMN), predicate);

        assertEquals(kept.size(), 0);
    }

    @Test
    public void testBucketNeverPrunes()
    {
        List<DuckLakeDataFile> files = identityIntFiles();
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, ID_COLUMN_ID, "bucket(4)"));
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 7L);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(ID_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(ID_COLUMN), predicate);

        assertEquals(kept.size(), files.size());
    }

    @Test
    public void testEpochMonthNeverPrunes()
    {
        List<DuckLakeDataFile> files = byMonthFiles();
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, TS_COLUMN_ID, "epoch_month"));
        TupleDomain<ColumnHandle> predicate = tsBetween(LocalDateTime.of(2024, 3, 1, 0, 0), LocalDateTime.of(2024, 4, 15, 0, 0));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(TS_COLUMN), predicate);

        assertEquals(kept.size(), files.size());
    }

    @Test
    public void testTimestampTzNeverPrunes()
    {
        List<DuckLakeDataFile> files = ImmutableList.of(
                dataFile(1, ImmutableMap.of(0, Optional.of("3"))),
                dataFile(2, ImmutableMap.of(0, Optional.of("11"))));
        List<DuckLakePartitionField> fields = ImmutableList.of(new DuckLakePartitionField(0, TIMESTAMPTZ_COLUMN_ID, "month"));
        Domain domain = Domain.create(ValueSet.ofRanges(Range.greaterThan(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE, 0L)), false);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(TIMESTAMPTZ_COLUMN, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, fields, columnsById(TIMESTAMPTZ_COLUMN), predicate);

        assertEquals(kept.size(), files.size());
    }

    @Test
    public void testAllPredicateReturnsSameInstance()
    {
        List<DuckLakeDataFile> files = byMonthFiles();

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, BY_MONTH_FIELDS, columnsById(TS_COLUMN), TupleDomain.all());

        assertSame(kept, files);
    }

    @Test
    public void testPredicateOnNonPartitionColumnKeepsAllFiles()
    {
        List<DuckLakeDataFile> files = byMonthFiles();
        DuckLakeColumnHandle otherColumn = DuckLakeColumnHandle.primitiveColumnHandle(999L, "other", "int32", IntegerType.INTEGER);
        Domain domain = Domain.singleValue(IntegerType.INTEGER, 1L);
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(otherColumn, domain));

        List<DuckLakeDataFile> kept = PartitionPruner.prune(files, BY_MONTH_FIELDS, columnsById(TS_COLUMN), predicate);

        assertEquals(kept.size(), files.size());
    }

    private static List<DuckLakeDataFile> byMonthFiles()
    {
        ImmutableList.Builder<DuckLakeDataFile> files = ImmutableList.builder();
        long id = 1;
        for (int month = 1; month <= 12; month++) {
            files.add(dataFile(id++, ImmutableMap.of(0, Optional.of(Integer.toString(month)), 1, Optional.of("2024"))));
        }
        for (int month = 1; month <= 2; month++) {
            files.add(dataFile(id++, ImmutableMap.of(0, Optional.of(Integer.toString(month)), 1, Optional.of("2025"))));
        }
        return files.build();
    }

    /** {@link #byMonthFiles()} plus one extra file for 2023-12, at id 0, outside all 14 months. */
    private static List<DuckLakeDataFile> byMonthFilesWithExtra2023Dec()
    {
        ImmutableList.Builder<DuckLakeDataFile> files = ImmutableList.builder();
        files.add(dataFile(0, ImmutableMap.of(0, Optional.of("12"), 1, Optional.of("2023"))));
        files.addAll(byMonthFiles());
        return files.build();
    }

    /** Files partitioned by a lone {@code year(ts)} field, one file per year, id equal to the year. */
    private static List<DuckLakeDataFile> yearOnlyFiles()
    {
        return ImmutableList.of(
                dataFile(2023, ImmutableMap.of(0, Optional.of("2023"))),
                dataFile(2024, ImmutableMap.of(0, Optional.of("2024"))),
                dataFile(2025, ImmutableMap.of(0, Optional.of("2025"))));
    }

    private static List<DuckLakeDataFile> identityIntFiles()
    {
        ImmutableList.Builder<DuckLakeDataFile> files = ImmutableList.builder();
        for (int value = 0; value <= 9; value++) {
            files.add(dataFile(value, ImmutableMap.of(0, Optional.of(Integer.toString(value)))));
        }
        return files.build();
    }

    private static DuckLakeDataFile dataFile(long id, Map<Integer, Optional<String>> partitionValues)
    {
        return new DuckLakeDataFile(
                id,
                "/data/f" + id + ".parquet",
                "parquet",
                100L,
                1024L,
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                partitionValues,
                ImmutableMap.of());
    }

    private static Map<Long, DuckLakeColumnHandle> columnsById(DuckLakeColumnHandle... columns)
    {
        ImmutableMap.Builder<Long, DuckLakeColumnHandle> builder = ImmutableMap.builder();
        for (DuckLakeColumnHandle column : columns) {
            builder.put(column.getId(), column);
        }
        return builder.build();
    }

    private static TupleDomain<ColumnHandle> tsBetween(LocalDateTime low, LocalDateTime high)
    {
        Domain domain = Domain.create(
                ValueSet.ofRanges(Range.range(TimestampType.TIMESTAMP, toEpochMillis(low), true, toEpochMillis(high), true)),
                false);
        return TupleDomain.withColumnDomains(ImmutableMap.of(TS_COLUMN, domain));
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
