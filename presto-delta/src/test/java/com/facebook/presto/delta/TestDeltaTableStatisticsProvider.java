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
package com.facebook.presto.delta;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.delta.kernel.utils.CloseableIterator;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.testing.TestingConnectorSession.SESSION;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link DeltaTableStatisticsProvider}.
 * Drives {@code computeTableStatistics} directly with synthetic file entries — no DeltaClient needed.
 */
public class TestDeltaTableStatisticsProvider
{
    private static final TypeManager TYPE_MANAGER = createTestFunctionAndTypeManager();
    private static final DeltaTableStatisticsProvider PROVIDER = new DeltaTableStatisticsProvider(TYPE_MANAGER);

    private static DeltaTable table(DeltaColumn... columns)
    {
        return new DeltaTable("schema", "t", "s3://b/t", Optional.empty(), Arrays.asList(columns));
    }

    private static DeltaColumn regular(String name, String type)
    {
        return new DeltaColumn(null, null, name, parseTypeSignature(type), true, false);
    }

    private static DeltaColumn partition(String name, String type)
    {
        return new DeltaColumn(null, null, name, parseTypeSignature(type), true, true);
    }

    private static DeltaColumnHandle handle(DeltaColumn col)
    {
        return new DeltaColumnHandle(
                col.getId(),
                col.getPhysicalName(),
                col.getLogicalName(),
                col.getType(),
                col.isPartition() ? PARTITION : REGULAR,
                Optional.empty());
    }

    private static DeltaFileEntry entry(String path, String statsJson, Map<String, String> pv)
    {
        return entry(path, 1024L, statsJson, pv, 0L);
    }

    private static DeltaFileEntry entry(String path, long size, String statsJson, Map<String, String> pv, long deletedRowCount)
    {
        Optional<DeltaJsonFileStatistics> stats = statsJson != null
                ? DeltaJsonFileStatistics.create(statsJson)
                : Optional.empty();
        return new DeltaFileEntry(path, size, 0L, pv, stats, deletedRowCount);
    }

    private static ColumnStatistics colStats(TableStatistics ts, DeltaColumn col)
    {
        DeltaColumnHandle h = handle(col);
        ColumnStatistics cs = ts.getColumnStatistics().get(h);
        assert cs != null : "No stats for column: " + col.getLogicalName();
        return cs;
    }

    @Test
    public void testEmptyFileListReturnsZeroStatistics()
    {
        DeltaColumn id = regular("id", "bigint");
        TableStatistics result = PROVIDER.computeTableStatistics(emptyList(), table(id));

        assertFalse(result.equals(TableStatistics.empty()));
        assertEquals(result.getRowCount(), Estimate.of(0));
        assertEquals(result.getTotalSize(), Estimate.of(0));
        assertEquals(colStats(result, id).getNullsFraction(), Estimate.of(0));
        assertEquals(colStats(result, id).getDistinctValuesCount(), Estimate.of(0));
    }

    @Test
    public void testSingleFileFullStats()
    {
        DeltaColumn id = regular("id", "bigint");
        String statsJson = "{\"numRecords\":5,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":5},\"nullCount\":{\"id\":0}}";
        List<DeltaFileEntry> entries = singletonList(entry("p0.parquet", statsJson, emptyMap()));

        TableStatistics result = PROVIDER.computeTableStatistics(entries, table(id));

        assertEquals(result.getRowCount(), Estimate.of(5));
        assertEquals(result.getTotalSize(), Estimate.of(1024L));
        ColumnStatistics idStats = colStats(result, id);
        assertEquals(idStats.getNullsFraction(), Estimate.of(0.0));
        assertTrue(idStats.getRange().isPresent());
        assertEquals(idStats.getRange().get().getMin(), 1.0);
        assertEquals(idStats.getRange().get().getMax(), 5.0);
    }

    @Test
    public void testMultiFileAggregation()
    {
        DeltaColumn id = regular("id", "bigint");
        String s1 = "{\"numRecords\":3,\"minValues\":{\"id\":10},\"maxValues\":{\"id\":30},\"nullCount\":{\"id\":0}}";
        String s2 = "{\"numRecords\":7,\"minValues\":{\"id\":2},\"maxValues\":{\"id\":50},\"nullCount\":{\"id\":1}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                Arrays.asList(entry("p0.parquet", s1, emptyMap()), entry("p1.parquet", s2, emptyMap())),
                table(id));

        assertEquals(result.getRowCount(), Estimate.of(10));
        assertEquals(result.getTotalSize(), Estimate.of(2048L));
        ColumnStatistics idStats = colStats(result, id);
        assertEquals(idStats.getRange().get().getMin(), 2.0);
        assertEquals(idStats.getRange().get().getMax(), 50.0);
        assertEquals(idStats.getNullsFraction(), Estimate.of(0.1));
    }

    @Test
    public void testFileMissingStatsReturnsEmpty()
    {
        DeltaColumn id = regular("id", "bigint");
        String goodStats = "{\"numRecords\":5,\"nullCount\":{\"id\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                Arrays.asList(
                        entry("p0.parquet", goodStats, emptyMap()),
                        entry("p1.parquet", null, emptyMap())),
                table(id));

        assertEquals(result, TableStatistics.empty());
    }

    @Test
    public void testFileMissingNumRecordsReturnsEmpty()
    {
        DeltaColumn id = regular("id", "bigint");
        String noCount = "{\"minValues\":{\"id\":1},\"nullCount\":{\"id\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                singletonList(entry("p0.parquet", noCount, emptyMap())),
                table(id));

        assertEquals(result, TableStatistics.empty());
    }

    @Test
    public void testMissingNullCountBecomesUnknown()
    {
        DeltaColumn id = regular("id", "bigint");
        String s1 = "{\"numRecords\":3,\"nullCount\":{\"id\":0}}";
        String s2 = "{\"numRecords\":5}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                Arrays.asList(entry("p0.parquet", s1, emptyMap()), entry("p1.parquet", s2, emptyMap())),
                table(id));

        assertEquals(result.getRowCount(), Estimate.of(8));
        assertTrue(colStats(result, id).getNullsFraction().isUnknown());
    }

    @Test
    public void testAllNullColumnNullsFractionIsOne()
    {
        DeltaColumn id = regular("id", "bigint");
        String statsJson = "{\"numRecords\":4,\"nullCount\":{\"id\":4}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                singletonList(entry("p0.parquet", statsJson, emptyMap())),
                table(id));

        assertEquals(result.getRowCount(), Estimate.of(4));
        assertEquals(colStats(result, id).getNullsFraction(), Estimate.of(1.0));
    }

    @Test
    public void testFloatingPointColumnsProduceRange()
    {
        DeltaColumn doubleColumn = regular("d", "double");
        DeltaColumn realColumn = regular("r", "real");
        String statsJson = "{\"numRecords\":5,\"minValues\":{\"d\":1.5,\"r\":0.5},\"maxValues\":{\"d\":9.5,\"r\":2.5},\"nullCount\":{\"d\":0,\"r\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                singletonList(entry("p0.parquet", statsJson, emptyMap())),
                table(doubleColumn, realColumn));

        assertEquals(colStats(result, doubleColumn).getRange(), Optional.of(new DoubleRange(1.5, 9.5)));
        assertEquals(colStats(result, realColumn).getRange(), Optional.of(new DoubleRange(0.5, 2.5)));
    }

    @Test
    public void testOnlyMinPresentProducesHalfOpenRange()
    {
        DeltaColumn id = regular("id", "bigint");
        String statsJson = "{\"numRecords\":4,\"minValues\":{\"id\":10},\"nullCount\":{\"id\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                singletonList(entry("p0.parquet", statsJson, emptyMap())),
                table(id));

        ColumnStatistics idStats = colStats(result, id);
        assertTrue(idStats.getRange().isPresent());
        assertEquals(idStats.getRange().get().getMin(), 10.0);
        assertEquals(idStats.getRange().get().getMax(), Double.POSITIVE_INFINITY);
    }

    @Test
    public void testOnlyMaxPresentProducesHalfOpenRange()
    {
        DeltaColumn id = regular("id", "bigint");
        String statsJson = "{\"numRecords\":4,\"maxValues\":{\"id\":99},\"nullCount\":{\"id\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                singletonList(entry("p0.parquet", statsJson, emptyMap())),
                table(id));

        ColumnStatistics idStats = colStats(result, id);
        assertTrue(idStats.getRange().isPresent());
        assertEquals(idStats.getRange().get().getMin(), Double.NEGATIVE_INFINITY);
        assertEquals(idStats.getRange().get().getMax(), 99.0);
    }

    @Test
    public void testPartitionColumnNDVAndRange()
    {
        DeltaColumn id = regular("id", "bigint");
        DeltaColumn part = partition("part", "integer");
        String stats = "{\"numRecords\":2,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":2},\"nullCount\":{\"id\":0}}";

        List<DeltaFileEntry> entries = Arrays.asList(
                entry("p0.parquet", stats, singletonMap("part", "10")),
                entry("p1.parquet", stats, singletonMap("part", "10")),
                entry("p2.parquet", stats, singletonMap("part", "30")));

        TableStatistics result = PROVIDER.computeTableStatistics(entries, table(id, part));

        assertEquals(result.getRowCount(), Estimate.of(6));
        ColumnStatistics partStats = colStats(result, part);
        assertEquals(partStats.getDistinctValuesCount(), Estimate.of(2));
        assertTrue(partStats.getRange().isPresent());
        assertEquals(partStats.getRange().get().getMin(), 10.0);
        assertEquals(partStats.getRange().get().getMax(), 30.0);
    }

    @Test
    public void testPartitionColumnStringRange()
    {
        DeltaColumn date = partition("date", "varchar");
        String stats = "{\"numRecords\":2}";

        List<DeltaFileEntry> entries = Arrays.asList(
                entry("p0.parquet", stats, singletonMap("date", "2024-01-01")),
                entry("p1.parquet", stats, singletonMap("date", "2024-01-03")),
                entry("p2.parquet", stats, singletonMap("date", "2024-01-02")));

        TableStatistics result = PROVIDER.computeTableStatistics(entries, table(date));

        assertEquals(result.getRowCount(), Estimate.of(6));
        ColumnStatistics dateStats = colStats(result, date);
        assertEquals(dateStats.getDistinctValuesCount(), Estimate.of(3));
        assertTrue(dateStats.getStringRange().isPresent());
        assertEquals(dateStats.getStringRange().get().getMin(), "2024-01-01");
        assertEquals(dateStats.getStringRange().get().getMax(), "2024-01-03");
    }

    @Test
    public void testNullPartitionValueNotCountedAsDistinct()
    {
        DeltaColumn date = partition("date", "varchar");
        String stats = "{\"numRecords\":3}";

        List<DeltaFileEntry> entries = Arrays.asList(
                entry("p0.parquet", stats, emptyMap()),
                entry("p1.parquet", stats, singletonMap("date", "2024-01-01")));

        TableStatistics result = PROVIDER.computeTableStatistics(entries, table(date));

        assertEquals(result.getRowCount(), Estimate.of(6));
        assertEquals(colStats(result, date).getDistinctValuesCount(), Estimate.of(1));
        assertEquals(colStats(result, date).getNullsFraction(), Estimate.of(0.5));
    }

    @Test
    public void testPartitionLookupCaseInsensitive()
    {
        DeltaColumn date = partition("date", "varchar");
        String stats = "{\"numRecords\":2}";

        Map<String, String> pv = singletonMap("Date", "2024-01-01");
        List<DeltaFileEntry> entries = singletonList(entry("p0.parquet", stats, pv));

        TableStatistics result = PROVIDER.computeTableStatistics(entries, table(date));

        assertEquals(colStats(result, date).getDistinctValuesCount(), Estimate.of(1));
        assertEquals(colStats(result, date).getNullsFraction(), Estimate.of(0.0));
    }

    @Test
    public void testRegularColumnDistinctCountIsUnknown()
    {
        DeltaColumn id = regular("id", "bigint");
        String statsJson = "{\"numRecords\":5,\"nullCount\":{\"id\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                singletonList(entry("p0.parquet", statsJson, emptyMap())),
                table(id));

        assertTrue(colStats(result, id).getDistinctValuesCount().isUnknown());
    }

    @Test
    public void testToStatsDoubleIntegerTypes()
    {
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(BIGINT, 42L), Optional.of(42.0));
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(INTEGER, 7), Optional.of(7.0));
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(SMALLINT, 3), Optional.of(3.0));
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(TINYINT, 1), Optional.of(1.0));
    }

    @Test
    public void testToStatsDoubleDoubleType()
    {
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(DOUBLE, 3.14), Optional.of(3.14));
    }

    @Test
    public void testToStatsDoubleRealTypeUsesDoubleValue()
    {
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(REAL, 1.5), Optional.of(1.5));
    }

    @Test
    public void testToStatsDoubleDecimal()
    {
        Type decimalType = createDecimalType(10, 2);
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(decimalType, 123.45), Optional.of(123.45));
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(decimalType, "123.45"), Optional.of(123.45));
    }

    @Test
    public void testToStatsDoubleDate()
    {
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(DATE, 19000), Optional.of(19000.0));
        // epoch day of 2024-01-15 is 19737
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(DATE, "2024-01-15"), Optional.of(19737.0));
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(DATE, "invalid-date"), Optional.empty());
    }

    @Test
    public void testToStatsDoubleInvalidOrNullReturnsEmpty()
    {
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(BIGINT, null), Optional.empty());
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(BIGINT, "not-a-number"), Optional.empty());
        assertEquals(DeltaTableStatisticsProvider.toStatsDouble(DOUBLE, Double.NaN), Optional.empty());
    }

    @Test
    public void testTimestampRangeIsOmitted()
    {
        DeltaColumn col = regular("created_at", "timestamp");
        String s1 = "{\"numRecords\":3,\"minValues\":{\"created_at\":\"2024-01-15T08:00:00Z\"},\"maxValues\":{\"created_at\":\"2024-01-15T10:00:00Z\"},\"nullCount\":{\"created_at\":0}}";
        String s2 = "{\"numRecords\":4,\"minValues\":{\"created_at\":\"2024-01-15T09:00:00Z\"},\"maxValues\":{\"created_at\":\"2024-01-15T12:00:00Z\"},\"nullCount\":{\"created_at\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                Arrays.asList(entry("p0.parquet", s1, emptyMap()), entry("p1.parquet", s2, emptyMap())),
                table(col));

        ColumnStatistics cs = colStats(result, col);
        // Presto CBO never consults timestamp ranges, so timestamp ranges are intentionally omitted
        assertFalse(cs.getRange().isPresent());
    }

    @Test
    public void testStringRangeAggregation()
    {
        DeltaColumn col = regular("name", "varchar");
        String s1 = "{\"numRecords\":3,\"minValues\":{\"name\":\"apple\"},\"maxValues\":{\"name\":\"cherry\"},\"nullCount\":{\"name\":0}}";
        String s2 = "{\"numRecords\":4,\"minValues\":{\"name\":\"banana\"},\"maxValues\":{\"name\":\"date\"},\"nullCount\":{\"name\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                Arrays.asList(entry("p0.parquet", s1, emptyMap()), entry("p1.parquet", s2, emptyMap())),
                table(col));

        ColumnStatistics cs = colStats(result, col);
        assertTrue(cs.getStringRange().isPresent());
        assertEquals(cs.getStringRange().get().getMin(), "apple");
        assertEquals(cs.getStringRange().get().getMax(), "date");
    }

    @Test
    public void testFileMissingMinMaxInvalidatesRange()
    {
        DeltaColumn id = regular("id", "bigint");
        // File 0 has non-null rows and min/max stats
        String s0 = "{\"numRecords\":3,\"minValues\":{\"id\":10},\"maxValues\":{\"id\":20},\"nullCount\":{\"id\":0}}";
        // File 1 has non-null rows but missing min/max stats for "id" column
        String s1 = "{\"numRecords\":3,\"nullCount\":{\"id\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                Arrays.asList(entry("p0.parquet", s0, emptyMap()), entry("p1.parquet", s1, emptyMap())),
                table(id));

        ColumnStatistics cs = colStats(result, id);
        assertFalse(cs.getRange().isPresent());
    }

    @Test
    public void testColumnMappingPhysicalNameUsedForStats()
    {
        DeltaColumn col = new DeltaColumn(1L, "uuid-xyz", "name", parseTypeSignature("bigint"), true, false);
        String statsJson = "{\"numRecords\":3,\"minValues\":{\"uuid-xyz\":100},\"maxValues\":{\"uuid-xyz\":300},\"nullCount\":{\"uuid-xyz\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                singletonList(entry("p0.parquet", statsJson, emptyMap())),
                table(col));

        ColumnStatistics cs = colStats(result, col);
        assertTrue(cs.getRange().isPresent());
        assertEquals(cs.getRange().get().getMin(), 100.0);
        assertEquals(cs.getRange().get().getMax(), 300.0);
        assertEquals(cs.getNullsFraction(), Estimate.of(0.0));
    }

    @Test
    public void testPartitionNdvExceedsThreshold()
    {
        DeltaColumn date = partition("date", "varchar");
        String stats = "{\"numRecords\":1}";
        List<DeltaFileEntry> entries = new ArrayList<>();
        for (int i = 0; i <= DeltaTableStatisticsProvider.MAX_PARTITION_NDV_THRESHOLD; i++) {
            entries.add(entry("p" + i + ".parquet", stats, singletonMap("date", "val-" + i)));
        }
        TableStatistics result = PROVIDER.computeTableStatistics(entries, table(date));
        assertTrue(colStats(result, date).getDistinctValuesCount().isUnknown());
    }

    @Test
    public void testDeletedRowsDeductedFromRowCount()
    {
        DeltaColumn id = regular("id", "bigint");
        String s1 = "{\"numRecords\":10,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":10},\"nullCount\":{\"id\":1}}";
        // 10 physical rows, 3 deleted rows via DV
        DeltaFileEntry f1 = entry("p0.parquet", 1024L, s1, emptyMap(), 3L);

        TableStatistics result = PROVIDER.computeTableStatistics(singletonList(f1), table(id));

        // Live rows = 10 - 3 = 7
        assertEquals(result.getRowCount(), Estimate.of(7));
        // Null count is 1 out of 10 physical rows = 0.1
        assertEquals(colStats(result, id).getNullsFraction(), Estimate.of(0.1));
    }

    @Test
    public void testAllNullFileDoesNotInvalidateBounds()
    {
        DeltaColumn id = regular("id", "bigint");
        // File 0: 5 rows, all non-null, min=1, max=10
        String s0 = "{\"numRecords\":5,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":10},\"nullCount\":{\"id\":0}}";
        // File 1: 5 rows, all null (nullCount=5 >= fileRows=5), legitimately has no min/max
        String s1 = "{\"numRecords\":5,\"nullCount\":{\"id\":5}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                Arrays.asList(entry("p0.parquet", s0, emptyMap()), entry("p1.parquet", s1, emptyMap())),
                table(id));

        assertEquals(result.getRowCount(), Estimate.of(10));
        ColumnStatistics cs = colStats(result, id);
        // Bounds should remain valid from File 0
        assertTrue(cs.getRange().isPresent());
        assertEquals(cs.getRange().get().getMin(), 1.0);
        assertEquals(cs.getRange().get().getMax(), 10.0);
        // Null fraction: (0 + 5) / 10 = 0.5
        assertEquals(cs.getNullsFraction(), Estimate.of(0.5));
    }

    @Test
    public void testPartitionPhysicalNameLookup()
    {
        DeltaColumn partCol = new DeltaColumn(1L, "phys_part_id", "part_name", parseTypeSignature("integer"), true, true);
        String stats = "{\"numRecords\":4}";
        Map<String, String> pv = singletonMap("phys_part_id", "42");
        DeltaFileEntry f0 = entry("p0.parquet", stats, pv);

        TableStatistics result = PROVIDER.computeTableStatistics(singletonList(f0), table(partCol));

        ColumnStatistics cs = colStats(result, partCol);
        assertEquals(cs.getDistinctValuesCount(), Estimate.of(1));
        assertTrue(cs.getRange().isPresent());
        assertEquals(cs.getRange().get().getMin(), 42.0);
        assertEquals(cs.getRange().get().getMax(), 42.0);
    }

    @Test
    public void testStatisticsAreKeyedByRequestedColumnHandles()
    {
        DeltaColumn id = regular("id", "bigint");
        DeltaColumn name = regular("name", "varchar");
        DeltaColumnHandle plannerIdHandle = handle(id);

        String stats = "{\"numRecords\":5,\"minValues\":{\"id\":1,\"name\":\"a\"},\"maxValues\":{\"id\":5,\"name\":\"z\"},\"nullCount\":{\"id\":0,\"name\":0}}";
        List<DeltaFileEntry> entries = singletonList(entry("p0.parquet", stats, emptyMap()));

        List<ColumnHandle> requested = singletonList(plannerIdHandle);
        TableStatistics result = PROVIDER.computeTableStatistics(entries, table(id, name), requested);

        assertTrue(result.getColumnStatistics().containsKey(plannerIdHandle));
        assertEquals(result.getColumnStatistics().size(), 1);
    }

    @Test
    public void testStatisticsAreCachedPerSnapshotAndPartitionConstraint()
    {
        DeltaColumn id = regular("id", "bigint");
        DeltaColumn name = regular("name", "varchar");
        DeltaTable deltaTable = new DeltaTable("schema", "t", "s3://b/t", Optional.of(7L), Arrays.asList(id, name));
        String stats = "{\"numRecords\":5,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":5},\"nullCount\":{\"id\":0,\"name\":0}}";
        AtomicInteger scans = new AtomicInteger();
        DeltaTableStatisticsProvider provider = new DeltaTableStatisticsProvider(
                (session, table, constraint, types) -> {
                    scans.incrementAndGet();
                    return iterator(singletonList(entry("p0.parquet", stats, emptyMap())));
                },
                TYPE_MANAGER,
                new DeltaConfig());

        DeltaTableHandle handle = new DeltaTableHandle("delta", deltaTable);
        Constraint<ColumnHandle> all = Constraint.alwaysTrue();
        TableStatistics first = provider.getTableStatistics(SESSION, handle, singletonList(handle(id)), all);
        TableStatistics repeat = provider.getTableStatistics(SESSION, handle, singletonList(handle(id)), all);

        assertEquals(scans.get(), 1);
        assertEquals(first.getRowCount(), Estimate.of(5));
        assertTrue(first.getColumnStatistics().containsKey(handle(id)));
        assertEquals(repeat, first);

        // a different snapshot is a different key
        DeltaTable newer = new DeltaTable("schema", "t", "s3://b/t", Optional.of(8L), Arrays.asList(id, name));
        provider.getTableStatistics(SESSION, new DeltaTableHandle("delta", newer), singletonList(handle(id)), all);
        assertEquals(scans.get(), 2);
        assertEquals(provider.cacheSize(), 2);
    }

    @Test
    public void testColumnStatisticsAreBuiltIncrementallyAndMerged()
    {
        DeltaColumn id = regular("id", "bigint");
        DeltaColumn name = regular("name", "varchar");
        DeltaTable deltaTable = new DeltaTable("schema", "t", "s3://b/t", Optional.of(7L), Arrays.asList(id, name));
        String stats = "{\"numRecords\":5,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":5},\"nullCount\":{\"id\":0,\"name\":2}}";
        AtomicInteger scans = new AtomicInteger();
        DeltaTableStatisticsProvider provider = new DeltaTableStatisticsProvider(
                (session, table, constraint, types) -> {
                    scans.incrementAndGet();
                    return iterator(singletonList(entry("p0.parquet", stats, emptyMap())));
                },
                TYPE_MANAGER,
                new DeltaConfig());

        DeltaTableHandle handle = new DeltaTableHandle("delta", deltaTable);
        Constraint<ColumnHandle> all = Constraint.alwaysTrue();

        TableStatistics idOnly = provider.getTableStatistics(SESSION, handle, singletonList(handle(id)), all);
        assertEquals(idOnly.getColumnStatistics().keySet(), ImmutableSet.of(handle(id)));
        assertEquals(scans.get(), 1);

        // a new column triggers one more scan, and only that column is computed
        TableStatistics nameOnly = provider.getTableStatistics(SESSION, handle, singletonList(handle(name)), all);
        assertEquals(nameOnly.getColumnStatistics().keySet(), ImmutableSet.of(handle(name)));
        assertEquals(nameOnly.getColumnStatistics().get(handle(name)).getNullsFraction(), Estimate.of(0.4));
        assertEquals(nameOnly.getRowCount(), Estimate.of(5));
        assertEquals(scans.get(), 2);

        // both columns are now merged into the same entry and need no further scan
        TableStatistics both = provider.getTableStatistics(SESSION, handle, Arrays.asList(handle(id), handle(name)), all);
        assertEquals(both.getColumnStatistics().keySet(), ImmutableSet.of(handle(id), handle(name)));
        assertEquals(both.getColumnStatistics().get(handle(id)), idOnly.getColumnStatistics().get(handle(id)));
        assertEquals(both.getColumnStatistics().get(handle(name)), nameOnly.getColumnStatistics().get(handle(name)));
        assertEquals(scans.get(), 2);
        assertEquals(provider.cacheSize(), 1);
    }

    @Test
    public void testIncrementalScanFailureLeavesCachedColumnsUsable()
    {
        DeltaColumn id = regular("id", "bigint");
        DeltaColumn name = regular("name", "varchar");
        DeltaTable deltaTable = new DeltaTable("schema", "t", "s3://b/t", Optional.of(7L), Arrays.asList(id, name));
        String stats = "{\"numRecords\":5,\"nullCount\":{\"id\":0,\"name\":0}}";
        AtomicInteger scans = new AtomicInteger();
        DeltaTableStatisticsProvider provider = new DeltaTableStatisticsProvider(
                (session, table, constraint, types) -> {
                    if (scans.incrementAndGet() == 2) {
                        throw new IllegalStateException("log unreadable");
                    }
                    return iterator(singletonList(entry("p0.parquet", stats, emptyMap())));
                },
                TYPE_MANAGER,
                new DeltaConfig());

        DeltaTableHandle handle = new DeltaTableHandle("delta", deltaTable);
        Constraint<ColumnHandle> all = Constraint.alwaysTrue();
        provider.getTableStatistics(SESSION, handle, singletonList(handle(id)), all);

        // the failed incremental scan degrades to empty without discarding what is cached
        assertEquals(provider.getTableStatistics(SESSION, handle, singletonList(handle(name)), all), TableStatistics.empty());
        assertEquals(provider.cacheSize(), 1);
        assertEquals(provider.getTableStatistics(SESSION, handle, singletonList(handle(id)), all).getRowCount(), Estimate.of(5));
        assertEquals(scans.get(), 2);

        // and the missing column is retried on the next request
        assertTrue(provider.getTableStatistics(SESSION, handle, singletonList(handle(name)), all).getColumnStatistics().containsKey(handle(name)));
        assertEquals(scans.get(), 3);
    }

    @Test
    public void testCacheKeySeparatesTimestampSemanticsAndZone()
    {
        DeltaColumn ts = partition("event_ts", "timestamp");
        DeltaTable deltaTable = new DeltaTable("schema", "t", "s3://b/t", Optional.of(42L), singletonList(ts));
        String stats = "{\"numRecords\":10,\"nullCount\":{}}";
        AtomicInteger scans = new AtomicInteger();
        DeltaTableStatisticsProvider provider = new DeltaTableStatisticsProvider(
                (session, table, constraint, types) -> {
                    scans.incrementAndGet();
                    return iterator(singletonList(entry("p0.parquet", stats, emptyMap())));
                },
                TYPE_MANAGER,
                new DeltaConfig());

        DeltaTableHandle handle = new DeltaTableHandle("delta", deltaTable);
        // the same internal value prunes different files once the session reads the wall clock differently
        Constraint<ColumnHandle> sameValue = new Constraint<>(TupleDomain.withColumnDomains(
                ImmutableMap.<ColumnHandle, Domain>of(handle(ts), Domain.singleValue(TIMESTAMP, 1_704_067_200_000L))));
        ConnectorSession legacyKolkata = timestampSession(true, "Asia/Kolkata");
        ConnectorSession legacyUtc = timestampSession(true, "UTC");
        ConnectorSession nonLegacyKolkata = timestampSession(false, "Asia/Kolkata");
        ConnectorSession nonLegacyUtc = timestampSession(false, "UTC");

        provider.getTableStatistics(legacyKolkata, handle, singletonList(handle(ts)), sameValue);
        provider.getTableStatistics(nonLegacyKolkata, handle, singletonList(handle(ts)), sameValue);
        assertEquals(scans.get(), 2);

        // under legacy semantics the zone matters
        provider.getTableStatistics(legacyUtc, handle, singletonList(handle(ts)), sameValue);
        assertEquals(scans.get(), 3);

        // outside legacy semantics the zone is irrelevant, so those sessions share an entry
        provider.getTableStatistics(nonLegacyUtc, handle, singletonList(handle(ts)), sameValue);
        assertEquals(scans.get(), 3);
        assertEquals(provider.cacheSize(), 3);
    }

    private static ConnectorSession timestampSession(boolean legacyTimestamp, String zone)
    {
        return new TestingConnectorSession(
                "user",
                new ConnectorIdentity("user", Optional.empty(), Optional.empty()),
                Optional.of("test"),
                Optional.empty(),
                TimeZoneKey.getTimeZoneKey(zone),
                Locale.ENGLISH,
                System.currentTimeMillis(),
                ImmutableList.of(),
                ImmutableMap.of(),
                legacyTimestamp,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                ImmutableMap.of());
    }

    @Test
    public void testUncomputableStatisticsDegradeToEmpty()
    {
        DeltaColumn id = regular("id", "bigint");
        DeltaTable deltaTable = new DeltaTable("schema", "t", "s3://b/t", Optional.of(1L), singletonList(id));
        AtomicInteger scans = new AtomicInteger();
        DeltaTableStatisticsProvider provider = new DeltaTableStatisticsProvider(
                (session, table, constraint, types) -> {
                    scans.incrementAndGet();
                    throw new IllegalStateException("log unreadable");
                },
                TYPE_MANAGER,
                new DeltaConfig());

        DeltaTableHandle handle = new DeltaTableHandle("delta", deltaTable);
        assertEquals(provider.getTableStatistics(SESSION, handle, singletonList(handle(id)), Constraint.alwaysTrue()), TableStatistics.empty());
        assertEquals(provider.getTableStatistics(SESSION, handle, singletonList(handle(id)), Constraint.alwaysTrue()), TableStatistics.empty());
        assertEquals(scans.get(), 2);
        assertEquals(provider.cacheSize(), 0);

        // a table handle without a pinned snapshot also degrades to empty statistics
        DeltaTableHandle unpinned = new DeltaTableHandle("delta", table(id));
        assertEquals(provider.getTableStatistics(SESSION, unpinned, singletonList(handle(id)), Constraint.alwaysTrue()), TableStatistics.empty());
    }

    private static CloseableIterator<DeltaFileEntry> iterator(List<DeltaFileEntry> entries)
    {
        Iterator<DeltaFileEntry> delegate = entries.iterator();
        return new CloseableIterator<DeltaFileEntry>()
        {
            @Override
            public boolean hasNext()
            {
                return delegate.hasNext();
            }

            @Override
            public DeltaFileEntry next()
            {
                return delegate.next();
            }

            @Override
            public void close() {}
        };
    }
}
