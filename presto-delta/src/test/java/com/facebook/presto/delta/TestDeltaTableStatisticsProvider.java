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

import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static com.facebook.presto.delta.DeltaColumnHandle.ColumnType.REGULAR;
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
    private static final DeltaTableStatisticsProvider PROVIDER = new DeltaTableStatisticsProvider(null);

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
        Optional<DeltaJsonFileStatistics> stats = statsJson != null
                ? DeltaJsonFileStatistics.create(statsJson)
                : Optional.empty();
        return new DeltaFileEntry(path, 1024L, 0L, pv, stats);
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
    public void testPartitionColumnNDV()
    {
        DeltaColumn id = regular("id", "bigint");
        DeltaColumn date = partition("date", "varchar");
        String stats = "{\"numRecords\":2,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":2},\"nullCount\":{\"id\":0}}";

        List<DeltaFileEntry> entries = Arrays.asList(
                entry("p0.parquet", stats, singletonMap("date", "2024-01-01")),
                entry("p1.parquet", stats, singletonMap("date", "2024-01-01")),
                entry("p2.parquet", stats, singletonMap("date", "2024-01-02")));

        TableStatistics result = PROVIDER.computeTableStatistics(entries, table(id, date));

        assertEquals(result.getRowCount(), Estimate.of(6));
        assertEquals(colStats(result, date).getDistinctValuesCount(), Estimate.of(2));
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
    public void testRegularColumnNdvIsUnknownInPhase1()
    {
        DeltaColumn id = regular("id", "bigint");
        String statsJson = "{\"numRecords\":5,\"nullCount\":{\"id\":0}}";

        TableStatistics result = PROVIDER.computeTableStatistics(
                singletonList(entry("p0.parquet", statsJson, emptyMap())),
                table(id));

        assertTrue(colStats(result, id).getDistinctValuesCount().isUnknown());
    }

    @Test
    public void testToDoubleStatisticIntegerTypes()
    {
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic(42L, parseTypeSignature("bigint")),
                OptionalDouble.of(42.0));
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic(7, parseTypeSignature("integer")),
                OptionalDouble.of(7.0));
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic(3, parseTypeSignature("smallint")),
                OptionalDouble.of(3.0));
    }

    @Test
    public void testToDoubleStatisticDoubleType()
    {
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic(3.14, parseTypeSignature("double")),
                OptionalDouble.of(3.14));
    }

    @Test
    public void testToDoubleStatisticRealTypeUsesDoubleValue()
    {
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic(1.5, parseTypeSignature("real")),
                OptionalDouble.of(1.5));
    }

    @Test
    public void testToDoubleStatisticDecimal()
    {
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic(12345, parseTypeSignature("decimal(10,2)")),
                OptionalDouble.of(123.45));
    }

    @Test
    public void testToDoubleStatisticDecimalAsString()
    {
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic("123.45", parseTypeSignature("decimal(10,2)")),
                OptionalDouble.of(123.45));
    }

    @Test
    public void testToDoubleStatisticDate()
    {
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic(19000, parseTypeSignature("date")),
                OptionalDouble.of(19000.0));
    }

    @Test
    public void testToDoubleStatisticNonNumericTypesReturnEmpty()
    {
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic("hello", parseTypeSignature("varchar")),
                OptionalDouble.empty());
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic(true, parseTypeSignature("boolean")),
                OptionalDouble.empty());
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic("x", parseTypeSignature("varbinary")),
                OptionalDouble.empty());
    }

    @Test
    public void testToDoubleStatisticNullReturnsEmpty()
    {
        assertEquals(DeltaTableStatisticsProvider.toDoubleStatistic(null, parseTypeSignature("bigint")),
                OptionalDouble.empty());
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
}
