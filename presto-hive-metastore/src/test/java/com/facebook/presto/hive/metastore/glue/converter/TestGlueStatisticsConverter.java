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
package com.facebook.presto.hive.metastore.glue.converter;

import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createStringColumnStatistics;
import static com.facebook.presto.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestGlueStatisticsConverter
{
    private static final String TEST_CATALOG = "test_catalog";
    private static final String TEST_DATABASE = "test_db";
    private static final String TEST_TABLE = "test_table";
    private static final String TEST_OWNER = "test_owner";

    @Test
    public void testBooleanStatisticsRoundTrip()
    {
        Column column = new Column("bool_col", HiveType.HIVE_BOOLEAN, Optional.empty(), Optional.empty());
        Table table = createTestTable(ImmutableList.of(column));

        HiveColumnStatistics hiveStats = createBooleanColumnStatistics(
                OptionalLong.of(100),  // trueCount
                OptionalLong.of(50),   // falseCount
                OptionalLong.of(10));  // nullsCount

        // Convert Hive -> Glue
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                table,
                ImmutableMap.of("bool_col", hiveStats),
                OptionalLong.of(160));

        assertEquals(glueStats.size(), 1);
        ColumnStatistics glueStat = glueStats.get(0);
        assertEquals(glueStat.columnName(), "bool_col");
        assertEquals(glueStat.statisticsData().type(), ColumnStatisticsType.BOOLEAN);
        assertEquals(glueStat.statisticsData().booleanColumnStatisticsData().numberOfTrues(), 100L);
        assertEquals(glueStat.statisticsData().booleanColumnStatisticsData().numberOfFalses(), 50L);
        assertEquals(glueStat.statisticsData().booleanColumnStatisticsData().numberOfNulls(), 10L);

        // Convert Glue -> Hive
        Map<String, HiveColumnStatistics> convertedBack = GlueStatisticsConverter.fromGlueColumnStatistics(
                glueStats,
                OptionalLong.of(160));

        HiveColumnStatistics reconverted = convertedBack.get("bool_col");
        assertNotNull(reconverted);
        assertTrue(reconverted.getBooleanStatistics().isPresent());
        assertEquals(reconverted.getBooleanStatistics().get().getTrueCount(), OptionalLong.of(100));
        assertEquals(reconverted.getBooleanStatistics().get().getFalseCount(), OptionalLong.of(50));
        assertEquals(reconverted.getNullsCount(), OptionalLong.of(10));
    }

    @Test
    public void testIntegerStatisticsRoundTrip()
    {
        Column column = new Column("int_col", HiveType.HIVE_INT, Optional.empty(), Optional.empty());
        Table table = createTestTable(ImmutableList.of(column));

        HiveColumnStatistics hiveStats = createIntegerColumnStatistics(
                OptionalLong.of(1),      // min
                OptionalLong.of(1000),   // max
                OptionalLong.of(5),      // nullsCount
                OptionalLong.of(100));   // distinctValuesCount

        // Convert Hive -> Glue
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                table,
                ImmutableMap.of("int_col", hiveStats),
                OptionalLong.of(105));

        assertEquals(glueStats.size(), 1);
        ColumnStatistics glueStat = glueStats.get(0);
        assertEquals(glueStat.columnName(), "int_col");
        assertEquals(glueStat.statisticsData().type(), ColumnStatisticsType.LONG);
        assertEquals(glueStat.statisticsData().longColumnStatisticsData().minimumValue(), 1L);
        assertEquals(glueStat.statisticsData().longColumnStatisticsData().maximumValue(), 1000L);
        assertEquals(glueStat.statisticsData().longColumnStatisticsData().numberOfNulls(), 5L);
        assertEquals(glueStat.statisticsData().longColumnStatisticsData().numberOfDistinctValues(), 101L); // +1 for null

        // Convert Glue -> Hive
        Map<String, HiveColumnStatistics> convertedBack = GlueStatisticsConverter.fromGlueColumnStatistics(
                glueStats,
                OptionalLong.of(105));

        HiveColumnStatistics reconverted = convertedBack.get("int_col");
        assertNotNull(reconverted);
        assertTrue(reconverted.getIntegerStatistics().isPresent());
        assertEquals(reconverted.getIntegerStatistics().get().getMin(), OptionalLong.of(1));
        assertEquals(reconverted.getIntegerStatistics().get().getMax(), OptionalLong.of(1000));
        assertEquals(reconverted.getNullsCount(), OptionalLong.of(5));
        assertEquals(reconverted.getDistinctValuesCount(), OptionalLong.of(100));
    }

    @Test
    public void testDoubleStatisticsRoundTrip()
    {
        Column column = new Column("double_col", HiveType.HIVE_DOUBLE, Optional.empty(), Optional.empty());
        Table table = createTestTable(ImmutableList.of(column));

        HiveColumnStatistics hiveStats = createDoubleColumnStatistics(
                OptionalDouble.of(1.5),      // min
                OptionalDouble.of(999.9),    // max
                OptionalLong.of(3),          // nullsCount
                OptionalLong.of(50));        // distinctValuesCount

        // Convert Hive -> Glue
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                table,
                ImmutableMap.of("double_col", hiveStats),
                OptionalLong.of(53));

        assertEquals(glueStats.size(), 1);
        ColumnStatistics glueStat = glueStats.get(0);
        assertEquals(glueStat.columnName(), "double_col");
        assertEquals(glueStat.statisticsData().type(), ColumnStatisticsType.DOUBLE);
        assertEquals(glueStat.statisticsData().doubleColumnStatisticsData().minimumValue(), 1.5);
        assertEquals(glueStat.statisticsData().doubleColumnStatisticsData().maximumValue(), 999.9);
        assertEquals(glueStat.statisticsData().doubleColumnStatisticsData().numberOfNulls(), 3L);

        // Convert Glue -> Hive
        Map<String, HiveColumnStatistics> convertedBack = GlueStatisticsConverter.fromGlueColumnStatistics(
                glueStats,
                OptionalLong.of(53));

        HiveColumnStatistics reconverted = convertedBack.get("double_col");
        assertNotNull(reconverted);
        assertTrue(reconverted.getDoubleStatistics().isPresent());
        assertEquals(reconverted.getDoubleStatistics().get().getMin(), OptionalDouble.of(1.5));
        assertEquals(reconverted.getDoubleStatistics().get().getMax(), OptionalDouble.of(999.9));
        assertEquals(reconverted.getNullsCount(), OptionalLong.of(3));
    }

    @Test
    public void testStringStatisticsRoundTrip()
    {
        Column column = new Column("string_col", HiveType.HIVE_STRING, Optional.empty(), Optional.empty());
        Table table = createTestTable(ImmutableList.of(column));

        HiveColumnStatistics hiveStats = createStringColumnStatistics(
                OptionalLong.of(100),    // maxValueSizeInBytes
                OptionalLong.of(5000),   // totalSizeInBytes
                OptionalLong.of(10),     // nullsCount
                OptionalLong.of(80));    // distinctValuesCount

        // Convert Hive -> Glue
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                table,
                ImmutableMap.of("string_col", hiveStats),
                OptionalLong.of(100));

        assertEquals(glueStats.size(), 1);
        ColumnStatistics glueStat = glueStats.get(0);
        assertEquals(glueStat.columnName(), "string_col");
        assertEquals(glueStat.statisticsData().type(), ColumnStatisticsType.STRING);
        assertEquals(glueStat.statisticsData().stringColumnStatisticsData().maximumLength(), 100L);
        assertEquals(glueStat.statisticsData().stringColumnStatisticsData().numberOfNulls(), 10L);

        // Convert Glue -> Hive
        Map<String, HiveColumnStatistics> convertedBack = GlueStatisticsConverter.fromGlueColumnStatistics(
                glueStats,
                OptionalLong.of(100));

        HiveColumnStatistics reconverted = convertedBack.get("string_col");
        assertNotNull(reconverted);
        assertEquals(reconverted.getMaxValueSizeInBytes(), OptionalLong.of(100));
        assertEquals(reconverted.getNullsCount(), OptionalLong.of(10));
    }

    @Test
    public void testBinaryStatisticsRoundTrip()
    {
        Column column = new Column("binary_col", HiveType.HIVE_BINARY, Optional.empty(), Optional.empty());
        Table table = createTestTable(ImmutableList.of(column));

        HiveColumnStatistics hiveStats = createBinaryColumnStatistics(
                OptionalLong.of(256),    // maxValueSizeInBytes
                OptionalLong.of(10000),  // totalSizeInBytes
                OptionalLong.of(5));     // nullsCount

        // Convert Hive -> Glue
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                table,
                ImmutableMap.of("binary_col", hiveStats),
                OptionalLong.of(100));

        assertEquals(glueStats.size(), 1);
        ColumnStatistics glueStat = glueStats.get(0);
        assertEquals(glueStat.columnName(), "binary_col");
        assertEquals(glueStat.statisticsData().type(), ColumnStatisticsType.BINARY);
        assertEquals(glueStat.statisticsData().binaryColumnStatisticsData().maximumLength(), 256L);
        assertEquals(glueStat.statisticsData().binaryColumnStatisticsData().numberOfNulls(), 5L);

        // Convert Glue -> Hive
        Map<String, HiveColumnStatistics> convertedBack = GlueStatisticsConverter.fromGlueColumnStatistics(
                glueStats,
                OptionalLong.of(100));

        HiveColumnStatistics reconverted = convertedBack.get("binary_col");
        assertNotNull(reconverted);
        assertEquals(reconverted.getMaxValueSizeInBytes(), OptionalLong.of(256));
        assertEquals(reconverted.getNullsCount(), OptionalLong.of(5));
    }

    @Test
    public void testDateStatisticsRoundTrip()
    {
        Column column = new Column("date_col", HiveType.HIVE_DATE, Optional.empty(), Optional.empty());
        Table table = createTestTable(ImmutableList.of(column));

        LocalDate minDate = LocalDate.of(2020, 1, 1);
        LocalDate maxDate = LocalDate.of(2023, 12, 31);

        HiveColumnStatistics hiveStats = createDateColumnStatistics(
                Optional.of(minDate),
                Optional.of(maxDate),
                OptionalLong.of(2),      // nullsCount
                OptionalLong.of(100));   // distinctValuesCount

        // Convert Hive -> Glue
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                table,
                ImmutableMap.of("date_col", hiveStats),
                OptionalLong.of(102));

        assertEquals(glueStats.size(), 1);
        ColumnStatistics glueStat = glueStats.get(0);
        assertEquals(glueStat.columnName(), "date_col");
        assertEquals(glueStat.statisticsData().type(), ColumnStatisticsType.DATE);
        assertNotNull(glueStat.statisticsData().dateColumnStatisticsData().minimumValue());
        assertNotNull(glueStat.statisticsData().dateColumnStatisticsData().maximumValue());
        assertEquals(glueStat.statisticsData().dateColumnStatisticsData().numberOfNulls(), 2L);

        // Convert Glue -> Hive
        Map<String, HiveColumnStatistics> convertedBack = GlueStatisticsConverter.fromGlueColumnStatistics(
                glueStats,
                OptionalLong.of(102));

        HiveColumnStatistics reconverted = convertedBack.get("date_col");
        assertNotNull(reconverted);
        assertTrue(reconverted.getDateStatistics().isPresent());
        assertEquals(reconverted.getDateStatistics().get().getMin(), Optional.of(minDate));
        assertEquals(reconverted.getDateStatistics().get().getMax(), Optional.of(maxDate));
        assertEquals(reconverted.getNullsCount(), OptionalLong.of(2));
    }

    @Test
    public void testDecimalStatisticsRoundTrip()
    {
        Column column = new Column("decimal_col", HiveType.valueOf("decimal(10,2)"), Optional.empty(), Optional.empty());
        Table table = createTestTable(ImmutableList.of(column));

        BigDecimal min = new BigDecimal("10.50");
        BigDecimal max = new BigDecimal("999.99");

        HiveColumnStatistics hiveStats = createDecimalColumnStatistics(
                Optional.of(min),
                Optional.of(max),
                OptionalLong.of(1),      // nullsCount
                OptionalLong.of(50));    // distinctValuesCount

        // Convert Hive -> Glue
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                table,
                ImmutableMap.of("decimal_col", hiveStats),
                OptionalLong.of(51));

        assertEquals(glueStats.size(), 1);
        ColumnStatistics glueStat = glueStats.get(0);
        assertEquals(glueStat.columnName(), "decimal_col");
        assertEquals(glueStat.statisticsData().type(), ColumnStatisticsType.DECIMAL);
        assertNotNull(glueStat.statisticsData().decimalColumnStatisticsData().minimumValue());
        assertNotNull(glueStat.statisticsData().decimalColumnStatisticsData().maximumValue());
        assertEquals(glueStat.statisticsData().decimalColumnStatisticsData().numberOfNulls(), 1L);

        // Convert Glue -> Hive
        Map<String, HiveColumnStatistics> convertedBack = GlueStatisticsConverter.fromGlueColumnStatistics(
                glueStats,
                OptionalLong.of(51));

        HiveColumnStatistics reconverted = convertedBack.get("decimal_col");
        assertNotNull(reconverted);
        assertTrue(reconverted.getDecimalStatistics().isPresent());
        assertEquals(reconverted.getDecimalStatistics().get().getMin().get().compareTo(min), 0);
        assertEquals(reconverted.getDecimalStatistics().get().getMax().get().compareTo(max), 0);
        assertEquals(reconverted.getNullsCount(), OptionalLong.of(1));
    }

    @Test
    public void testMultipleColumnsConversion()
    {
        Column intCol = new Column("int_col", HiveType.HIVE_INT, Optional.empty(), Optional.empty());
        Column stringCol = new Column("string_col", HiveType.HIVE_STRING, Optional.empty(), Optional.empty());
        Column boolCol = new Column("bool_col", HiveType.HIVE_BOOLEAN, Optional.empty(), Optional.empty());

        Table table = createTestTable(ImmutableList.of(intCol, stringCol, boolCol));

        Map<String, HiveColumnStatistics> hiveStats = ImmutableMap.of(
                "int_col", createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(100), OptionalLong.of(0), OptionalLong.of(100)),
                "string_col", createStringColumnStatistics(OptionalLong.of(50), OptionalLong.of(1000), OptionalLong.of(5), OptionalLong.of(80)),
                "bool_col", createBooleanColumnStatistics(OptionalLong.of(60), OptionalLong.of(40), OptionalLong.of(0)));

        // Convert Hive -> Glue
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                table,
                hiveStats,
                OptionalLong.of(100));

        assertEquals(glueStats.size(), 3);

        // Convert Glue -> Hive
        Map<String, HiveColumnStatistics> convertedBack = GlueStatisticsConverter.fromGlueColumnStatistics(
                glueStats,
                OptionalLong.of(100));

        assertEquals(convertedBack.size(), 3);
        assertTrue(convertedBack.containsKey("int_col"));
        assertTrue(convertedBack.containsKey("string_col"));
        assertTrue(convertedBack.containsKey("bool_col"));
    }

    @Test
    public void testPartitionStatisticsConversion()
    {
        Column column = new Column("int_col", HiveType.HIVE_INT, Optional.empty(), Optional.empty());
        Partition partition = createTestPartition(ImmutableList.of(column));

        HiveColumnStatistics hiveStats = createIntegerColumnStatistics(
                OptionalLong.of(1),
                OptionalLong.of(100),
                OptionalLong.of(5),
                OptionalLong.of(95));

        // Convert Hive -> Glue for partition
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                partition,
                ImmutableMap.of("int_col", hiveStats),
                OptionalLong.of(100));

        assertEquals(glueStats.size(), 1);
        assertEquals(glueStats.get(0).columnName(), "int_col");

        // Convert Glue -> Hive for partition
        Map<String, HiveColumnStatistics> convertedBack = GlueStatisticsConverter.fromGlueColumnStatisticsForPartition(
                partition,
                glueStats);

        assertNotNull(convertedBack.get("int_col"));
    }

    @Test
    public void testEmptyStatistics()
    {
        Column column = new Column("empty_col", HiveType.HIVE_INT, Optional.empty(), Optional.empty());
        Table table = createTestTable(ImmutableList.of(column));

        // Convert empty statistics
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                table,
                ImmutableMap.of(),
                OptionalLong.empty());

        assertTrue(glueStats.isEmpty());

        // Convert back empty statistics
        Map<String, HiveColumnStatistics> convertedBack = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(),
                OptionalLong.empty());

        assertTrue(convertedBack.isEmpty());
    }

    @Test
    public void testStatisticsWithoutRowCount()
    {
        Column column = new Column("int_col", HiveType.HIVE_INT, Optional.empty(), Optional.empty());
        Table table = createTestTable(ImmutableList.of(column));

        HiveColumnStatistics hiveStats = createIntegerColumnStatistics(
                OptionalLong.of(1),
                OptionalLong.of(100),
                OptionalLong.of(0),
                OptionalLong.of(100));

        // Convert without row count
        List<ColumnStatistics> glueStats = GlueStatisticsConverter.toGlueColumnStatistics(
                table,
                ImmutableMap.of("int_col", hiveStats),
                OptionalLong.empty());

        assertEquals(glueStats.size(), 1);
        assertNotNull(glueStats.get(0));
    }

    private Table createTestTable(List<Column> columns)
    {
        return Table.builder()
                .setDatabaseName(TEST_DATABASE)
                .setTableName(TEST_TABLE)
                .setOwner(TEST_OWNER)
                .setTableType(PrestoTableType.valueOf("MANAGED_TABLE"))
                .setDataColumns(columns)
                .setPartitionColumns(ImmutableList.of())
                .setParameters(ImmutableMap.of())
                .withStorage(storage -> storage
                        .setStorageFormat(fromHiveStorageFormat(HiveStorageFormat.ORC))
                        .setLocation("/test/location")
                        .setSerdeParameters(ImmutableMap.of(SERIALIZATION_LIB, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
                .build();
    }

    private Partition createTestPartition(List<Column> columns)
    {
        return Partition.builder()
                .setCatalogName(Optional.of(TEST_CATALOG))
                .setDatabaseName(TEST_DATABASE)
                .setTableName(TEST_TABLE)
                .setColumns(columns)
                .setValues(ImmutableList.of("partition_value"))
                .setParameters(ImmutableMap.of())
                .withStorage(storage -> storage
                        .setStorageFormat(fromHiveStorageFormat(HiveStorageFormat.ORC))
                        .setLocation("/test/location")
                        .setSerdeParameters(ImmutableMap.of(SERIALIZATION_LIB, "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")))
                .build();
    }
}
