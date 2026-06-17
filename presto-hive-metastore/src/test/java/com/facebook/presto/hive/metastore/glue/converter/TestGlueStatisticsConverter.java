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
import software.amazon.awssdk.services.glue.model.BinaryColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.BooleanColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.ColumnStatisticsType;
import software.amazon.awssdk.services.glue.model.DateColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.DecimalNumber;
import software.amazon.awssdk.services.glue.model.DoubleColumnStatisticsData;
import software.amazon.awssdk.services.glue.model.StringColumnStatisticsData;

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

    @Test
    public void testIntegerStatisticsWithMissingMinMax()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("int_col")
                .columnType("int")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.LONG)
                        .longColumnStatisticsData(software.amazon.awssdk.services.glue.model.LongColumnStatisticsData.builder()
                                .minimumValue(null)
                                .maximumValue(null)
                                .numberOfNulls(5L)
                                .numberOfDistinctValues(100L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(105));

        HiveColumnStatistics stats = hiveStats.get("int_col");
        assertNotNull(stats);
        assertTrue(stats.getIntegerStatistics().isPresent());
        assertEquals(stats.getIntegerStatistics().get().getMin(), OptionalLong.empty());
        assertEquals(stats.getIntegerStatistics().get().getMax(), OptionalLong.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.of(5));
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.of(99)); // Adjusted for null as distinct value
    }

    @Test
    public void testIntegerStatisticsWithMissingNullCount()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("int_col")
                .columnType("int")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.LONG)
                        .longColumnStatisticsData(software.amazon.awssdk.services.glue.model.LongColumnStatisticsData.builder()
                                .minimumValue(1L)
                                .maximumValue(1000L)
                                .numberOfNulls(null)
                                .numberOfDistinctValues(100L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("int_col");
        assertNotNull(stats);
        assertTrue(stats.getIntegerStatistics().isPresent());
        assertEquals(stats.getIntegerStatistics().get().getMin(), OptionalLong.of(1));
        assertEquals(stats.getIntegerStatistics().get().getMax(), OptionalLong.of(1000));
        assertEquals(stats.getNullsCount(), OptionalLong.empty());
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.empty()); // Empty because nullsCount is missing
    }

    @Test
    public void testIntegerStatisticsWithMissingNDV()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("int_col")
                .columnType("int")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.LONG)
                        .longColumnStatisticsData(software.amazon.awssdk.services.glue.model.LongColumnStatisticsData.builder()
                                .minimumValue(1L)
                                .maximumValue(1000L)
                                .numberOfNulls(5L)
                                .numberOfDistinctValues(null)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(105));

        HiveColumnStatistics stats = hiveStats.get("int_col");
        assertNotNull(stats);
        assertTrue(stats.getIntegerStatistics().isPresent());
        assertEquals(stats.getIntegerStatistics().get().getMin(), OptionalLong.of(1));
        assertEquals(stats.getIntegerStatistics().get().getMax(), OptionalLong.of(1000));
        assertEquals(stats.getNullsCount(), OptionalLong.of(5));
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.empty());
    }

    @Test
    public void testIntegerStatisticsWithAllFieldsMissing()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("int_col")
                .columnType("int")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.LONG)
                        .longColumnStatisticsData(software.amazon.awssdk.services.glue.model.LongColumnStatisticsData.builder()
                                .minimumValue(null)
                                .maximumValue(null)
                                .numberOfNulls(null)
                                .numberOfDistinctValues(null)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("int_col");
        assertNotNull(stats);
        assertTrue(stats.getIntegerStatistics().isPresent());
        assertEquals(stats.getIntegerStatistics().get().getMin(), OptionalLong.empty());
        assertEquals(stats.getIntegerStatistics().get().getMax(), OptionalLong.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.empty());
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.empty());
    }

    @Test
    public void testDoubleStatisticsWithMissingMinMax()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("double_col")
                .columnType("double")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.DOUBLE)
                        .doubleColumnStatisticsData(DoubleColumnStatisticsData.builder()
                                .minimumValue(null)
                                .maximumValue(null)
                                .numberOfNulls(3L)
                                .numberOfDistinctValues(50L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(53));

        HiveColumnStatistics stats = hiveStats.get("double_col");
        assertNotNull(stats);
        assertTrue(stats.getDoubleStatistics().isPresent());
        assertEquals(stats.getDoubleStatistics().get().getMin(), OptionalDouble.empty());
        assertEquals(stats.getDoubleStatistics().get().getMax(), OptionalDouble.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.of(3));
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.of(49)); // Adjusted for null as distinct value
    }

    @Test
    public void testDoubleStatisticsWithMissingNullCount()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("double_col")
                .columnType("double")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.DOUBLE)
                        .doubleColumnStatisticsData(DoubleColumnStatisticsData.builder()
                                .minimumValue(1.5)
                                .maximumValue(999.9)
                                .numberOfNulls(null)
                                .numberOfDistinctValues(50L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(50));

        HiveColumnStatistics stats = hiveStats.get("double_col");
        assertNotNull(stats);
        assertTrue(stats.getDoubleStatistics().isPresent());
        assertEquals(stats.getDoubleStatistics().get().getMin(), OptionalDouble.of(1.5));
        assertEquals(stats.getDoubleStatistics().get().getMax(), OptionalDouble.of(999.9));
        assertEquals(stats.getNullsCount(), OptionalLong.empty());
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.empty()); // Empty because nullsCount is missing
    }

    @Test
    public void testDoubleStatisticsWithAllFieldsMissing()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("double_col")
                .columnType("double")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.DOUBLE)
                        .doubleColumnStatisticsData(DoubleColumnStatisticsData.builder()
                                .minimumValue(null)
                                .maximumValue(null)
                                .numberOfNulls(null)
                                .numberOfDistinctValues(null)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("double_col");
        assertNotNull(stats);
        assertTrue(stats.getDoubleStatistics().isPresent());
        assertEquals(stats.getDoubleStatistics().get().getMin(), OptionalDouble.empty());
        assertEquals(stats.getDoubleStatistics().get().getMax(), OptionalDouble.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.empty());
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.empty());
    }

    @Test
    public void testStringStatisticsWithMissingMaxLength()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("string_col")
                .columnType("string")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.STRING)
                        .stringColumnStatisticsData(StringColumnStatisticsData.builder()
                                .maximumLength(null)
                                .averageLength(50.0)
                                .numberOfNulls(10L)
                                .numberOfDistinctValues(80L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("string_col");
        assertNotNull(stats);
        assertEquals(stats.getMaxValueSizeInBytes(), OptionalLong.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.of(10));
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.of(79)); // Adjusted for null as distinct value
        assertTrue(stats.getTotalSizeInBytes().isPresent());
    }

    @Test
    public void testStringStatisticsWithMissingAverageLength()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("string_col")
                .columnType("string")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.STRING)
                        .stringColumnStatisticsData(StringColumnStatisticsData.builder()
                                .maximumLength(100L)
                                .averageLength(null)
                                .numberOfNulls(10L)
                                .numberOfDistinctValues(80L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("string_col");
        assertNotNull(stats);
        assertEquals(stats.getMaxValueSizeInBytes(), OptionalLong.of(100));
        assertEquals(stats.getTotalSizeInBytes(), OptionalLong.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.of(10));
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.of(79)); // Adjusted for null as distinct value
    }

    @Test
    public void testStringStatisticsWithMissingNullCountAndNDV()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("string_col")
                .columnType("string")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.STRING)
                        .stringColumnStatisticsData(StringColumnStatisticsData.builder()
                                .maximumLength(100L)
                                .averageLength(50.0)
                                .numberOfNulls(null)
                                .numberOfDistinctValues(null)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("string_col");
        assertNotNull(stats);
        assertEquals(stats.getMaxValueSizeInBytes(), OptionalLong.of(100));
        assertEquals(stats.getTotalSizeInBytes(), OptionalLong.empty()); // Empty because both avg and nullsCount are missing
        assertEquals(stats.getNullsCount(), OptionalLong.empty());
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.empty());
    }

    @Test
    public void testStringStatisticsWithAllFieldsMissing()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("string_col")
                .columnType("string")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.STRING)
                        .stringColumnStatisticsData(StringColumnStatisticsData.builder()
                                .maximumLength(null)
                                .averageLength(null)
                                .numberOfNulls(null)
                                .numberOfDistinctValues(null)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("string_col");
        assertNotNull(stats);
        assertEquals(stats.getMaxValueSizeInBytes(), OptionalLong.empty());
        assertEquals(stats.getTotalSizeInBytes(), OptionalLong.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.empty());
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.empty());
    }

    @Test
    public void testBinaryStatisticsWithMissingMaxLength()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("binary_col")
                .columnType("binary")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.BINARY)
                        .binaryColumnStatisticsData(BinaryColumnStatisticsData.builder()
                                .maximumLength(null)
                                .averageLength(128.0)
                                .numberOfNulls(5L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("binary_col");
        assertNotNull(stats);
        assertEquals(stats.getMaxValueSizeInBytes(), OptionalLong.empty());
        assertTrue(stats.getTotalSizeInBytes().isPresent());
        assertEquals(stats.getNullsCount(), OptionalLong.of(5));
    }

    @Test
    public void testBinaryStatisticsWithMissingAverageLength()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("binary_col")
                .columnType("binary")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.BINARY)
                        .binaryColumnStatisticsData(BinaryColumnStatisticsData.builder()
                                .maximumLength(256L)
                                .averageLength(null)
                                .numberOfNulls(5L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("binary_col");
        assertNotNull(stats);
        assertEquals(stats.getMaxValueSizeInBytes(), OptionalLong.of(256));
        assertEquals(stats.getTotalSizeInBytes(), OptionalLong.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.of(5));
    }

    @Test
    public void testBinaryStatisticsWithAllFieldsMissing()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("binary_col")
                .columnType("binary")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.BINARY)
                        .binaryColumnStatisticsData(BinaryColumnStatisticsData.builder()
                                .maximumLength(null)
                                .averageLength(null)
                                .numberOfNulls(null)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("binary_col");
        assertNotNull(stats);
        assertEquals(stats.getMaxValueSizeInBytes(), OptionalLong.empty());
        assertEquals(stats.getTotalSizeInBytes(), OptionalLong.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.empty());
    }

    @Test
    public void testBooleanStatisticsWithMissingTrueCount()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("bool_col")
                .columnType("boolean")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.BOOLEAN)
                        .booleanColumnStatisticsData(BooleanColumnStatisticsData.builder()
                                .numberOfTrues(null)
                                .numberOfFalses(50L)
                                .numberOfNulls(10L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(60));

        HiveColumnStatistics stats = hiveStats.get("bool_col");
        assertNotNull(stats);
        assertTrue(stats.getBooleanStatistics().isPresent());
        assertEquals(stats.getBooleanStatistics().get().getTrueCount(), OptionalLong.empty());
        assertEquals(stats.getBooleanStatistics().get().getFalseCount(), OptionalLong.of(50));
        assertEquals(stats.getNullsCount(), OptionalLong.of(10));
    }

    @Test
    public void testBooleanStatisticsWithMissingFalseCount()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("bool_col")
                .columnType("boolean")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.BOOLEAN)
                        .booleanColumnStatisticsData(BooleanColumnStatisticsData.builder()
                                .numberOfTrues(100L)
                                .numberOfFalses(null)
                                .numberOfNulls(10L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(110));

        HiveColumnStatistics stats = hiveStats.get("bool_col");
        assertNotNull(stats);
        assertTrue(stats.getBooleanStatistics().isPresent());
        assertEquals(stats.getBooleanStatistics().get().getTrueCount(), OptionalLong.of(100));
        assertEquals(stats.getBooleanStatistics().get().getFalseCount(), OptionalLong.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.of(10));
    }

    @Test
    public void testBooleanStatisticsWithAllFieldsMissing()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("bool_col")
                .columnType("boolean")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.BOOLEAN)
                        .booleanColumnStatisticsData(BooleanColumnStatisticsData.builder()
                                .numberOfTrues(null)
                                .numberOfFalses(null)
                                .numberOfNulls(null)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("bool_col");
        assertNotNull(stats);
        assertTrue(stats.getBooleanStatistics().isPresent());
        assertEquals(stats.getBooleanStatistics().get().getTrueCount(), OptionalLong.empty());
        assertEquals(stats.getBooleanStatistics().get().getFalseCount(), OptionalLong.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.empty());
    }

    @Test
    public void testDateStatisticsWithMissingMinMax()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("date_col")
                .columnType("date")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.DATE)
                        .dateColumnStatisticsData(DateColumnStatisticsData.builder()
                                .minimumValue(null)
                                .maximumValue(null)
                                .numberOfNulls(2L)
                                .numberOfDistinctValues(100L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(102));

        HiveColumnStatistics stats = hiveStats.get("date_col");
        assertNotNull(stats);
        assertTrue(stats.getDateStatistics().isPresent());
        assertEquals(stats.getDateStatistics().get().getMin(), Optional.empty());
        assertEquals(stats.getDateStatistics().get().getMax(), Optional.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.of(2));
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.of(99)); // Adjusted for null as distinct value
    }

    @Test
    public void testDateStatisticsWithAllFieldsMissing()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("date_col")
                .columnType("date")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.DATE)
                        .dateColumnStatisticsData(DateColumnStatisticsData.builder()
                                .minimumValue(null)
                                .maximumValue(null)
                                .numberOfNulls(null)
                                .numberOfDistinctValues(null)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("date_col");
        assertNotNull(stats);
        assertTrue(stats.getDateStatistics().isPresent());
        assertEquals(stats.getDateStatistics().get().getMin(), Optional.empty());
        assertEquals(stats.getDateStatistics().get().getMax(), Optional.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.empty());
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.empty());
    }

    @Test
    public void testDecimalStatisticsWithMissingMinMax()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("decimal_col")
                .columnType("decimal(10,2)")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.DECIMAL)
                        .decimalColumnStatisticsData(DecimalColumnStatisticsData.builder()
                                .minimumValue((DecimalNumber) null)
                                .maximumValue((DecimalNumber) null)
                                .numberOfNulls(1L)
                                .numberOfDistinctValues(50L)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(51));

        HiveColumnStatistics stats = hiveStats.get("decimal_col");
        assertNotNull(stats);
        assertTrue(stats.getDecimalStatistics().isPresent());
        assertEquals(stats.getDecimalStatistics().get().getMin(), Optional.empty());
        assertEquals(stats.getDecimalStatistics().get().getMax(), Optional.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.of(1));
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.of(49)); // Adjusted for null as distinct value
    }

    @Test
    public void testDecimalStatisticsWithAllFieldsMissing()
    {
        ColumnStatistics glueStats = ColumnStatistics.builder()
                .columnName("decimal_col")
                .columnType("decimal(10,2)")
                .statisticsData(ColumnStatisticsData.builder()
                        .type(ColumnStatisticsType.DECIMAL)
                        .decimalColumnStatisticsData(DecimalColumnStatisticsData.builder()
                                .minimumValue((DecimalNumber) null)
                                .maximumValue((DecimalNumber) null)
                                .numberOfNulls(null)
                                .numberOfDistinctValues(null)
                                .build())
                        .build())
                .build();

        Map<String, HiveColumnStatistics> hiveStats = GlueStatisticsConverter.fromGlueColumnStatistics(
                ImmutableList.of(glueStats),
                OptionalLong.of(100));

        HiveColumnStatistics stats = hiveStats.get("decimal_col");
        assertNotNull(stats);
        assertTrue(stats.getDecimalStatistics().isPresent());
        assertEquals(stats.getDecimalStatistics().get().getMin(), Optional.empty());
        assertEquals(stats.getDecimalStatistics().get().getMax(), Optional.empty());
        assertEquals(stats.getNullsCount(), OptionalLong.empty());
        assertEquals(stats.getDistinctValuesCount(), OptionalLong.empty());
    }
}
