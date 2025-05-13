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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.PartitionStatistics;
import com.facebook.presto.iceberg.ColumnIdentity.TypeCategory;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.statistics.ColumnStatistics;
import com.facebook.presto.spi.statistics.DoubleRange;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.TableStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.BooleanType.createBlockForSingleNonNullValue;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.PARTITION_KEY;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.iceberg.util.StatisticsUtil.SUPPORTED_MERGE_FLAGS;
import static com.facebook.presto.iceberg.util.StatisticsUtil.calculateAndSetTableSize;
import static com.facebook.presto.iceberg.util.StatisticsUtil.combineSelectedAndPredicateColumns;
import static com.facebook.presto.iceberg.util.StatisticsUtil.decodeMergeFlags;
import static com.facebook.presto.iceberg.util.StatisticsUtil.encodeMergeFlags;
import static com.facebook.presto.iceberg.util.StatisticsUtil.mergeHiveStatistics;
import static com.facebook.presto.spi.relation.ConstantExpression.createConstantExpression;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestStatisticsUtil
{
    @Test
    public void testMergeStrategyNone()
    {
        TableStatistics merged = mergeHiveStatistics(generateSingleColumnIcebergStats(), generateSingleColumnHiveStatistics(), EnumSet.noneOf(ColumnStatisticType.class), PartitionSpec.unpartitioned());
        assertEquals(merged.getRowCount(), Estimate.of(1));
        assertEquals(merged.getTotalSize(), Estimate.of(16));
        assertEquals(merged.getColumnStatistics().size(), 2);
        Map<String, ColumnStatistics> columnStats = mapToString(merged);
        ColumnStatistics stats = columnStats.get("testint");
        assertEquals(stats.getDataSize(), Estimate.of(8));
        assertEquals(stats.getDistinctValuesCount(), Estimate.of(1));
        assertEquals(stats.getNullsFraction(), Estimate.of(0.1));
        assertEquals(stats.getRange().get(), new DoubleRange(0.0, 1.0));
        stats = columnStats.get("testvarchar");
        assertEquals(stats.getDataSize(), Estimate.of(8));
        assertEquals(stats.getDistinctValuesCount(), Estimate.of(1));
        assertEquals(stats.getNullsFraction(), Estimate.of(0.25));
    }

    @Test
    public void testMergeStrategyWithPartitioned()
    {
        TableStatistics merged = mergeHiveStatistics(generateSingleColumnIcebergStats(), generateSingleColumnHiveStatistics(), EnumSet.of(NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES),
                PartitionSpec.builderFor(new Schema(Types.NestedField.required(0, "test", Types.IntegerType.get()))).bucket("test", 100).build());
        assertEquals(merged.getRowCount(), Estimate.of(1));
        assertEquals(merged.getTotalSize(), Estimate.unknown());
        assertEquals(merged.getColumnStatistics().size(), 2);
        Map<String, ColumnStatistics> columnStats = mapToString(merged);
        ColumnStatistics stats = columnStats.get("testint");
        assertEquals(stats.getDataSize(), Estimate.of(8));
        assertEquals(stats.getDistinctValuesCount(), Estimate.of(1));
        assertEquals(stats.getNullsFraction(), Estimate.of(0.1));
        assertEquals(stats.getRange().get(), new DoubleRange(0.0, 1.0));
        stats = columnStats.get("testvarchar");
        assertEquals(stats.getDataSize(), Estimate.of(8));
        assertEquals(stats.getDistinctValuesCount(), Estimate.of(1));
        assertEquals(stats.getNullsFraction(), Estimate.of(0.25));
    }

    @Test
    public void testMergeStrategyNDVs()
    {
        TableStatistics merged = mergeHiveStatistics(generateSingleColumnIcebergStats(), generateSingleColumnHiveStatistics(), EnumSet.of(NUMBER_OF_DISTINCT_VALUES), PartitionSpec.unpartitioned());
        assertEquals(merged.getRowCount(), Estimate.of(1));
        assertEquals(merged.getTotalSize(), Estimate.of(16));
        assertEquals(merged.getColumnStatistics().size(), 2);
        Map<String, ColumnStatistics> columnStats = mapToString(merged);
        ColumnStatistics stats = columnStats.get("testint");
        assertEquals(stats.getDataSize(), Estimate.of(8));
        assertEquals(stats.getDistinctValuesCount(), Estimate.of(2));
        assertEquals(stats.getNullsFraction(), Estimate.of(0.1));
        assertEquals(stats.getRange().get(), new DoubleRange(0.0, 1.0));
        stats = columnStats.get("testvarchar");
        assertEquals(stats.getDataSize(), Estimate.of(8));
        assertEquals(stats.getDistinctValuesCount(), Estimate.of(2));
        assertEquals(stats.getNullsFraction(), Estimate.of(0.25));
    }

    @Test
    public void testMergeStrategyDataSize()
    {
        TableStatistics merged = mergeHiveStatistics(generateSingleColumnIcebergStats(), generateSingleColumnHiveStatistics(), EnumSet.of(TOTAL_SIZE_IN_BYTES), PartitionSpec.unpartitioned());
        assertEquals(merged.getRowCount(), Estimate.of(1));
        assertEquals(merged.getTotalSize(), Estimate.of(22));
        assertEquals(merged.getColumnStatistics().size(), 2);
        Map<String, ColumnStatistics> columnStats = mapToString(merged);
        ColumnStatistics stats = columnStats.get("testint");
        assertEquals(stats.getDataSize(), Estimate.of(8));
        assertEquals(stats.getDistinctValuesCount(), Estimate.of(1));
        assertEquals(stats.getNullsFraction(), Estimate.of(0.1));
        assertEquals(stats.getRange().get(), new DoubleRange(0.0, 1.0));
        stats = columnStats.get("testvarchar");
        assertEquals(stats.getDataSize(), Estimate.of(14));
        assertEquals(stats.getDistinctValuesCount(), Estimate.of(1));
        assertEquals(stats.getNullsFraction(), Estimate.of(0.25));
    }

    @Test
    public void testMergeStrategyNDVsAndNulls()
    {
        TableStatistics merged = mergeHiveStatistics(generateSingleColumnIcebergStats(), generateSingleColumnHiveStatistics(), EnumSet.of(NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES), PartitionSpec.unpartitioned());
        assertEquals(merged.getRowCount(), Estimate.of(1));
        assertEquals(merged.getTotalSize(), Estimate.of(22));
        assertEquals(merged.getColumnStatistics().size(), 2);
        Map<String, ColumnStatistics> columnStats = mapToString(merged);
        ColumnStatistics stats = columnStats.get("testint");
        assertEquals(stats.getDataSize(), Estimate.of(8));
        assertEquals(stats.getDistinctValuesCount(), Estimate.of(2));
        assertEquals(stats.getNullsFraction(), Estimate.of(0.1));
        assertEquals(stats.getRange().get(), new DoubleRange(0.0, 1.0));
        stats = columnStats.get("testvarchar");
        assertEquals(stats.getDataSize(), Estimate.of(14));
        assertEquals(stats.getDistinctValuesCount(), Estimate.of(2));
        assertEquals(stats.getNullsFraction(), Estimate.of(0.25));
    }

    @Test
    public void testEncodeDecode()
    {
        assertTrue(decodeMergeFlags("").isEmpty());
        assertEquals(decodeMergeFlags(NUMBER_OF_DISTINCT_VALUES.name()), EnumSet.of(NUMBER_OF_DISTINCT_VALUES));
        assertEquals(decodeMergeFlags(NUMBER_OF_DISTINCT_VALUES + "," + TOTAL_SIZE_IN_BYTES),
                EnumSet.of(NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES));
        assertEquals(decodeMergeFlags(NUMBER_OF_DISTINCT_VALUES + "," + TOTAL_SIZE_IN_BYTES + "," + TOTAL_SIZE_IN_BYTES),
                EnumSet.of(NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES));

        assertEquals(encodeMergeFlags(EnumSet.noneOf(ColumnStatisticType.class)), "");
        assertEquals(encodeMergeFlags(EnumSet.of(NUMBER_OF_DISTINCT_VALUES)), NUMBER_OF_DISTINCT_VALUES.name());
        assertEquals(encodeMergeFlags(EnumSet.of(NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES)),
                NUMBER_OF_DISTINCT_VALUES.name() + "," + TOTAL_SIZE_IN_BYTES.name());
        assertEquals(encodeMergeFlags(EnumSet.of(NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES)),
                NUMBER_OF_DISTINCT_VALUES.name() + "," + TOTAL_SIZE_IN_BYTES.name());

        EnumSet<ColumnStatisticType> invalidFlags = EnumSet.allOf(ColumnStatisticType.class);
        SUPPORTED_MERGE_FLAGS.forEach(invalidFlags::remove);
        // throw on encode
        invalidFlags.forEach(flag -> assertThrows(PrestoException.class, () -> {
            encodeMergeFlags(EnumSet.of(flag));
        }));

        // throw on decode
        invalidFlags.forEach(flag -> assertThrows(PrestoException.class, () -> {
            decodeMergeFlags(flag.toString());
        }));
    }

    @Test
    public void testCalculateTableSize()
    {
        // 1 row, but no column statistics
        assertEquals(calculateAndSetTableSize(TableStatistics.builder()
                .setRowCount(Estimate.of(1.0))).build().getTotalSize(), Estimate.of(0.0));
        // unknown rows
        assertEquals(calculateAndSetTableSize(TableStatistics.builder()
                .setRowCount(Estimate.unknown())).build().getTotalSize(), Estimate.unknown());
        // non-fixed width column with known data size
        assertEquals(calculateAndSetTableSize(TableStatistics.builder()
                .setRowCount(Estimate.of(100.0))
                .setColumnStatistics(handleWithName("c1", VARCHAR), ColumnStatistics.builder()
                        .setDataSize(Estimate.of(100_000))
                        .build()))
                .build()
                .getTotalSize(), Estimate.of(100_000));
        // fixed-width column with some nulls
        assertEquals(calculateAndSetTableSize(TableStatistics.builder()
                .setRowCount(Estimate.of(100.0))
                .setColumnStatistics(handleWithName("c1", INTEGER), ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(0.2))
                        .build()))
                .build()
                .getTotalSize(), Estimate.of(INTEGER.getFixedSize() * 100 * (1 - 0.2)));
        // fixed-width column with all nulls
        assertEquals(calculateAndSetTableSize(TableStatistics.builder()
                .setRowCount(Estimate.of(100.0))
                .setColumnStatistics(handleWithName("c1", INTEGER), ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(1.0))
                        .build()))
                .build()
                .getTotalSize(), Estimate.of(0.0));

        // two columns which are fixed and non-fixed widths added together with some nulls
        assertEquals(calculateAndSetTableSize(TableStatistics.builder()
                .setRowCount(Estimate.of(100.0))
                .setColumnStatistics(handleWithName("c1", INTEGER), ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(0.2))
                        .build())
                .setColumnStatistics(handleWithName("c2", VARCHAR), ColumnStatistics.builder()
                        .setDataSize(Estimate.of(12345))
                        .setNullsFraction(Estimate.of(0.5))
                        .build()))
                .build()
                .getTotalSize(), Estimate.of(12345 + (INTEGER.getFixedSize() * 100 * (1 - 0.2))));

        // two columns which are fixed and non-fixed widths but null fraction is unknown on the fixed-width column
        assertEquals(calculateAndSetTableSize(TableStatistics.builder()
                .setRowCount(Estimate.of(100.0))
                .setColumnStatistics(handleWithName("c1", INTEGER), ColumnStatistics.builder()
                        .setNullsFraction(Estimate.unknown())
                        .build())
                .setColumnStatistics(handleWithName("c2", VARCHAR), ColumnStatistics.builder()
                        .setDataSize(Estimate.of(12345))
                        .setNullsFraction(Estimate.of(0.5))
                        .build()))
                .build()
                .getTotalSize(), Estimate.unknown());
        // two columns which are fixed and non-fixed widths but data size is unknown on non-fixed-width column
        assertEquals(calculateAndSetTableSize(TableStatistics.builder()
                .setRowCount(Estimate.of(100.0))
                .setColumnStatistics(handleWithName("c1", INTEGER), ColumnStatistics.builder()
                        .setNullsFraction(Estimate.of(0.5))
                        .build())
                .setColumnStatistics(handleWithName("c2", VARCHAR), ColumnStatistics.builder()
                        .setDataSize(Estimate.unknown())
                        .setNullsFraction(Estimate.of(0.5))
                        .build()))
                .build()
                .getTotalSize(), Estimate.unknown());
    }

    @Test
    public void testGenerateStatisticColumnSets()
    {
        IcebergColumnHandle i = generateIcebergColumnHandle("i", false);
        IcebergColumnHandle j = generateIcebergColumnHandle("j", false);
        IcebergColumnHandle k = generateIcebergColumnHandle("k", false);
        IcebergColumnHandle l = generateIcebergColumnHandle("l", false);
        Set<IcebergColumnHandle> regularColumns = ImmutableSet.of(i, j, k, l);
        IcebergColumnHandle x = generateIcebergColumnHandle("x", true);
        IcebergColumnHandle y = generateIcebergColumnHandle("y", true);
        IcebergTableLayoutHandle.Builder builder = new IcebergTableLayoutHandle.Builder()
                .setPartitionColumns(ImmutableList.of(x, y))
                .setRemainingPredicate(createConstantExpression(createBlockForSingleNonNullValue(true), BOOLEAN))
                .setPartitionColumnPredicate(TupleDomain.all())
                .setPartitions(Optional.empty())
                .setDataColumns(ImmutableList.of())
                .setPredicateColumns(ImmutableMap.of())
                .setRequestedColumns(Optional.empty())
                .setTable(new IcebergTableHandle("test", IcebergTableName.from("test"), false, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), ImmutableList.of(), ImmutableList.of()))
                .setDomainPredicate(TupleDomain.all());
        // verify all selected columns are included
        List<IcebergColumnHandle> includedColumns = combineSelectedAndPredicateColumns(
                regularColumns.stream().collect(toImmutableList()),
                Optional.of(builder.build()));
        assertEquals(new HashSet<>(includedColumns), new HashSet<>(regularColumns));

        // verify all selected and predicate columns are included (i, j) are selected, (k, l) are predicate
        builder.setPredicateColumns(ImmutableList.of(i, j).stream().collect(toImmutableMap(IcebergColumnHandle::getName, Function.identity())));
        includedColumns = combineSelectedAndPredicateColumns(ImmutableList.of(k, l), Optional.of(builder.build()));
        assertEquals(new HashSet<>(includedColumns), new HashSet<>(regularColumns));

        // verify all selected, predicate, and partition predicate columns are included when pushdown filter is enabled
        builder.setPredicateColumns(ImmutableList.of(i, j).stream().collect(toImmutableMap(IcebergColumnHandle::getName, Function.identity())))
                .setPushdownFilterEnabled(true)
                .setPartitionColumnPredicate(TupleDomain.withColumnDomains(ImmutableMap.of(x, Domain.notNull(INTEGER))));

        includedColumns = combineSelectedAndPredicateColumns(ImmutableList.of(k, l), Optional.of(builder.build()));
        assertEquals(new HashSet<>(includedColumns), ImmutableSet.of(i, j, k, l, x));

        // verify partition predicate columns are not included when filter pushdown is disabled
        builder.setPushdownFilterEnabled(false)
                .setPartitionColumnPredicate(TupleDomain.withColumnDomains(ImmutableMap.of(x, Domain.notNull(INTEGER), y, Domain.notNull(INTEGER))));
        includedColumns = combineSelectedAndPredicateColumns(ImmutableList.of(k, l), Optional.of(builder.build()));
        assertEquals(new HashSet<>(includedColumns), ImmutableSet.of(i, j, k, l));
    }

    private static IcebergColumnHandle generateIcebergColumnHandle(String name, boolean partitioned)
    {
        return new IcebergColumnHandle(new ColumnIdentity(ThreadLocalRandom.current().nextInt(), name, TypeCategory.PRIMITIVE, ImmutableList.of()),
                INTEGER,
                Optional.empty(),
                partitioned ? PARTITION_KEY : REGULAR,
                ImmutableList.of());
    }

    private static TableStatistics generateSingleColumnIcebergStats()
    {
        return TableStatistics.builder()
                .setRowCount(Estimate.of(1))
                .setTotalSize(Estimate.unknown())
                .setColumnStatistics(new IcebergColumnHandle(
                                new ColumnIdentity(1, "testint", TypeCategory.PRIMITIVE, Collections.emptyList()),
                                INTEGER,
                                Optional.empty(),
                                REGULAR),
                        ColumnStatistics.builder()
                                .setNullsFraction(Estimate.of(0.1))
                                .setRange(new DoubleRange(0.0, 1.0))
                                .setDataSize(Estimate.of(8))
                                .setDistinctValuesCount(Estimate.of(1))
                                .build())
                .setColumnStatistics(new IcebergColumnHandle(
                                new ColumnIdentity(1, "testvarchar", TypeCategory.PRIMITIVE, Collections.emptyList()),
                                VARCHAR,
                                Optional.empty(),
                                REGULAR),
                        ColumnStatistics.builder()
                                .setNullsFraction(Estimate.of(0.25))
                                .setDataSize(Estimate.of(8))
                                .setDistinctValuesCount(Estimate.of(1))
                                .build())
                .build();
    }

    private static IcebergColumnHandle handleWithName(String name, Type type)
    {
        return new IcebergColumnHandle(
                new ColumnIdentity(1, name, TypeCategory.PRIMITIVE, Collections.emptyList()),
                type,
                Optional.empty(),
                REGULAR);
    }

    private static PartitionStatistics generateSingleColumnHiveStatistics()
    {
        return new PartitionStatistics(
                new HiveBasicStatistics(1, 2, 16, 16),
                ImmutableMap.of("testint",
                        HiveColumnStatistics.createIntegerColumnStatistics(
                                OptionalLong.of(1),
                                OptionalLong.of(2),
                                OptionalLong.of(2),
                                OptionalLong.of(2)),
                        "testvarchar",
                        HiveColumnStatistics.createStringColumnStatistics(
                                OptionalLong.of(4L),
                                OptionalLong.of(14L),
                                OptionalLong.of(2),
                                OptionalLong.of(2))));
    }

    private static Map<String, ColumnStatistics> mapToString(TableStatistics statistics)
    {
        return statistics.getColumnStatistics().entrySet().stream().collect(Collectors.toMap(entry -> ((IcebergColumnHandle) entry.getKey()).getName(), Map.Entry::getValue));
    }
}
