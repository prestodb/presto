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
package com.facebook.presto.hive.util;

import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.metastore.BooleanStatistics;
import com.facebook.presto.hive.metastore.DateStatistics;
import com.facebook.presto.hive.metastore.DecimalStatistics;
import com.facebook.presto.hive.metastore.DoubleStatistics;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.IntegerStatistics;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.hive.HiveBasicStatistics.createZeroStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static com.facebook.presto.hive.util.Statistics.ReduceOperator.ADD;
import static com.facebook.presto.hive.util.Statistics.ReduceOperator.SUBTRACT;
import static com.facebook.presto.hive.util.Statistics.merge;
import static com.facebook.presto.hive.util.Statistics.reduce;
import static org.assertj.core.api.Assertions.assertThat;

public class TestStatistics
{
    @Test
    public void testReduce()
    {
        assertThat(reduce(createEmptyStatistics(), createEmptyStatistics(), ADD)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createZeroStatistics(), createEmptyStatistics(), ADD)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createEmptyStatistics(), createZeroStatistics(), ADD)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createEmptyStatistics(), createEmptyStatistics(), SUBTRACT)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createZeroStatistics(), createEmptyStatistics(), SUBTRACT)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(createEmptyStatistics(), createZeroStatistics(), SUBTRACT)).isEqualTo(createEmptyStatistics());
        assertThat(reduce(
                new HiveBasicStatistics(11, 9, 7, 5),
                new HiveBasicStatistics(1, 2, 3, 4), ADD))
                .isEqualTo(new HiveBasicStatistics(12, 11, 10, 9));
        assertThat(reduce(
                new HiveBasicStatistics(11, 9, 7, 5),
                new HiveBasicStatistics(1, 2, 3, 4), SUBTRACT))
                .isEqualTo(new HiveBasicStatistics(10, 7, 4, 1));
    }

    @Test
    public void testMergeEmptyColumnStatistics()
    {
        assertMergeHiveColumnStatistics(HiveColumnStatistics.empty(), OptionalLong.empty(), HiveColumnStatistics.empty(), OptionalLong.empty(), HiveColumnStatistics.empty());
    }

    @Test
    public void testMergeIntegerColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(2))).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(0), OptionalLong.of(3))).build(),
                HiveColumnStatistics.builder().setIntegerStatistics(new IntegerStatistics(OptionalLong.of(0), OptionalLong.of(3))).build());
    }

    @Test
    public void testMergeDoubleColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(1), OptionalDouble.of(2))).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.empty(), OptionalDouble.empty())).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(1), OptionalDouble.of(2))).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(1), OptionalDouble.of(2))).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(0), OptionalDouble.of(3))).build(),
                HiveColumnStatistics.builder().setDoubleStatistics(new DoubleStatistics(OptionalDouble.of(0), OptionalDouble.of(3))).build());
    }

    @Test
    public void testMergeDecimalColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(2)))).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(2)))).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(2)))).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(0)), Optional.of(BigDecimal.valueOf(3)))).build(),
                HiveColumnStatistics.builder().setDecimalStatistics(new DecimalStatistics(Optional.of(BigDecimal.valueOf(0)), Optional.of(BigDecimal.valueOf(3)))).build());
    }

    @Test
    public void testMergeDateColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(2)))).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.empty(), Optional.empty())).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(2)))).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(2)))).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(0)), Optional.of(LocalDate.ofEpochDay(3)))).build(),
                HiveColumnStatistics.builder().setDateStatistics(new DateStatistics(Optional.of(LocalDate.ofEpochDay(0)), Optional.of(LocalDate.ofEpochDay(3)))).build());
    }

    @Test
    public void testMergeBooleanColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.empty(), OptionalLong.empty())).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(1), OptionalLong.of(2))).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(2), OptionalLong.of(3))).build(),
                HiveColumnStatistics.builder().setBooleanStatistics(new BooleanStatistics(OptionalLong.of(3), OptionalLong.of(5))).build());
    }

    @Test
    public void testMergeStringColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setMaxColumnLength(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setMaxColumnLength(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setMaxColumnLength(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setMaxColumnLength(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setMaxColumnLength(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setMaxColumnLength(OptionalLong.of(1)).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setMaxColumnLength(OptionalLong.of(2)).build(),
                HiveColumnStatistics.builder().setMaxColumnLength(OptionalLong.of(3)).build(),
                HiveColumnStatistics.builder().setMaxColumnLength(OptionalLong.of(3)).build());

        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.empty()).build(),
                OptionalLong.empty(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.empty()).build(),
                OptionalLong.empty(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(4)).build(),
                OptionalLong.empty(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(6)).build(),
                OptionalLong.empty(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(4)).build(),
                OptionalLong.empty(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.empty()).build(),
                OptionalLong.empty(),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(4)).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(4)).build(),
                OptionalLong.of(1),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(6)).build(),
                OptionalLong.of(1),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(5)).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(3)).build(),
                OptionalLong.of(1),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(6)).build(),
                OptionalLong.of(3),
                HiveColumnStatistics.builder().setAverageColumnLength(OptionalDouble.of(5.25)).build());
    }

    @Test
    public void testMergeGenericColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.of(2)).build(),
                HiveColumnStatistics.builder().setDistinctValuesCount(OptionalLong.of(2)).build());

        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.empty()).build());
        assertMergeHiveColumnStatistics(
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(1)).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(2)).build(),
                HiveColumnStatistics.builder().setNullsCount(OptionalLong.of(3)).build());
    }

    @Test
    public void testMergeHiveColumnStatisticsMap()
    {
        Map<String, HiveColumnStatistics> first = ImmutableMap.of(
                "column1", createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)),
                "column2", createDoubleColumnStatistics(OptionalDouble.of(2), OptionalDouble.of(3), OptionalLong.of(4), OptionalLong.of(5)),
                "column3", createBinaryColumnStatistics(OptionalLong.of(5), OptionalDouble.of(5), OptionalLong.of(10)),
                "column4", createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3)));
        Map<String, HiveColumnStatistics> second = ImmutableMap.of(
                "column5", createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3), OptionalLong.of(4)),
                "column2", createDoubleColumnStatistics(OptionalDouble.of(1), OptionalDouble.of(4), OptionalLong.of(4), OptionalLong.of(6)),
                "column3", createBinaryColumnStatistics(OptionalLong.of(6), OptionalDouble.of(5), OptionalLong.of(10)),
                "column6", createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.of(2), OptionalLong.of(3)));
        Map<String, HiveColumnStatistics> expected = ImmutableMap.of(
                "column2", createDoubleColumnStatistics(OptionalDouble.of(1), OptionalDouble.of(4), OptionalLong.of(8), OptionalLong.of(6)),
                "column3", createBinaryColumnStatistics(OptionalLong.of(6), OptionalDouble.of(5), OptionalLong.of(20)));
        assertThat(merge(first, OptionalLong.of(5), second, OptionalLong.of(5))).isEqualTo(expected);
        assertThat(merge(ImmutableMap.of(), OptionalLong.empty(), ImmutableMap.of(), OptionalLong.empty())).isEqualTo(ImmutableMap.of());
    }

    private static void assertMergeHiveColumnStatistics(HiveColumnStatistics first, HiveColumnStatistics second, HiveColumnStatistics expected)
    {
        assertMergeHiveColumnStatistics(first, OptionalLong.empty(), second, OptionalLong.empty(), expected);
    }

    private static void assertMergeHiveColumnStatistics(
            HiveColumnStatistics first,
            OptionalLong firstRowCount,
            HiveColumnStatistics second,
            OptionalLong secondRowCount,
            HiveColumnStatistics expected)
    {
        assertThat(merge(first, firstRowCount, second, secondRowCount)).isEqualTo(expected);
        assertThat(merge(second, secondRowCount, first, firstRowCount)).isEqualTo(expected);
    }
}
