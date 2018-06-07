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
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static com.facebook.presto.hive.HiveBasicStatistics.createEmptyStatistics;
import static com.facebook.presto.hive.HiveBasicStatistics.createZeroStatistics;
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
    public void testMergeHiveColumnStatistics()
    {
        assertMergeHiveColumnStatistics(
                new HiveColumnStatistics(
                        Optional.empty(), Optional.empty(), OptionalLong.empty(), OptionalDouble.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()),
                OptionalLong.empty(),
                new HiveColumnStatistics(
                        Optional.empty(), Optional.empty(), OptionalLong.empty(), OptionalDouble.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()),
                OptionalLong.empty(),
                new HiveColumnStatistics(
                        Optional.empty(), Optional.empty(), OptionalLong.empty(), OptionalDouble.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()));

        assertMergeHiveColumnStatistics(
                new HiveColumnStatistics(
                        Optional.empty(), Optional.empty(), OptionalLong.empty(), OptionalDouble.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()),
                OptionalLong.empty(),
                new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(1), OptionalDouble.of(2), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)),
                OptionalLong.empty(),
                new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.empty(), OptionalDouble.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()));

        assertMergeHiveColumnStatistics(
                new HiveColumnStatistics(
                        Optional.of(6L), Optional.of(4L), OptionalLong.of(5), OptionalDouble.of(3), OptionalLong.of(7), OptionalLong.of(9), OptionalLong.of(2), OptionalLong.of(8)),
                OptionalLong.of(1),
                new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(3), OptionalDouble.of(2), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)),
                OptionalLong.of(1),
                new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(5), OptionalDouble.of(2.5), OptionalLong.of(10), OptionalLong.of(13), OptionalLong.of(7), OptionalLong.of(8)));

        assertMergeHiveColumnStatistics(
                new HiveColumnStatistics(
                        Optional.of(6L), Optional.of(4L), OptionalLong.of(5), OptionalDouble.of(3), OptionalLong.of(7), OptionalLong.of(9), OptionalLong.of(2), OptionalLong.of(8)),
                OptionalLong.empty(),
                new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(3), OptionalDouble.of(2), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)),
                OptionalLong.of(1),
                new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(5), OptionalDouble.empty(), OptionalLong.of(10), OptionalLong.of(13), OptionalLong.of(7), OptionalLong.of(8)));

        assertMergeHiveColumnStatistics(
                new HiveColumnStatistics(
                        Optional.of(6L), Optional.of(4L), OptionalLong.of(5), OptionalDouble.of(3), OptionalLong.of(7), OptionalLong.of(9), OptionalLong.of(2), OptionalLong.of(8)),
                OptionalLong.of(4),
                new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(3), OptionalDouble.of(4), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)),
                OptionalLong.of(1),
                new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(5), OptionalDouble.of(3.2), OptionalLong.of(10), OptionalLong.of(13), OptionalLong.of(7), OptionalLong.of(8)));

        assertMergeHiveColumnStatistics(
                new HiveColumnStatistics(
                        Optional.of("5"), Optional.of("5"), OptionalLong.of(3), OptionalDouble.of(2), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)),
                OptionalLong.of(1),
                new HiveColumnStatistics(
                        Optional.of("6"), Optional.of("4"), OptionalLong.of(5), OptionalDouble.of(3), OptionalLong.of(7), OptionalLong.of(9), OptionalLong.of(2), OptionalLong.of(8)),
                OptionalLong.of(1),
                new HiveColumnStatistics(
                        Optional.of("5"), Optional.of("5"), OptionalLong.of(5), OptionalDouble.of(2.5), OptionalLong.of(10), OptionalLong.of(13), OptionalLong.of(7), OptionalLong.of(8)));
    }

    @Test
    public void testMergeHiveColumnStatisticsMap()
    {
        Map<String, HiveColumnStatistics> first = ImmutableMap.of(
                "column1", new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(1), OptionalDouble.of(2), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)),
                "column2", new HiveColumnStatistics(
                        Optional.of(6L), Optional.of(4L), OptionalLong.of(5), OptionalDouble.of(3), OptionalLong.of(7), OptionalLong.of(9), OptionalLong.of(2), OptionalLong.of(8)),
                "column3", new HiveColumnStatistics(
                        Optional.of("5"), Optional.of("5"), OptionalLong.of(3), OptionalDouble.of(2), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)),
                "column4", new HiveColumnStatistics(
                        Optional.of("5"), Optional.of("5"), OptionalLong.of(3), OptionalDouble.of(2), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)));
        Map<String, HiveColumnStatistics> second = ImmutableMap.of(
                "column5", new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(1), OptionalDouble.of(2), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)),
                "column2", new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(3), OptionalDouble.of(2), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)),
                "column3", new HiveColumnStatistics(
                        Optional.of("6"), Optional.of("4"), OptionalLong.of(5), OptionalDouble.of(3), OptionalLong.of(7), OptionalLong.of(9), OptionalLong.of(2), OptionalLong.of(8)),
                "column6", new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(1), OptionalDouble.of(2), OptionalLong.of(3), OptionalLong.of(4), OptionalLong.of(5), OptionalLong.of(6)));
        Map<String, HiveColumnStatistics> expected = ImmutableMap.of(
                "column2", new HiveColumnStatistics(
                        Optional.of(5L), Optional.of(5L), OptionalLong.of(5), OptionalDouble.of(2.5), OptionalLong.of(10), OptionalLong.of(13), OptionalLong.of(7), OptionalLong.of(8)),
                "column3", new HiveColumnStatistics(
                        Optional.of("5"), Optional.of("5"), OptionalLong.of(5), OptionalDouble.of(2.5), OptionalLong.of(10), OptionalLong.of(13), OptionalLong.of(7), OptionalLong.of(8)));
        assertThat(merge(first, OptionalLong.of(5), second, OptionalLong.of(5))).isEqualTo(expected);
        assertThat(merge(ImmutableMap.of(), OptionalLong.empty(), ImmutableMap.of(), OptionalLong.empty())).isEqualTo(ImmutableMap.of());
    }

    private static void assertMergeHiveColumnStatistics(
            HiveColumnStatistics first, OptionalLong firstRowCount, HiveColumnStatistics second, OptionalLong secondRowCount, HiveColumnStatistics expected)
    {
        assertThat(merge(first, firstRowCount, second, secondRowCount)).isEqualTo(expected);
    }
}
