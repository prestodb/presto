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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.function.Function;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class StatisticAggregationsDescriptor<T>
{
    private final Map<T, String> grouping;
    private final Map<T, TableStatisticType> tableStatistics;
    private final Map<T, ColumnStatisticMetadata> columnStatistics;

    public static <T> StatisticAggregationsDescriptor<T> empty()
    {
        return StatisticAggregationsDescriptor.<T>builder().build();
    }

    @JsonCreator
    public StatisticAggregationsDescriptor(
            @JsonProperty("grouping") Map<T, String> grouping,
            @JsonProperty("tableStatistics") Map<T, TableStatisticType> tableStatistics,
            @JsonProperty("columnStatistics") Map<T, ColumnStatisticMetadata> columnStatistics)
    {
        this.grouping = ImmutableMap.copyOf(requireNonNull(grouping, "grouping is null"));
        this.tableStatistics = ImmutableMap.copyOf(requireNonNull(tableStatistics, "tableStatistics is null"));
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
    }

    @JsonProperty
    public Map<T, String> getGrouping()
    {
        return grouping;
    }

    @JsonProperty
    public Map<T, TableStatisticType> getTableStatistics()
    {
        return tableStatistics;
    }

    @JsonProperty
    public Map<T, ColumnStatisticMetadata> getColumnStatistics()
    {
        return columnStatistics;
    }

    public static <B> Builder<B> builder()
    {
        return new Builder<>();
    }

    public <T2> StatisticAggregationsDescriptor<T2> map(Function<T, T2> mapper)
    {
        return new StatisticAggregationsDescriptor<>(
                map(this.getGrouping(), mapper),
                map(this.getTableStatistics(), mapper),
                map(this.getColumnStatistics(), mapper));
    }

    private static <K1, K2, V> Map<K2, V> map(Map<K1, V> input, Function<K1, K2> mapper)
    {
        return input.entrySet()
                .stream()
                .collect(toImmutableMap(entry -> mapper.apply(entry.getKey()), Map.Entry::getValue));
    }

    public static class Builder<T>
    {
        private final ImmutableMap.Builder<T, String> grouping = ImmutableMap.builder();
        private final ImmutableMap.Builder<T, TableStatisticType> tableStatistics = ImmutableMap.builder();
        private final ImmutableMap.Builder<T, ColumnStatisticMetadata> columnStatistics = ImmutableMap.builder();

        public void addGrouping(T key, String column)
        {
            grouping.put(key, column);
        }

        public void addTableStatistic(T key, TableStatisticType type)
        {
            tableStatistics.put(key, type);
        }

        public void addColumnStatistic(T key, ColumnStatisticMetadata statisticMetadata)
        {
            columnStatistics.put(key, statisticMetadata);
        }

        public StatisticAggregationsDescriptor<T> build()
        {
            return new StatisticAggregationsDescriptor<>(grouping.build(), tableStatistics.build(), columnStatistics.build());
        }
    }
}
