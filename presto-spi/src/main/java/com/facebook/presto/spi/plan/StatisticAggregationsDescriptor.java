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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.TableStatisticType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class StatisticAggregationsDescriptor<T>
{
    private final Map<String, T> grouping;
    private final Map<TableStatisticType, T> tableStatistics;
    private final List<ColumnStatisticsDescriptor<T>> columnStatistics;

    public static <T> StatisticAggregationsDescriptor<T> empty()
    {
        return StatisticAggregationsDescriptor.<T>builder().build();
    }

    @JsonCreator
    public StatisticAggregationsDescriptor(
            @JsonProperty("grouping") Map<String, T> grouping,
            @JsonProperty("tableStatistics") Map<TableStatisticType, T> tableStatistics,
            @JsonProperty("columnStatistics") List<ColumnStatisticsDescriptor<T>> columnStatistics)
    {
        this.grouping = Collections.unmodifiableMap(new LinkedHashMap<>(requireNonNull(grouping, "grouping is null")));
        this.tableStatistics = Collections.unmodifiableMap(new LinkedHashMap<>(requireNonNull(tableStatistics, "tableStatistics is null")));
        this.columnStatistics = requireNonNull(columnStatistics, "columnStatistics is null");
    }

    public StatisticAggregationsDescriptor(
            Map<String, T> grouping,
            Map<TableStatisticType, T> tableStatistics,
            Map<ColumnStatisticMetadata, T> columnStatistics)
    {
        this(grouping, tableStatistics, Collections.unmodifiableList(columnStatistics.entrySet().stream()
                .map(e -> new ColumnStatisticsDescriptor<>(e.getKey(), e.getValue())).collect(Collectors.toList())));
    }

    @JsonProperty
    public Map<String, T> getGrouping()
    {
        return grouping;
    }

    @JsonProperty
    public Map<TableStatisticType, T> getTableStatistics()
    {
        return tableStatistics;
    }

    @JsonProperty
    public List<ColumnStatisticsDescriptor<T>> getColumnStatistics()
    {
        return columnStatistics;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StatisticAggregationsDescriptor<?> that = (StatisticAggregationsDescriptor<?>) o;
        return Objects.equals(grouping, that.grouping) &&
                Objects.equals(tableStatistics, that.tableStatistics) &&
                Objects.equals(columnStatistics, that.columnStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(grouping, tableStatistics, columnStatistics);
    }

    @Override
    public String toString()
    {
        return format("%s {grouping=%s, tableStatistics=%s, columnStatistics=%s}", this.getClass().getSimpleName(), grouping, tableStatistics, columnStatistics);
    }

    public static <B> Builder<B> builder()
    {
        return new Builder<>();
    }

    public <T2> StatisticAggregationsDescriptor<T2> map(Function<T, T2> mapper)
    {
        Map<ColumnStatisticMetadata, T> newMap = new LinkedHashMap<>();
        this.getColumnStatistics().forEach(x ->
        {
            if (newMap.containsKey(x.getMetadata())) {
                throw new IllegalStateException(format("Duplicate key %s", x));
            }
            newMap.put(x.getMetadata(), x.getItem());
        });
        return new StatisticAggregationsDescriptor<>(
                map(this.getGrouping(), mapper),
                map(this.getTableStatistics(), mapper),
                map(Collections.unmodifiableMap(newMap),
                        mapper));
    }

    private static <K, V1, V2> Map<K, V2> map(Map<K, V1> input, Function<V1, V2> mapper)
    {
        return Collections.unmodifiableMap(input.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> mapper.apply(entry.getValue()), (e1, e2) -> { throw new IllegalStateException(format("Duplicate key %s", e1)); }, LinkedHashMap::new)));
    }

    public static class Builder<T>
    {
        private final Map<String, T> grouping = new LinkedHashMap<>();
        private final Map<TableStatisticType, T> tableStatistics = new LinkedHashMap<>();
        private final Map<ColumnStatisticMetadata, T> columnStatistics = new LinkedHashMap<>();

        public void addGrouping(String column, T key)
        {
            grouping.put(column, key);
        }

        public void addTableStatistic(TableStatisticType type, T key)
        {
            tableStatistics.put(type, key);
        }

        public void addColumnStatistic(ColumnStatisticMetadata statisticMetadata, T key)
        {
            columnStatistics.put(statisticMetadata, key);
        }

        public StatisticAggregationsDescriptor<T> build()
        {
            return new StatisticAggregationsDescriptor<>(Collections.unmodifiableMap(grouping), Collections.unmodifiableMap(tableStatistics), Collections.unmodifiableMap(columnStatistics));
        }
    }

    public static class ColumnStatisticsDescriptor<T>
    {
        private final ColumnStatisticMetadata metadata;
        private final T item;

        @JsonCreator
        public ColumnStatisticsDescriptor(@JsonProperty("metadata") ColumnStatisticMetadata metadata, @JsonProperty("item") T item)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.item = requireNonNull(item, "item is null");
        }

        @JsonProperty
        public T getItem()
        {
            return item;
        }

        @JsonProperty
        public ColumnStatisticMetadata getMetadata()
        {
            return metadata;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(metadata, item);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }

            if (!(o instanceof ColumnStatisticsDescriptor)) {
                return false;
            }

            ColumnStatisticsDescriptor<?> other = (ColumnStatisticsDescriptor<?>) o;
            return metadata.equals(other.metadata) &&
                    item.equals(other.item);
        }

        @Override
        public String toString()
        {
            return format("%s {metadata=%s, item=%s}", this.getClass().getSimpleName(), metadata, item);
        }
    }
}
