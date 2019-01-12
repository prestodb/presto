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
package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.statistics.ColumnStatisticMetadata;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.statistics.TableStatisticType;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class StatisticAggregationsDescriptor<T>
{
    private final Map<String, T> grouping;
    private final Map<TableStatisticType, T> tableStatistics;
    private final Map<ColumnStatisticMetadata, T> columnStatistics;

    public static <T> StatisticAggregationsDescriptor<T> empty()
    {
        return StatisticAggregationsDescriptor.<T>builder().build();
    }

    @JsonCreator
    public StatisticAggregationsDescriptor(
            @JsonProperty("grouping") Map<String, T> grouping,
            @JsonProperty("tableStatistics") Map<TableStatisticType, T> tableStatistics,
            @JsonProperty("columnStatistics") Map<ColumnStatisticMetadata, T> columnStatistics)
    {
        this.grouping = ImmutableMap.copyOf(requireNonNull(grouping, "grouping is null"));
        this.tableStatistics = ImmutableMap.copyOf(requireNonNull(tableStatistics, "tableStatistics is null"));
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
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
    @JsonSerialize(keyUsing = ColumnStatisticMetadataKeySerializer.class)
    @JsonDeserialize(keyUsing = ColumnStatisticMetadataKeyDeserializer.class)
    public Map<ColumnStatisticMetadata, T> getColumnStatistics()
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
        return toStringHelper(this)
                .add("grouping", grouping)
                .add("tableStatistics", tableStatistics)
                .add("columnStatistics", columnStatistics)
                .toString();
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

    private static <K, V1, V2> Map<K, V2> map(Map<K, V1> input, Function<V1, V2> mapper)
    {
        return input.entrySet()
                .stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> mapper.apply(entry.getValue())));
    }

    public static class Builder<T>
    {
        private final ImmutableMap.Builder<String, T> grouping = ImmutableMap.builder();
        private final ImmutableMap.Builder<TableStatisticType, T> tableStatistics = ImmutableMap.builder();
        private final ImmutableMap.Builder<ColumnStatisticMetadata, T> columnStatistics = ImmutableMap.builder();

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
            return new StatisticAggregationsDescriptor<>(grouping.build(), tableStatistics.build(), columnStatistics.build());
        }
    }

    public static class ColumnStatisticMetadataKeySerializer
            extends JsonSerializer<ColumnStatisticMetadata>
    {
        @Override
        public void serialize(ColumnStatisticMetadata value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException
        {
            verify(value != null, "value is null");
            gen.writeFieldName(serialize(value));
        }

        @VisibleForTesting
        static String serialize(ColumnStatisticMetadata value)
        {
            return value.getStatisticType().name() + ":" + value.getColumnName();
        }
    }

    public static class ColumnStatisticMetadataKeyDeserializer
            extends KeyDeserializer
    {
        @Override
        public ColumnStatisticMetadata deserializeKey(String key, DeserializationContext ctxt)
        {
            return deserialize(requireNonNull(key, "key is null"));
        }

        @VisibleForTesting
        static ColumnStatisticMetadata deserialize(String value)
        {
            int separatorIndex = value.indexOf(':');
            checkArgument(separatorIndex >= 0, "separator not found: %s", value);
            String statisticType = value.substring(0, separatorIndex);
            String column = value.substring(separatorIndex + 1);
            return new ColumnStatisticMetadata(column, ColumnStatisticType.valueOf(statisticType));
        }
    }
}
