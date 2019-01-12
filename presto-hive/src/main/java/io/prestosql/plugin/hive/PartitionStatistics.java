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

package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.metastore.HiveColumnStatistics;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@Immutable
public class PartitionStatistics
{
    private static final PartitionStatistics EMPTY = new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of());

    private final HiveBasicStatistics basicStatistics;
    private final Map<String, HiveColumnStatistics> columnStatistics;

    public static PartitionStatistics empty()
    {
        return EMPTY;
    }

    @JsonCreator
    public PartitionStatistics(
            @JsonProperty("basicStatistics") HiveBasicStatistics basicStatistics,
            @JsonProperty("columnStatistics") Map<String, HiveColumnStatistics> columnStatistics)
    {
        this.basicStatistics = requireNonNull(basicStatistics, "basicStatistics is null");
        this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics can not be null"));
    }

    @JsonProperty
    public HiveBasicStatistics getBasicStatistics()
    {
        return basicStatistics;
    }

    @JsonProperty
    public Map<String, HiveColumnStatistics> getColumnStatistics()
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
        PartitionStatistics that = (PartitionStatistics) o;
        return Objects.equals(basicStatistics, that.basicStatistics) &&
                Objects.equals(columnStatistics, that.columnStatistics);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(basicStatistics, columnStatistics);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("basicStatistics", basicStatistics)
                .add("columnStatistics", columnStatistics)
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private HiveBasicStatistics basicStatistics = HiveBasicStatistics.createEmptyStatistics();
        private Map<String, HiveColumnStatistics> columnStatistics = ImmutableMap.of();

        public Builder setBasicStatistics(HiveBasicStatistics basicStatistics)
        {
            this.basicStatistics = requireNonNull(basicStatistics, "basicStatistics is null");
            return this;
        }

        public Builder setColumnStatistics(Map<String, HiveColumnStatistics> columnStatistics)
        {
            this.columnStatistics = ImmutableMap.copyOf(requireNonNull(columnStatistics, "columnStatistics is null"));
            return this;
        }

        public PartitionStatistics build()
        {
            return new PartitionStatistics(basicStatistics, columnStatistics);
        }
    }
}
