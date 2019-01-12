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
package io.prestosql.plugin.tpch.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ColumnStatisticsData
{
    private final Optional<Long> distinctValuesCount;
    private final Optional<Object> min;
    private final Optional<Object> max;
    private final Optional<Long> dataSize;

    @JsonCreator
    public ColumnStatisticsData(
            @JsonProperty("distinctValuesCount") Optional<Long> distinctValuesCount,
            @JsonProperty("min") Optional<Object> min,
            @JsonProperty("max") Optional<Object> max,
            @JsonProperty("dataSize") Optional<Long> dataSize)
    {
        this.distinctValuesCount = requireNonNull(distinctValuesCount, "distinctValuesCount is null");
        this.min = requireNonNull(min);
        this.max = requireNonNull(max);
        this.dataSize = requireNonNull(dataSize, "dataSize is null");
    }

    public static ColumnStatisticsData empty()
    {
        return new ColumnStatisticsData(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    public static ColumnStatisticsData zero()
    {
        return new ColumnStatisticsData(Optional.of(0L), Optional.empty(), Optional.empty(), Optional.of(0L));
    }

    public Optional<Long> getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    public Optional<Object> getMin()
    {
        return min;
    }

    public Optional<Object> getMax()
    {
        return max;
    }

    public Optional<Long> getDataSize()
    {
        return dataSize;
    }
}
