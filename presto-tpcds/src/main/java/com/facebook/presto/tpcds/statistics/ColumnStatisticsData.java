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

package com.facebook.presto.tpcds.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ColumnStatisticsData
{
    private final long distinctValuesCount;
    private final long nullsCount;
    private final Optional<Object> min;
    private final Optional<Object> max;

    @JsonCreator
    public ColumnStatisticsData(
            @JsonProperty("distinctValuesCount") long distinctValuesCount,
            @JsonProperty("nullsCount") long nullsCount,
            @JsonProperty("min") Optional<Object> min,
            @JsonProperty("max") Optional<Object> max)
    {
        this.distinctValuesCount = distinctValuesCount;
        this.nullsCount = nullsCount;
        this.min = requireNonNull(min);
        this.max = requireNonNull(max);
    }

    public long getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    public long getNullsCount()
    {
        return nullsCount;
    }

    public Optional<Object> getMin()
    {
        return min;
    }

    public Optional<Object> getMax()
    {
        return max;
    }
}
