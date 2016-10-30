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

package com.facebook.presto.hive.metastore;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

public class ColumnStatistics<T>
{
    private final Optional<T> lowValue;
    private final Optional<T> highValue;
    private final OptionalLong maxColumnLength;
    private final OptionalDouble averageColumnLength;
    private final OptionalLong trueCount;
    private final OptionalLong falseCount;
    private final OptionalLong nullsCount;
    private final OptionalLong distinctValuesCount;

    @JsonCreator
    public ColumnStatistics(
            @JsonProperty("minValue") Optional<T> lowValue,
            @JsonProperty("maxValue") Optional<T> highValue,
            @JsonProperty("maxColumnLength") OptionalLong maxColumnLength,
            @JsonProperty("averageColumnLength") OptionalDouble averageColumnLength,
            @JsonProperty("trueCount") OptionalLong trueCount,
            @JsonProperty("falseCount") OptionalLong falseCount,
            @JsonProperty("nullsCount") OptionalLong nullsCount,
            @JsonProperty("distinctValuesCount") OptionalLong distinctValuesCount)
    {
        this.lowValue = lowValue;
        this.highValue = highValue;
        this.maxColumnLength = maxColumnLength;
        this.averageColumnLength = averageColumnLength;
        this.trueCount = trueCount;
        this.falseCount = falseCount;
        this.nullsCount = nullsCount;
        this.distinctValuesCount = distinctValuesCount;
    }

    @JsonProperty
    public Optional<T> getLowValue()
    {
        return lowValue;
    }

    @JsonProperty
    public Optional<T> getHighValue()
    {
        return highValue;
    }

    @JsonProperty
    public OptionalLong getMaxColumnLength()
    {
        return maxColumnLength;
    }

    @JsonProperty
    public OptionalDouble getAverageColumnLength()
    {
        return averageColumnLength;
    }

    @JsonProperty
    public OptionalLong getTrueCount()
    {
        return trueCount;
    }

    @JsonProperty
    public OptionalLong getFalseCount()
    {
        return falseCount;
    }

    @JsonProperty
    public OptionalLong getNullsCount()
    {
        return nullsCount;
    }

    @JsonProperty
    public OptionalLong getDistinctValuesCount()
    {
        return distinctValuesCount;
    }
}
