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

public class StringColumnStatistics
{
    private final long maxColumnLength;
    private final double averageColumnLength;
    private final long distinctValuesCount;
    private final long nullsCount;

    @JsonCreator
    public StringColumnStatistics(
            @JsonProperty("maxColumnLength") long maxColumnLength,
            @JsonProperty("averageColumnLength") double averageColumnLength,
            @JsonProperty("distinctValuesCount") long distinctValuesCount,
            @JsonProperty("nullsCount") long nullsCount)
    {
        this.maxColumnLength = maxColumnLength;
        this.averageColumnLength = averageColumnLength;
        this.distinctValuesCount = distinctValuesCount;
        this.nullsCount = nullsCount;
    }

    @JsonProperty
    public long getMaxColumnLength()
    {
        return maxColumnLength;
    }

    @JsonProperty
    public double getAverageColumnLength()
    {
        return averageColumnLength;
    }

    @JsonProperty
    public long getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    @JsonProperty
    public long getNullsCount()
    {
        return nullsCount;
    }
}
