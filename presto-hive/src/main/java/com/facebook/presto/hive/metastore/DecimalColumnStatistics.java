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

import java.math.BigDecimal;

public class DecimalColumnStatistics
{
    private final BigDecimal lowValue;
    private final BigDecimal highValue;
    private final long distinctValuesCount;
    private final long nullsCount;

    @JsonCreator
    public DecimalColumnStatistics(
            @JsonProperty("lowValue") BigDecimal lowValue,
            @JsonProperty("highValue") BigDecimal highValue,
            @JsonProperty("distinctValuesCount") long distinctValuesCount,
            @JsonProperty("nullsCount") long nullsCount)
    {
        this.lowValue = lowValue;
        this.highValue = highValue;
        this.distinctValuesCount = distinctValuesCount;
        this.nullsCount = nullsCount;
    }

    @JsonProperty
    public BigDecimal getLowValue()
    {
        return lowValue;
    }

    @JsonProperty
    public BigDecimal getHighValue()
    {
        return highValue;
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
