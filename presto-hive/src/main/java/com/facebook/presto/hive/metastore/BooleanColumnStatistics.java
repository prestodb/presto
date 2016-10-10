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

public class BooleanColumnStatistics
{
    private long trueCount;
    private long falseCount;
    private long nullsCount;

    @JsonCreator
    public BooleanColumnStatistics(
            @JsonProperty("trueCount") long trueCount,
            @JsonProperty("falseCount") long falseCount,
            @JsonProperty("nullsCount") long nullsCount)
    {
        this.trueCount = trueCount;
        this.falseCount = falseCount;
        this.nullsCount = nullsCount;
    }

    @JsonProperty
    public long getTrueCount()
    {
        return trueCount;
    }

    @JsonProperty
    public long getFalseCount()
    {
        return falseCount;
    }

    @JsonProperty
    public long getNullsCount()
    {
        return nullsCount;
    }
}
