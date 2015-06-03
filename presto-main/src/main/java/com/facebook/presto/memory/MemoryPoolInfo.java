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
package com.facebook.presto.memory;

import com.facebook.presto.execution.QueryId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class MemoryPoolInfo
{
    private final long maxBytes;
    private final long freeBytes;
    private final Map<QueryId, Long> queryMemoryReservations;

    @JsonCreator
    public MemoryPoolInfo(@JsonProperty("maxBytes") long maxBytes, @JsonProperty("freeBytes") long freeBytes, @JsonProperty("queryMemoryReservations") Map<QueryId, Long> queryMemoryReservations)
    {
        this.maxBytes = maxBytes;
        this.freeBytes = freeBytes;
        this.queryMemoryReservations = ImmutableMap.copyOf(queryMemoryReservations);
    }

    @JsonProperty
    public long getMaxBytes()
    {
        return maxBytes;
    }

    @JsonProperty
    public long getFreeBytes()
    {
        return freeBytes;
    }

    @JsonProperty
    public Map<QueryId, Long> getQueryMemoryReservations()
    {
        return queryMemoryReservations;
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("maxBytes", maxBytes)
                .add("freeBytes", freeBytes)
                .toString();
    }
}
