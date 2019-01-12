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
package io.prestosql.spi.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.QueryId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Collections.unmodifiableMap;

public final class MemoryPoolInfo
{
    private final long maxBytes;
    private final long reservedBytes;
    private final long reservedRevocableBytes;
    private final Map<QueryId, Long> queryMemoryReservations;
    private final Map<QueryId, List<MemoryAllocation>> queryMemoryAllocations;
    private final Map<QueryId, Long> queryMemoryRevocableReservations;

    @JsonCreator
    public MemoryPoolInfo(
            @JsonProperty("maxBytes") long maxBytes,
            @JsonProperty("reservedBytes") long reservedBytes,
            @JsonProperty("reservedRevocableBytes") long reservedRevocableBytes,
            @JsonProperty("queryMemoryReservations") Map<QueryId, Long> queryMemoryReservations,
            @JsonProperty("queryMemoryAllocations") Map<QueryId, List<MemoryAllocation>> queryMemoryAllocations,
            @JsonProperty("queryMemoryRevocableReservations") Map<QueryId, Long> queryMemoryRevocableReservations)
    {
        this.maxBytes = maxBytes;
        this.reservedBytes = reservedBytes;
        this.reservedRevocableBytes = reservedRevocableBytes;
        this.queryMemoryReservations = unmodifiableMap(new HashMap<>(queryMemoryReservations));
        this.queryMemoryAllocations = unmodifiableMap(new HashMap<>(queryMemoryAllocations));
        this.queryMemoryRevocableReservations = unmodifiableMap(new HashMap<>(queryMemoryRevocableReservations));
    }

    @JsonProperty
    public long getMaxBytes()
    {
        return maxBytes;
    }

    @JsonProperty
    public long getFreeBytes()
    {
        return maxBytes - reservedBytes - reservedRevocableBytes;
    }

    @JsonProperty
    public long getReservedBytes()
    {
        return reservedBytes;
    }

    @JsonProperty
    public long getReservedRevocableBytes()
    {
        return reservedRevocableBytes;
    }

    @JsonProperty
    public Map<QueryId, Long> getQueryMemoryReservations()
    {
        return queryMemoryReservations;
    }

    @JsonProperty
    public Map<QueryId, List<MemoryAllocation>> getQueryMemoryAllocations()
    {
        return queryMemoryAllocations;
    }

    @JsonProperty
    public Map<QueryId, Long> getQueryMemoryRevocableReservations()
    {
        return queryMemoryRevocableReservations;
    }

    @Override
    public String toString()
    {
        return format("maxBytes=%s,reservedBytes=%s,reserveRevocableBytes=%s", maxBytes, reservedBytes, reservedRevocableBytes);
    }
}
