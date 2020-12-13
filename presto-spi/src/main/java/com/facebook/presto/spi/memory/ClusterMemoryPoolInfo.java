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
package com.facebook.presto.spi.memory;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.QueryId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class ClusterMemoryPoolInfo
{
    private final MemoryPoolInfo memoryPoolInfo;
    private final int blockedNodes;
    private final int assignedQueries;
    private final Optional<QueryId> largestMemoryQuery;

    public ClusterMemoryPoolInfo(
            MemoryPoolInfo memoryPoolInfo,
            int blockedNodes,
            int assignedQueries)
    {
        this(memoryPoolInfo, blockedNodes, assignedQueries, Optional.empty());
    }

    @ThriftConstructor
    @JsonCreator
    public ClusterMemoryPoolInfo(
            @JsonProperty("memoryPoolInfo") MemoryPoolInfo memoryPoolInfo,
            int blockedNodes,
            int assignedQueries,
            Optional<QueryId> largestMemoryQuery)
    {
        this.memoryPoolInfo = requireNonNull(memoryPoolInfo, "memoryPoolInfo is null");
        this.blockedNodes = blockedNodes;
        this.assignedQueries = assignedQueries;
        this.largestMemoryQuery = largestMemoryQuery;
    }

    @ThriftField(1)
    @JsonProperty
    public MemoryPoolInfo getMemoryPoolInfo()
    {
        return memoryPoolInfo;
    }

    @ThriftField(2)
    @JsonProperty
    public int getBlockedNodes()
    {
        return blockedNodes;
    }

    @ThriftField(3)
    @JsonProperty
    public int getAssignedQueries()
    {
        return assignedQueries;
    }

    @ThriftField(4)
    @JsonProperty
    public Optional<QueryId> getLargestMemoryQuery()
    {
        return largestMemoryQuery;
    }
}
