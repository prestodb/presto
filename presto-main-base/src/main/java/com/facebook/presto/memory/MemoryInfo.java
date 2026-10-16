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

import com.facebook.airlift.units.DataSize;
import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class MemoryInfo
{
    private final DataSize totalNodeMemory;
    // Total process-level memory on native workers (cgroup inactive_anon +
    // active_anon, sampled by LinuxMemoryChecker). Covers the presto_server
    // binary, shared libraries, mmap'd files and process overhead that
    // pool-tracked query memory does not. 0 on JVM workers and on native
    // workers where the memory checker is disabled.
    private final long systemMemoryBytes;
    private final Map<MemoryPoolId, MemoryPoolInfo> pools;

    public MemoryInfo(DataSize totalNodeMemory, Map<MemoryPoolId, MemoryPoolInfo> pools)
    {
        this(totalNodeMemory, 0L, pools);
    }

    @ThriftConstructor
    @JsonCreator
    public MemoryInfo(
            @JsonProperty("totalNodeMemory") DataSize totalNodeMemory,
            @JsonProperty("systemMemoryBytes") long systemMemoryBytes,
            @JsonProperty("pools") Map<MemoryPoolId, MemoryPoolInfo> pools)
    {
        this.totalNodeMemory = requireNonNull(totalNodeMemory, "totalNodeMemory is null");
        this.systemMemoryBytes = systemMemoryBytes;
        this.pools = ImmutableMap.copyOf(requireNonNull(pools, "pools is null"));
    }

    @ThriftField(1)
    @JsonProperty
    public DataSize getTotalNodeMemory()
    {
        return totalNodeMemory;
    }

    @ThriftField(2)
    @JsonProperty
    public Map<MemoryPoolId, MemoryPoolInfo> getPools()
    {
        return pools;
    }

    @ThriftField(3)
    @JsonProperty
    public long getSystemMemoryBytes()
    {
        return systemMemoryBytes;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("totalNodeMemory", totalNodeMemory)
                .add("systemMemoryBytes", systemMemoryBytes)
                .add("pools", pools)
                .toString();
    }
}
