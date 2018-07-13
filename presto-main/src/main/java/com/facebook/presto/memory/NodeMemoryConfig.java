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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.BYTE;

// This is separate from MemoryManagerConfig because it's difficult to test the default value of maxQueryMemoryPerNode
public class NodeMemoryConfig
{
    public static final long AVAILABLE_HEAP_MEMORY = Runtime.getRuntime().maxMemory();
    public static final String QUERY_MAX_MEMORY_PER_NODE_CONFIG = "query.max-memory-per-node";
    public static final String QUERY_MAX_TOTAL_MEMORY_PER_NODE_CONFIG = "query.max-total-memory-per-node";

    private boolean isLegacySystemPoolEnabled;

    private DataSize maxQueryMemoryPerNode = new DataSize(AVAILABLE_HEAP_MEMORY * 0.1, BYTE);

    // This is a per-query limit for the user plus system allocations.
    private DataSize maxQueryTotalMemoryPerNode = new DataSize(AVAILABLE_HEAP_MEMORY * 0.3, BYTE);
    private DataSize heapHeadroom = new DataSize(AVAILABLE_HEAP_MEMORY * 0.3, BYTE);

    @NotNull
    public DataSize getMaxQueryMemoryPerNode()
    {
        return maxQueryMemoryPerNode;
    }

    @Config(QUERY_MAX_MEMORY_PER_NODE_CONFIG)
    public NodeMemoryConfig setMaxQueryMemoryPerNode(DataSize maxQueryMemoryPerNode)
    {
        this.maxQueryMemoryPerNode = maxQueryMemoryPerNode;
        return this;
    }

    public boolean isLegacySystemPoolEnabled()
    {
        return isLegacySystemPoolEnabled;
    }

    @Config("deprecated.legacy-system-pool-enabled")
    public NodeMemoryConfig setLegacySystemPoolEnabled(boolean legacySystemPoolEnabled)
    {
        isLegacySystemPoolEnabled = legacySystemPoolEnabled;
        return this;
    }

    @NotNull
    public DataSize getMaxQueryTotalMemoryPerNode()
    {
        return maxQueryTotalMemoryPerNode;
    }

    @Config(QUERY_MAX_TOTAL_MEMORY_PER_NODE_CONFIG)
    public NodeMemoryConfig setMaxQueryTotalMemoryPerNode(DataSize maxQueryTotalMemoryPerNode)
    {
        this.maxQueryTotalMemoryPerNode = maxQueryTotalMemoryPerNode;
        return this;
    }

    public DataSize getHeapHeadroom()
    {
        return heapHeadroom;
    }

    @NotNull
    @Config("memory.heap-headroom-per-node")
    @ConfigDescription("The amount of heap memory to set aside as headroom/buffer (e.g., for untracked allocations)")
    public NodeMemoryConfig setHeapHeadroom(DataSize heapHeadroom)
    {
        this.heapHeadroom = heapHeadroom;
        return this;
    }
}
