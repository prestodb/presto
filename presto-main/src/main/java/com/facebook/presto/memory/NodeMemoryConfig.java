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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.BYTE;

// This is separate from MemoryManagerConfig because it's difficult to test the default value of maxQueryMemoryPerNode
public class NodeMemoryConfig
{
    public static final long AVAILABLE_HEAP_MEMORY = Runtime.getRuntime().maxMemory();
    public static final String QUERY_MAX_BROADCAST_MEMORY_CONFIG = "query.max-broadcast-memory";
    public static final String QUERY_MAX_MEMORY_PER_NODE_CONFIG = "query.max-memory-per-node";
    public static final String QUERY_SOFT_MAX_MEMORY_PER_NODE_CONFIG = "query.soft-max-memory-per-node";
    public static final String QUERY_MAX_TOTAL_MEMORY_PER_NODE_CONFIG = "query.max-total-memory-per-node";
    public static final String QUERY_SOFT_MAX_TOTAL_MEMORY_PER_NODE_CONFIG = "query.soft-max-total-memory-per-node";

    private boolean isReservedPoolEnabled = true;

    private DataSize maxQueryBroadcastMemory;
    private DataSize maxQueryMemoryPerNode = new DataSize(AVAILABLE_HEAP_MEMORY * 0.1, BYTE);
    private DataSize softMaxQueryMemoryPerNode;

    // This is a per-query limit for the user plus system allocations.
    private DataSize maxQueryTotalMemoryPerNode = new DataSize(AVAILABLE_HEAP_MEMORY * 0.3, BYTE);
    private DataSize softMaxQueryTotalMemoryPerNode;
    private DataSize heapHeadroom = new DataSize(AVAILABLE_HEAP_MEMORY * 0.3, BYTE);

    private boolean verboseExceededMemoryLimitErrorsEnabled = true;

    @NotNull
    public DataSize getMaxQueryBroadcastMemory()
    {
        if (maxQueryBroadcastMemory == null) {
            return getMaxQueryMemoryPerNode();
        }
        return maxQueryBroadcastMemory;
    }

    @Config(QUERY_MAX_BROADCAST_MEMORY_CONFIG)
    public NodeMemoryConfig setMaxQueryBroadcastMemory(DataSize maxQueryBroadcastMemory)
    {
        if (maxQueryBroadcastMemory.toBytes() < getMaxQueryMemoryPerNode().toBytes()) {
            this.maxQueryBroadcastMemory = maxQueryBroadcastMemory;
        }
        return this;
    }

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

    @NotNull
    public DataSize getSoftMaxQueryMemoryPerNode()
    {
        if (softMaxQueryMemoryPerNode == null) {
            return getMaxQueryMemoryPerNode();
        }
        return softMaxQueryMemoryPerNode;
    }

    @Config(QUERY_SOFT_MAX_MEMORY_PER_NODE_CONFIG)
    public NodeMemoryConfig setSoftMaxQueryMemoryPerNode(DataSize softMaxQueryMemoryPerNode)
    {
        this.softMaxQueryMemoryPerNode = softMaxQueryMemoryPerNode;
        return this;
    }

    public boolean isReservedPoolEnabled()
    {
        return isReservedPoolEnabled;
    }

    @Config("experimental.reserved-pool-enabled")
    public NodeMemoryConfig setReservedPoolEnabled(boolean reservedPoolEnabled)
    {
        isReservedPoolEnabled = reservedPoolEnabled;
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

    @NotNull
    public DataSize getSoftMaxQueryTotalMemoryPerNode()
    {
        if (softMaxQueryTotalMemoryPerNode == null) {
            return getMaxQueryTotalMemoryPerNode();
        }
        return softMaxQueryTotalMemoryPerNode;
    }

    @Config(QUERY_SOFT_MAX_TOTAL_MEMORY_PER_NODE_CONFIG)
    public NodeMemoryConfig setSoftMaxQueryTotalMemoryPerNode(DataSize softMaxQueryTotalMemoryPerNode)
    {
        this.softMaxQueryTotalMemoryPerNode = softMaxQueryTotalMemoryPerNode;
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

    public boolean isVerboseExceededMemoryLimitErrorsEnabled()
    {
        return verboseExceededMemoryLimitErrorsEnabled;
    }

    @Config("memory.verbose-exceeded-memory-limit-errors-enabled")
    @ConfigDescription("When enabled the error message for exceeded memory limit errors will contain additional operator memory allocation details")
    public NodeMemoryConfig setVerboseExceededMemoryLimitErrorsEnabled(boolean verboseExceededMemoryLimitErrorsEnabled)
    {
        this.verboseExceededMemoryLimitErrorsEnabled = verboseExceededMemoryLimitErrorsEnabled;
        return this;
    }
}
