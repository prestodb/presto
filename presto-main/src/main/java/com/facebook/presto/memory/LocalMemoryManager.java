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

import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.memory.NodeMemoryConfig.QUERY_MAX_MEMORY_PER_NODE_CONFIG;
import static com.facebook.presto.memory.NodeMemoryConfig.QUERY_MAX_TOTAL_MEMORY_PER_NODE_CONFIG;
import static com.facebook.presto.memory.NodeMemoryConfig.QUERY_SOFT_MAX_MEMORY_PER_NODE_CONFIG;
import static com.facebook.presto.memory.NodeMemoryConfig.QUERY_SOFT_MAX_TOTAL_MEMORY_PER_NODE_CONFIG;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class LocalMemoryManager
{
    public static final MemoryPoolId GENERAL_POOL = new MemoryPoolId("general");
    public static final MemoryPoolId RESERVED_POOL = new MemoryPoolId("reserved");

    private DataSize maxMemory;
    private Map<MemoryPoolId, MemoryPool> pools;

    @Inject
    public LocalMemoryManager(NodeMemoryConfig config)
    {
        this(config, Runtime.getRuntime().maxMemory());
    }

    @VisibleForTesting
    public LocalMemoryManager(NodeMemoryConfig config, long availableMemory)
    {
        requireNonNull(config, "config is null");
        configureMemoryPools(config, availableMemory);
    }

    private void configureMemoryPools(NodeMemoryConfig config, long availableMemory)
    {
        validateHeapHeadroom(config, availableMemory);
        maxMemory = new DataSize(availableMemory - config.getHeapHeadroom().toBytes(), BYTE);
        checkArgument(
                config.getMaxQueryMemoryPerNode().toBytes() <= config.getMaxQueryTotalMemoryPerNode().toBytes(),
                "Max query memory per node (%s) cannot be greater than the max query total memory per node (%s).",
                QUERY_MAX_MEMORY_PER_NODE_CONFIG,
                QUERY_MAX_TOTAL_MEMORY_PER_NODE_CONFIG);
        checkArgument(
                config.getMaxQueryMemoryPerNode().toBytes() >= config.getSoftMaxQueryMemoryPerNode().toBytes(),
                "Max query memory per node (%s) must be >= soft limit (%s).",
                QUERY_MAX_MEMORY_PER_NODE_CONFIG,
                QUERY_SOFT_MAX_MEMORY_PER_NODE_CONFIG);
        checkArgument(
                config.getMaxQueryTotalMemoryPerNode().toBytes() >= config.getSoftMaxQueryTotalMemoryPerNode().toBytes(),
                "Max query total memory per node (%s) must be >= soft limit (%s).",
                QUERY_MAX_TOTAL_MEMORY_PER_NODE_CONFIG,
                QUERY_SOFT_MAX_TOTAL_MEMORY_PER_NODE_CONFIG);
        ImmutableMap.Builder<MemoryPoolId, MemoryPool> builder = ImmutableMap.builder();
        long generalPoolSize = maxMemory.toBytes();
        if (config.isReservedPoolEnabled()) {
            builder.put(RESERVED_POOL, new MemoryPool(RESERVED_POOL, config.getMaxQueryTotalMemoryPerNode()));
            generalPoolSize -= config.getMaxQueryTotalMemoryPerNode().toBytes();
        }
        verify(generalPoolSize > 0, "general memory pool size is 0");
        builder.put(GENERAL_POOL, new MemoryPool(GENERAL_POOL, new DataSize(generalPoolSize, BYTE)));
        this.pools = builder.build();
    }

    @VisibleForTesting
    static void validateHeapHeadroom(NodeMemoryConfig config, long availableMemory)
    {
        long maxQueryTotalMemoryPerNode = config.getMaxQueryTotalMemoryPerNode().toBytes();
        long heapHeadroom = config.getHeapHeadroom().toBytes();
        // (availableMemory - maxQueryTotalMemoryPerNode) bytes will be available for the general pool and the
        // headroom/untracked allocations, so the heapHeadroom cannot be larger than that space.
        if (heapHeadroom < 0 || heapHeadroom + maxQueryTotalMemoryPerNode > availableMemory) {
            throw new IllegalArgumentException(
                    format("Invalid memory configuration. The sum of max total query memory per node (%s) and heap headroom (%s) cannot be larger than the available heap memory (%s)",
                            maxQueryTotalMemoryPerNode,
                            heapHeadroom,
                            availableMemory));
        }
    }

    public MemoryInfo getInfo()
    {
        ImmutableMap.Builder<MemoryPoolId, MemoryPoolInfo> builder = ImmutableMap.builder();
        for (Map.Entry<MemoryPoolId, MemoryPool> entry : pools.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getInfo());
        }
        return new MemoryInfo(maxMemory, builder.build());
    }

    public List<MemoryPool> getPools()
    {
        return ImmutableList.copyOf(pools.values());
    }

    public MemoryPool getGeneralPool()
    {
        return pools.get(GENERAL_POOL);
    }

    public Optional<MemoryPool> getReservedPool()
    {
        return Optional.ofNullable(pools.get(RESERVED_POOL));
    }
}
