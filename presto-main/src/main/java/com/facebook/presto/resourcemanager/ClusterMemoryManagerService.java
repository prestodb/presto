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
package com.facebook.presto.resourcemanager;

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.facebook.presto.util.PeriodicTaskExecutor;
import com.google.common.collect.ImmutableMap;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static java.util.Objects.requireNonNull;

public class ClusterMemoryManagerService
{
    private static final ClusterMemoryPoolInfo EMPTY_MEMORY_POOL = new ClusterMemoryPoolInfo(
            new MemoryPoolInfo(0, 0, 0, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of()),
            0,
            0);

    private final DriftClient<ResourceManagerClient> resourceManagerClient;
    private final ScheduledExecutorService executorService;
    private final AtomicReference<Map<MemoryPoolId, ClusterMemoryPoolInfo>> memoryPools;
    private final long memoryPoolFetchIntervalMillis;
    private final boolean isReservedPoolEnabled;
    private final PeriodicTaskExecutor memoryPoolUpdater;

    @Inject
    public ClusterMemoryManagerService(
            @ForResourceManager DriftClient<ResourceManagerClient> resourceManagerClient,
            @ForResourceManager ScheduledExecutorService executorService,
            ResourceManagerConfig resourceManagerConfig,
            NodeMemoryConfig nodeMemoryConfig)
    {
        this.resourceManagerClient = requireNonNull(resourceManagerClient, "resourceManagerClient is null");
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.memoryPoolFetchIntervalMillis = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getMemoryPoolFetchInterval().toMillis();
        this.isReservedPoolEnabled = requireNonNull(nodeMemoryConfig, "nodeMemoryConfig is null").isReservedPoolEnabled();

        ImmutableMap.Builder<MemoryPoolId, ClusterMemoryPoolInfo> defaultPoolBuilder = ImmutableMap.<MemoryPoolId, ClusterMemoryPoolInfo>builder()
                .put(GENERAL_POOL, EMPTY_MEMORY_POOL);
        if (isReservedPoolEnabled) {
            defaultPoolBuilder.put(RESERVED_POOL, EMPTY_MEMORY_POOL);
        }
        this.memoryPools = new AtomicReference<>(defaultPoolBuilder.build());
        this.memoryPoolUpdater = new PeriodicTaskExecutor(memoryPoolFetchIntervalMillis, executorService, () -> memoryPools.set(updateMemoryPoolInfo()));
    }

    @PostConstruct
    public void init()
    {
        memoryPoolUpdater.start();
    }

    @PreDestroy
    public void stop()
    {
        memoryPoolUpdater.stop();
    }

    public Map<MemoryPoolId, ClusterMemoryPoolInfo> getMemoryPoolInfo()
    {
        return memoryPools.get();
    }

    private Map<MemoryPoolId, ClusterMemoryPoolInfo> updateMemoryPoolInfo()
    {
        Map<MemoryPoolId, ClusterMemoryPoolInfo> memoryPoolInfos = resourceManagerClient.get().getMemoryPoolInfo();
        memoryPoolInfos.putIfAbsent(GENERAL_POOL, EMPTY_MEMORY_POOL);
        if (isReservedPoolEnabled) {
            memoryPoolInfos.putIfAbsent(RESERVED_POOL, EMPTY_MEMORY_POOL);
        }
        return ImmutableMap.copyOf(memoryPoolInfos);
    }
}
