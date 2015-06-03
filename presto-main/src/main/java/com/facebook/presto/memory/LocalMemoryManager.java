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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;

public final class LocalMemoryManager
{
    public static final MemoryPoolId GENERAL_POOL = new MemoryPoolId("general");
    public static final MemoryPoolId RESERVED_POOL = new MemoryPoolId("reserved");
    private static final Logger log = Logger.get(LocalMemoryManager.class);

    private final DataSize maxMemory;
    private final MBeanExporter exporter;
    private final Map<MemoryPoolId, MemoryPool> pools;

    @Inject
    public LocalMemoryManager(MemoryManagerConfig config, ReservedSystemMemoryConfig systemMemoryConfig, MBeanExporter exporter)
    {
        this.exporter = requireNonNull(exporter, "exporter is null");
        requireNonNull(config, "config is null");
        requireNonNull(systemMemoryConfig, "systemMemoryConfig is null");
        long maxHeap = Runtime.getRuntime().maxMemory();
        checkArgument(systemMemoryConfig.getReservedSystemMemory().toBytes() < maxHeap, "Reserved memory %s is greater than available heap %s", systemMemoryConfig.getReservedSystemMemory(), new DataSize(maxHeap, BYTE));
        maxMemory = new DataSize(maxHeap - systemMemoryConfig.getReservedSystemMemory().toBytes(), BYTE);

        ImmutableMap.Builder<MemoryPoolId, MemoryPool> builder = ImmutableMap.builder();
        builder.put(RESERVED_POOL, createPool(RESERVED_POOL, config.getMaxQueryMemoryPerNode(), exporter, config.isClusterMemoryManagerEnabled()));
        DataSize generalPoolSize = new DataSize(Math.max(0, maxMemory.toBytes() - config.getMaxQueryMemoryPerNode().toBytes()), BYTE);
        builder.put(GENERAL_POOL, createPool(GENERAL_POOL, generalPoolSize, exporter, config.isClusterMemoryManagerEnabled()));
        this.pools = builder.build();
    }

    public MemoryInfo getInfo()
    {
        ImmutableMap.Builder<MemoryPoolId, MemoryPoolInfo> builder = ImmutableMap.builder();
        for (Map.Entry<MemoryPoolId, MemoryPool> entry : pools.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getInfo());
        }
        return new MemoryInfo(maxMemory, builder.build());
    }

    @VisibleForTesting
    public List<MemoryPool> getPools()
    {
        return ImmutableList.copyOf(pools.values());
    }

    public MemoryPool getPool(MemoryPoolId id)
    {
        return pools.get(id);
    }

    private static MemoryPool createPool(MemoryPoolId id, DataSize size, MBeanExporter exporter, boolean enableBlocking)
    {
        MemoryPool pool = new MemoryPool(id, size, enableBlocking);
        try {
            String objectName = ObjectNames.builder(MemoryPool.class, pool.getId().toString()).build();
            exporter.export(objectName, pool);
        }
        catch (JmxException e) {
            log.warn(e, "Unable to export memory pool %s", id);
        }
        return pool;
    }

    @PreDestroy
    public void destroy()
    {
        for (MemoryPool pool : pools.values()) {
            String objectName = ObjectNames.builder(MemoryPool.class, pool.getId().toString()).build();
            try {
                exporter.unexport(objectName);
            }
            catch (JmxException e) {
                log.warn(e, "Unable to unexport memory pool %s", pool.getId());
            }
        }
    }
}
