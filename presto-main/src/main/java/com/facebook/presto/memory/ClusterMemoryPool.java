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

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.memory.MemoryPoolInfo;
import com.google.common.collect.ImmutableMap;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class ClusterMemoryPool
{
    private final MemoryPoolId id;

    @GuardedBy("this")
    private long totalDistributedBytes;

    @GuardedBy("this")
    private long freeDistributedBytes;

    @GuardedBy("this")
    private int nodes;

    @GuardedBy("this")
    private int blockedNodes;

    @GuardedBy("this")
    private int assignedQueries;

    // Does not include queries with zero memory usage
    @GuardedBy("this")
    private final Map<QueryId, Long> queryMemoryReservations = new HashMap<>();

    public ClusterMemoryPool(MemoryPoolId id)
    {
        this.id = requireNonNull(id, "id is null");
    }

    public synchronized MemoryPoolInfo getInfo()
    {
        return new MemoryPoolInfo(totalDistributedBytes, freeDistributedBytes, ImmutableMap.copyOf(queryMemoryReservations));
    }

    public MemoryPoolId getId()
    {
        return id;
    }

    @Managed
    public synchronized long getTotalDistributedBytes()
    {
        return totalDistributedBytes;
    }

    @Managed
    public synchronized long getFreeDistributedBytes()
    {
        return freeDistributedBytes;
    }

    @Managed
    public synchronized int getNodes()
    {
        return nodes;
    }

    @Managed
    public synchronized int getBlockedNodes()
    {
        return blockedNodes;
    }

    @Managed
    public synchronized int getAssignedQueries()
    {
        return assignedQueries;
    }

    public synchronized Map<QueryId, Long> getQueryMemoryReservations()
    {
        return queryMemoryReservations;
    }

    public synchronized void update(List<MemoryInfo> memoryInfos, int assignedQueries)
    {
        nodes = 0;
        blockedNodes = 0;
        totalDistributedBytes = 0;
        freeDistributedBytes = 0;
        this.assignedQueries = assignedQueries;
        this.queryMemoryReservations.clear();

        for (MemoryInfo info : memoryInfos) {
            MemoryPoolInfo poolInfo = info.getPools().get(id);
            if (poolInfo != null) {
                nodes++;
                if (poolInfo.getFreeBytes() <= 0) {
                    blockedNodes++;
                }
                totalDistributedBytes += poolInfo.getMaxBytes();
                freeDistributedBytes += poolInfo.getFreeBytes();
                for (Map.Entry<QueryId, Long> entry : poolInfo.getQueryMemoryReservations().entrySet()) {
                    queryMemoryReservations.merge(entry.getKey(), entry.getValue(), Long::sum);
                }
            }
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterMemoryPool that = (ClusterMemoryPool) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id);
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("totalDistributedBytes", totalDistributedBytes)
                .add("freeDistributedBytes", freeDistributedBytes)
                .add("nodes", nodes)
                .add("blockedNodes", blockedNodes)
                .add("assignedQueries", assignedQueries)
                .add("queryMemoryReservations", queryMemoryReservations)
                .toString();
    }
}
