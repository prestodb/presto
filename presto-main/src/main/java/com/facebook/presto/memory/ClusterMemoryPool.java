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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;

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
    private int queries;

    public ClusterMemoryPool(MemoryPoolId id)
    {
        this.id = requireNonNull(id, "id is null");
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
    public synchronized int getQueries()
    {
        return queries;
    }

    public synchronized void update(List<MemoryInfo> memoryInfos, int queries)
    {
        nodes = 0;
        blockedNodes = 0;
        totalDistributedBytes = 0;
        freeDistributedBytes = 0;
        this.queries = queries;

        for (MemoryInfo info : memoryInfos) {
            MemoryPoolInfo poolInfo = info.getPools().get(id);
            if (poolInfo != null) {
                nodes++;
                if (poolInfo.getFreeBytes() <= 0) {
                    blockedNodes++;
                }
                totalDistributedBytes += poolInfo.getMaxBytes();
                freeDistributedBytes += poolInfo.getFreeBytes();
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
        return Objects.equal(id, that.id);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(id);
    }

    @Override
    public synchronized String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("totalDistributedBytes", totalDistributedBytes)
                .add("freeDistributedBytes", freeDistributedBytes)
                .add("nodes", nodes)
                .add("blockedNodes", blockedNodes)
                .add("queries", queries)
                .toString();
    }
}
