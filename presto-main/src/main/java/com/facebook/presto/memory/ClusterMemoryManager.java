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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.execution.QueryExecution;
import io.airlift.units.DataSize;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

public class ClusterMemoryManager
{
    private final DataSize maxQueryMemory;
    private final boolean enabled;
    private final AtomicLong clusterMemoryUsageBytes = new AtomicLong();

    @Inject
    public ClusterMemoryManager(MemoryManagerConfig config)
    {
        requireNonNull(config, "config is null");
        this.maxQueryMemory = config.getMaxQueryMemory();
        this.enabled = config.isClusterMemoryManagerEnabled();
    }

    public void process(Iterable<QueryExecution> queries)
    {
        if (!enabled) {
            return;
        }
        long totalBytes = 0;
        for (QueryExecution query : queries) {
            long bytes = query.getQueryInfo().getQueryStats().getTotalMemoryReservation().toBytes();
            totalBytes += bytes;
            if (bytes > maxQueryMemory.toBytes()) {
                query.fail(new ExceededMemoryLimitException("Query", maxQueryMemory));
            }
        }
        clusterMemoryUsageBytes.set(totalBytes);
    }

    @Managed
    public long getClusterMemoryUsageBytes()
    {
        return clusterMemoryUsageBytes.get();
    }
}
