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
import io.airlift.units.DataSize;

import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class MemoryManagerConfig
{
    private DataSize maxQueryMemory = new DataSize(20, GIGABYTE);
    private DataSize maxQueryMemoryPerNode = new DataSize(1, GIGABYTE);
    private boolean clusterMemoryManagerEnabled;

    @NotNull
    public DataSize getMaxQueryMemory()
    {
        return maxQueryMemory;
    }

    @Config("query.max-memory")
    public MemoryManagerConfig setMaxQueryMemory(DataSize maxQueryMemory)
    {
        this.maxQueryMemory = maxQueryMemory;
        return this;
    }

    @NotNull
    public DataSize getMaxQueryMemoryPerNode()
    {
        return maxQueryMemoryPerNode;
    }

    @Config("query.max-memory-per-node")
    public MemoryManagerConfig setMaxQueryMemoryPerNode(DataSize maxQueryMemoryPerNode)
    {
        this.maxQueryMemoryPerNode = maxQueryMemoryPerNode;
        return this;
    }

    public boolean isClusterMemoryManagerEnabled()
    {
        return clusterMemoryManagerEnabled;
    }

    @Config("experimental.cluster-memory-manager-enabled")
    public MemoryManagerConfig setClusterMemoryManagerEnabled(boolean clusterMemoryManagerEnabled)
    {
        this.clusterMemoryManagerEnabled = clusterMemoryManagerEnabled;
        return this;
    }
}
