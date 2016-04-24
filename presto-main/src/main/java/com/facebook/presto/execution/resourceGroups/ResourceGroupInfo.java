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
package com.facebook.presto.execution.resourceGroups;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ResourceGroupInfo
{
    private final ResourceGroupId id;

    private final DataSize softMemoryLimit;
    private final int maxRunningQueries;
    private final int maxQueuedQueries;
    private final int runningQueries;
    private final int queuedQueries;
    private final DataSize memoryUsage;
    private final List<ResourceGroupInfo> subGroups;

    public ResourceGroupInfo(
            ResourceGroupId id,
            DataSize softMemoryLimit,
            int maxRunningQueries,
            int maxQueuedQueries,
            int runningQueries,
            int queuedQueries,
            DataSize memoryUsage,
            List<ResourceGroupInfo> subGroups)
    {
        this.id = id;
        this.softMemoryLimit = requireNonNull(softMemoryLimit, "softMemoryLimit is null");
        this.maxRunningQueries = maxRunningQueries;
        this.maxQueuedQueries = maxQueuedQueries;
        this.runningQueries = runningQueries;
        this.queuedQueries = queuedQueries;
        this.memoryUsage = requireNonNull(memoryUsage, "memoryUsage is null");
        this.subGroups = ImmutableList.copyOf(requireNonNull(subGroups, "subGroups is null"));
    }

    public ResourceGroupId getId()
    {
        return id;
    }

    public DataSize getSoftMemoryLimit()
    {
        return softMemoryLimit;
    }

    public int getMaxRunningQueries()
    {
        return maxRunningQueries;
    }

    public int getMaxQueuedQueries()
    {
        return maxQueuedQueries;
    }

    public List<ResourceGroupInfo> getSubGroups()
    {
        return subGroups;
    }

    public int getRunningQueries()
    {
        return runningQueries;
    }

    public int getQueuedQueries()
    {
        return queuedQueries;
    }

    public DataSize getMemoryUsage()
    {
        return memoryUsage;
    }
}
