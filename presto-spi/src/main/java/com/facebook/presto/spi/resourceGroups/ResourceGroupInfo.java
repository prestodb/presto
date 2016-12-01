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
package com.facebook.presto.spi.resourceGroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;

import java.util.Collections;
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

    @JsonCreator
    public ResourceGroupInfo(
            @JsonProperty("id") ResourceGroupId id,
            @JsonProperty("softMemoryLimit")DataSize softMemoryLimit,
            @JsonProperty("maxRunningQueries")int maxRunningQueries,
            @JsonProperty("maxQueuedQueries")int maxQueuedQueries,
            @JsonProperty("runningQueries")int runningQueries,
            @JsonProperty("queuedQueries")int queuedQueries,
            @JsonProperty("memoryUsage")DataSize memoryUsage,
            @JsonProperty("subGroups") List<ResourceGroupInfo> subGroups)
    {
        this.id = id;
        this.softMemoryLimit = requireNonNull(softMemoryLimit, "softMemoryLimit is null");
        this.maxRunningQueries = maxRunningQueries;
        this.maxQueuedQueries = maxQueuedQueries;
        this.runningQueries = runningQueries;
        this.queuedQueries = queuedQueries;
        this.memoryUsage = requireNonNull(memoryUsage, "memoryUsage is null");
        this.subGroups = Collections.unmodifiableList(requireNonNull(subGroups, "subGroups is null"));
    }

    @JsonProperty
    public ResourceGroupId getId()
    {
        return id;
    }

    @JsonProperty
    public DataSize getSoftMemoryLimit()
    {
        return softMemoryLimit;
    }

    @JsonProperty
    public int getMaxRunningQueries()
    {
        return maxRunningQueries;
    }

    @JsonProperty
    public int getMaxQueuedQueries()
    {
        return maxQueuedQueries;
    }

    @JsonProperty
    public List<ResourceGroupInfo> getSubGroups()
    {
        return subGroups;
    }

    @JsonProperty
    public int getRunningQueries()
    {
        return runningQueries;
    }

    @JsonProperty
    public int getQueuedQueries()
    {
        return queuedQueries;
    }

    @JsonProperty
    public DataSize getMemoryUsage()
    {
        return memoryUsage;
    }
}
