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
package com.facebook.presto.server;

import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupInfo;
import com.facebook.presto.spi.resourceGroups.ResourceGroupState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ResourceGroupStateInfo
{
    private final ResourceGroupId id;
    private final ResourceGroupState state;

    private final DataSize softMemoryLimit;
    private final DataSize memoryUsage;

    private final List<ResourceGroupInfo> subGroups;

    private final int softConcurrencyLimit;
    private final int hardConcurrencyLimit;
    private final int maxQueuedQueries;
    private final Duration runningTimeLimit;
    private final Duration queuedTimeLimit;
    private final List<QueryStateInfo> runningQueries;
    private final int numQueuedQueries;

    @JsonCreator
    public ResourceGroupStateInfo(
            @JsonProperty("id") ResourceGroupId id,
            @JsonProperty("state") ResourceGroupState state,
            @JsonProperty("softMemoryLimit") DataSize softMemoryLimit,
            @JsonProperty("memoryUsage") DataSize memoryUsage,
            @JsonProperty("softConcurrencyLimit") int softConcurrencyLimit,
            @JsonProperty("hardConcurrencyLimit") int hardConcurrencyLimit,
            @JsonProperty("maxQueuedQueries") int maxQueuedQueries,
            @JsonProperty("runningTimeLimit") Duration runningTimeLimit,
            @JsonProperty("queuedTimeLimit") Duration queuedTimeLimit,
            @JsonProperty("runningQueries") List<QueryStateInfo> runningQueries,
            @JsonProperty("numQueuedQueries") int numQueuedQueries,
            @JsonProperty("subGroups") List<ResourceGroupInfo> subGroups)
    {
        this.id = requireNonNull(id, "id is null");
        this.state = requireNonNull(state, "state is null");

        this.softMemoryLimit = requireNonNull(softMemoryLimit, "softMemoryLimit is null");
        this.memoryUsage = requireNonNull(memoryUsage, "memoryUsage is null");

        this.softConcurrencyLimit = softConcurrencyLimit;
        this.hardConcurrencyLimit = hardConcurrencyLimit;
        this.maxQueuedQueries = maxQueuedQueries;

        this.runningTimeLimit = requireNonNull(runningTimeLimit, "runningTimeLimit is null");
        this.queuedTimeLimit = requireNonNull(queuedTimeLimit, "queuedTimeLimit is null");

        this.runningQueries = ImmutableList.copyOf(requireNonNull(runningQueries, "runningQueries is null"));
        this.numQueuedQueries = numQueuedQueries;

        this.subGroups = ImmutableList.copyOf(requireNonNull(subGroups, "subGroups is null"));
    }

    @JsonProperty
    public ResourceGroupId getId()
    {
        return id;
    }

    @JsonProperty
    public ResourceGroupState getState()
    {
        return state;
    }

    @JsonProperty
    public DataSize getSoftMemoryLimit()
    {
        return softMemoryLimit;
    }

    @JsonProperty
    public DataSize getMemoryUsage()
    {
        return memoryUsage;
    }

    @JsonProperty
    public int getSoftConcurrencyLimit()
    {
        return softConcurrencyLimit;
    }

    @JsonProperty
    public int getHardConcurrencyLimit()
    {
        return hardConcurrencyLimit;
    }

    @JsonProperty
    @Deprecated
    public int getMaxRunningQueries()
    {
        // TODO: Remove when dependent tools are updated
        return hardConcurrencyLimit;
    }

    @JsonProperty
    public int getMaxQueuedQueries()
    {
        return maxQueuedQueries;
    }

    @JsonProperty
    public Duration getQueuedTimeLimit()
    {
        return queuedTimeLimit;
    }

    @JsonProperty
    public Duration getRunningTimeLimit()
    {
        return runningTimeLimit;
    }

    @JsonProperty
    public List<QueryStateInfo> getRunningQueries()
    {
        return runningQueries;
    }

    @JsonProperty
    public int getNumQueuedQueries()
    {
        return numQueuedQueries;
    }

    @JsonProperty
    public List<ResourceGroupInfo> getSubGroups()
    {
        return subGroups;
    }
}
