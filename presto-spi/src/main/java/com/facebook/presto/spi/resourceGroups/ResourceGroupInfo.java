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
import io.airlift.units.Duration;

import java.util.List;
import java.util.Optional;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class ResourceGroupInfo
{
    private final ResourceGroupId id;

    private final DataSize softMemoryLimit;
    private final int softConcurrencyLimit;
    private final int hardConcurrencyLimit;
    private final Duration runningTimeLimit;
    private final int maxQueuedQueries;
    private final Duration queuedTimeLimit;

    private final ResourceGroupState state;
    private final int numEligibleSubGroups;
    private final DataSize memoryUsage;
    private final int numAggregatedRunningQueries;
    private final int numAggregatedQueuedQueries;

    private final List<ResourceGroupInfo> subGroups;

    @JsonCreator
    public ResourceGroupInfo(
            @JsonProperty("id") ResourceGroupId id,
            @JsonProperty("softMemoryLimit") DataSize softMemoryLimit,
            @JsonProperty("softConcurrencyLimit") int softConcurrencyLimit,
            @JsonProperty("hardConcurrencyLimit") int hardConcurrencyLimit,
            @JsonProperty("runningTimeLimit") Duration runningTimeLimit,
            @JsonProperty("maxQueuedQueries") int maxQueuedQueries,
            @JsonProperty("queuedTimeLimit") Duration queuedTimeLimit,
            @JsonProperty("state") ResourceGroupState state,
            @JsonProperty("numEligibleSubGroups") int numEligibleSubGroups,
            @JsonProperty("memoryUsage") DataSize memoryUsage,
            @JsonProperty("numAggregatedRunningQueries") int numAggregatedRunningQueries,
            @JsonProperty("numAggregatedQueuedQueries") int numAggregatedQueuedQueries)
    {
        this(id, softMemoryLimit, softConcurrencyLimit, hardConcurrencyLimit, runningTimeLimit, maxQueuedQueries, queuedTimeLimit, state, numEligibleSubGroups, memoryUsage, numAggregatedRunningQueries, numAggregatedQueuedQueries, emptyList());
    }

    public ResourceGroupInfo(
            ResourceGroupId id,
            DataSize softMemoryLimit,
            int softConcurrencyLimit,
            int hardConcurrencyLimit,
            Duration runningTimeLimit,
            int maxQueuedQueries,
            Duration queuedTimeLimit,
            ResourceGroupState state,
            int numEligibleSubGroups,
            DataSize memoryUsage,
            int numAggregatedRunningQueries,
            int numAggregatedQueuedQueries,
            List<ResourceGroupInfo> subGroups)
    {
        this.id = requireNonNull(id, "id is null");
        this.softMemoryLimit = requireNonNull(softMemoryLimit, "softMemoryLimit is null");
        this.softConcurrencyLimit = softConcurrencyLimit;
        this.hardConcurrencyLimit = hardConcurrencyLimit;
        this.runningTimeLimit = runningTimeLimit;
        this.maxQueuedQueries = maxQueuedQueries;
        this.queuedTimeLimit = queuedTimeLimit;
        this.state = requireNonNull(state, "state is null");
        this.numEligibleSubGroups = numEligibleSubGroups;
        this.memoryUsage = requireNonNull(memoryUsage, "memoryUsage is null");
        this.numAggregatedRunningQueries = numAggregatedRunningQueries;
        this.numAggregatedQueuedQueries = numAggregatedQueuedQueries;
        this.subGroups = unmodifiableList(requireNonNull(subGroups, "subGroups is null"));
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
    public Duration getRunningTimeLimit()
    {
        return runningTimeLimit;
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

    public List<ResourceGroupInfo> getSubGroups()
    {
        return subGroups;
    }

    @JsonProperty
    public DataSize getMemoryUsage()
    {
        return memoryUsage;
    }

    public Optional<ResourceGroupInfo> getSubGroup(ResourceGroupId resourceGroupId)
    {
        return subGroups.stream()
                .filter(subGroup -> subGroup.getId().equals(resourceGroupId) || subGroup.getId().isAncestorOf(resourceGroupId))
                .findFirst();
    }

    @JsonProperty
    public int getNumAggregatedRunningQueries()
    {
        return numAggregatedRunningQueries;
    }

    @JsonProperty
    public int getNumAggregatedQueuedQueries()
    {
        return numAggregatedQueuedQueries;
    }

    @JsonProperty
    public ResourceGroupState getState()
    {
        return state;
    }

    @JsonProperty
    public int getNumEligibleSubGroups()
    {
        return numEligibleSubGroups;
    }

    public ResourceGroupInfo createSingleNodeInfo()
    {
        return new ResourceGroupInfo(
                getId(),
                getSoftMemoryLimit(),
                getSoftConcurrencyLimit(),
                getHardConcurrencyLimit(),
                getRunningTimeLimit(),
                getMaxQueuedQueries(),
                getQueuedTimeLimit(),
                getState(),
                getNumEligibleSubGroups(),
                getMemoryUsage(),
                getNumAggregatedRunningQueries(),
                getNumAggregatedQueuedQueries());
    }
}
