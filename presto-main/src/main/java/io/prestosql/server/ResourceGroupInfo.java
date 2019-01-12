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
package io.prestosql.server;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.resourcegroups.ResourceGroupState;
import io.prestosql.spi.resourcegroups.SchedulingPolicy;

import javax.annotation.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/*
 * This class is exposed to external systems via ResourceGroupStateInfoResource and QueryStateInfoResource.
 * Be careful while changing it.
 */
public class ResourceGroupInfo
{
    private final ResourceGroupId id;
    private final ResourceGroupState state;

    private final SchedulingPolicy schedulingPolicy;
    private final int schedulingWeight;

    private final DataSize softMemoryLimit;
    private final int softConcurrencyLimit;
    private final int hardConcurrencyLimit;
    private final int maxQueuedQueries;

    private final DataSize memoryUsage;
    private final int numQueuedQueries;
    private final int numRunningQueries;
    private final int numEligibleSubGroups;

    // Summaries do not include the following fields
    private final List<ResourceGroupInfo> subGroups;
    private final List<QueryStateInfo> runningQueries;

    public ResourceGroupInfo(
            ResourceGroupId id,
            ResourceGroupState state,

            SchedulingPolicy schedulingPolicy,
            int schedulingWeight,

            DataSize softMemoryLimit,
            int softConcurrencyLimit,
            int hardConcurrencyLimit,
            int maxQueuedQueries,

            DataSize memoryUsage,
            int numQueuedQueries,
            int numRunningQueries,
            int numEligibleSubGroups,

            List<ResourceGroupInfo> subGroups,

            List<QueryStateInfo> runningQueries)
    {
        this.id = requireNonNull(id, "id is null");
        this.state = requireNonNull(state, "state is null");

        this.schedulingPolicy = requireNonNull(schedulingPolicy, "schedulingPolicy is null");
        this.schedulingWeight = schedulingWeight;

        this.softMemoryLimit = requireNonNull(softMemoryLimit, "softMemoryLimit is null");

        this.softConcurrencyLimit = softConcurrencyLimit;
        this.hardConcurrencyLimit = hardConcurrencyLimit;
        this.maxQueuedQueries = maxQueuedQueries;

        this.memoryUsage = requireNonNull(memoryUsage, "memoryUsage is null");
        this.numQueuedQueries = numQueuedQueries;
        this.numRunningQueries = numRunningQueries;
        this.numEligibleSubGroups = numEligibleSubGroups;

        this.runningQueries = runningQueries;

        this.subGroups = subGroups;
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
    public SchedulingPolicy getSchedulingPolicy()
    {
        return schedulingPolicy;
    }

    @JsonProperty
    public int getSchedulingWeight()
    {
        return schedulingWeight;
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
    public int getMaxQueuedQueries()
    {
        return maxQueuedQueries;
    }

    @JsonProperty
    public int getNumQueuedQueries()
    {
        return numQueuedQueries;
    }

    @JsonProperty
    public int getNumRunningQueries()
    {
        return numRunningQueries;
    }

    @JsonProperty
    @Deprecated
    public int numAggregatedQueuedQueries()
    {
        return numQueuedQueries;
    }

    @JsonProperty
    @Deprecated
    public int numAggregatedRunningQueries()
    {
        return numRunningQueries;
    }

    @JsonProperty
    public int getNumEligibleSubGroups()
    {
        return numEligibleSubGroups;
    }

    @JsonProperty
    @Nullable
    public List<QueryStateInfo> getRunningQueries()
    {
        return runningQueries;
    }

    @JsonProperty
    @Nullable
    public List<ResourceGroupInfo> getSubGroups()
    {
        return subGroups;
    }
}
