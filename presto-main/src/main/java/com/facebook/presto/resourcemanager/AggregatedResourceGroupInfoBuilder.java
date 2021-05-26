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
package com.facebook.presto.resourcemanager;

import com.facebook.presto.server.QueryStateInfo;
import com.facebook.presto.server.ResourceGroupInfo;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupState;
import com.facebook.presto.spi.resourceGroups.SchedulingPolicy;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_QUEUE;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.CAN_RUN;
import static com.facebook.presto.spi.resourceGroups.ResourceGroupState.FULL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Math.addExact;
import static java.util.Objects.requireNonNull;

public class AggregatedResourceGroupInfoBuilder
{
    private ResourceGroupId id;
    private SchedulingPolicy schedulingPolicy;
    private int schedulingWeight;
    private Map<ResourceGroupId, AggregatedResourceGroupInfoBuilder> subGroupsMap;
    private ImmutableList.Builder<QueryStateInfo> runningQueriesBuilder;
    private static final Map<ResourceGroupState, Integer> resourceGroupStatePreference
            = ImmutableMap.of(FULL, 1, CAN_QUEUE, 2, CAN_RUN, 3);
    private ResourceGroupState state;
    private DataSize softMemoryLimit;
    private int softConcurrencyLimit;
    private int hardConcurrencyLimit;
    private int maxQueuedQueries;
    private long memoryUsageBytes;
    private int numQueuedQueries;

    private void init(ResourceGroupInfo resourceGroupInfo)
    {
        this.id = requireNonNull(resourceGroupInfo.getId(), "id is null");
        this.state = requireNonNull(resourceGroupInfo.getState(), "state is null");
        this.schedulingPolicy = resourceGroupInfo.getSchedulingPolicy();
        this.schedulingWeight = resourceGroupInfo.getSchedulingWeight();
        this.softMemoryLimit = resourceGroupInfo.getSoftMemoryLimit();
        this.softConcurrencyLimit = resourceGroupInfo.getSoftConcurrencyLimit();
        this.hardConcurrencyLimit = resourceGroupInfo.getHardConcurrencyLimit();
        this.maxQueuedQueries = resourceGroupInfo.getMaxQueuedQueries();
        this.memoryUsageBytes = resourceGroupInfo.getMemoryUsage().toBytes();
        this.numQueuedQueries = resourceGroupInfo.getNumQueuedQueries();
        this.subGroupsMap = new HashMap<>();
        this.runningQueriesBuilder = ImmutableList.builder();
        addRunningQueries(resourceGroupInfo.getRunningQueries());
        addSubgroups(resourceGroupInfo.getSubGroups());
    }

    public AggregatedResourceGroupInfoBuilder add(ResourceGroupInfo resourceGroupInfo)
    {
        if (this.id == null) {
            init(resourceGroupInfo);
            return this;
        }
        checkState(resourceGroupInfo != null && this.id.equals(resourceGroupInfo.getId()));
        this.numQueuedQueries = addExact(this.numQueuedQueries, resourceGroupInfo.getNumQueuedQueries());
        if (resourceGroupStatePreference.get(resourceGroupInfo.getState()) < resourceGroupStatePreference.get(this.state)) {
            this.state = resourceGroupInfo.getState();
        }
        this.memoryUsageBytes = addExact(this.memoryUsageBytes, resourceGroupInfo.getMemoryUsage().toBytes());
        List<ResourceGroupInfo> subGroups = resourceGroupInfo.getSubGroups();
        addSubgroups(subGroups);

        List<QueryStateInfo> runningQueries = resourceGroupInfo.getRunningQueries();
        addRunningQueries(runningQueries);
        return this;
    }

    private void addSubgroups(List<ResourceGroupInfo> subGroups)
    {
        if (subGroups == null) {
            return;
        }
        for (ResourceGroupInfo subgroup : subGroups) {
            subGroupsMap.computeIfAbsent(subgroup.getId(), k -> new AggregatedResourceGroupInfoBuilder()).add(subgroup);
        }
    }

    private void addRunningQueries(List<QueryStateInfo> runningQueries)
    {
        if (runningQueries == null) {
            return;
        }
        this.runningQueriesBuilder.addAll(runningQueries);
    }

    public ResourceGroupInfo build()
    {
        if (this.id == null) {
            return null;
        }
        ImmutableList<QueryStateInfo> runningQueries = runningQueriesBuilder.build();
        return new ResourceGroupInfo(
                id,
                state,
                schedulingPolicy,
                schedulingWeight,
                softMemoryLimit,
                softConcurrencyLimit,
                hardConcurrencyLimit,
                maxQueuedQueries,
                DataSize.succinctBytes(memoryUsageBytes),
                numQueuedQueries,
                runningQueries.size(),
                0,
                subGroupsMap.values().stream().map(AggregatedResourceGroupInfoBuilder::build).collect(toImmutableList()),
                runningQueries);
    }
}
