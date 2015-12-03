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
package com.facebook.presto.execution.scheduler;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@DefunctConfig("node-scheduler.location-aware-scheduling-enabled")
public class NodeSchedulerConfig
{
    public static final String LEGACY_NETWORK_TOPOLOGY = "legacy";
    private int minCandidates = 10;
    private boolean includeCoordinator = true;
    private boolean multipleTasksPerNode;
    private int maxSplitsPerNode = 100;
    private int maxPendingSplitsPerNodePerTask = 10;
    private String networkTopology = LEGACY_NETWORK_TOPOLOGY;

    @NotNull
    public String getNetworkTopology()
    {
        return networkTopology;
    }

    @Config("node-scheduler.network-topology")
    public NodeSchedulerConfig setNetworkTopology(String networkTopology)
    {
        this.networkTopology = networkTopology;
        return this;
    }

    public boolean isMultipleTasksPerNodeEnabled()
    {
        return multipleTasksPerNode;
    }

    @ConfigDescription("Allow nodes to be selected multiple times by the node scheduler, in a single stage")
    @Config("node-scheduler.multiple-tasks-per-node-enabled")
    public NodeSchedulerConfig setMultipleTasksPerNodeEnabled(boolean multipleTasksPerNode)
    {
        this.multipleTasksPerNode = multipleTasksPerNode;
        return this;
    }

    @Min(1)
    public int getMinCandidates()
    {
        return minCandidates;
    }

    @Config("node-scheduler.min-candidates")
    public NodeSchedulerConfig setMinCandidates(int candidates)
    {
        this.minCandidates = candidates;
        return this;
    }

    public boolean isIncludeCoordinator()
    {
        return includeCoordinator;
    }

    @Config("node-scheduler.include-coordinator")
    public NodeSchedulerConfig setIncludeCoordinator(boolean includeCoordinator)
    {
        this.includeCoordinator = includeCoordinator;
        return this;
    }

    @Config("node-scheduler.max-pending-splits-per-node-per-task")
    public NodeSchedulerConfig setMaxPendingSplitsPerNodePerTask(int maxPendingSplitsPerNodePerTask)
    {
        this.maxPendingSplitsPerNodePerTask = maxPendingSplitsPerNodePerTask;
        return this;
    }

    public int getMaxPendingSplitsPerNodePerTask()
    {
        return maxPendingSplitsPerNodePerTask;
    }

    public int getMaxSplitsPerNode()
    {
        return maxSplitsPerNode;
    }

    @Config("node-scheduler.max-splits-per-node")
    public NodeSchedulerConfig setMaxSplitsPerNode(int maxSplitsPerNode)
    {
        this.maxSplitsPerNode = maxSplitsPerNode;
        return this;
    }
}
