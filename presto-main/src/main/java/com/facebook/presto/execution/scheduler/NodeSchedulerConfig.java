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

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.configuration.DefunctConfig;
import com.facebook.airlift.configuration.LegacyConfig;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@DefunctConfig({"node-scheduler.location-aware-scheduling-enabled", "node-scheduler.multiple-tasks-per-node-enabled"})
public class NodeSchedulerConfig
{
    public static class NetworkTopologyType
    {
        public static final String LEGACY = "legacy";
        public static final String FLAT = "flat";
        public static final String BENCHMARK = "benchmark";
    }

    private int minCandidates = 10;
    private boolean includeCoordinator = true;
    private int maxSplitsPerNode = 100;
    private int maxPendingSplitsPerTask = 10;
    private int maxUnacknowledgedSplitsPerTask = 500;
    private String networkTopology = NetworkTopologyType.LEGACY;
    private ResourceAwareSchedulingStrategy resourceAwareSchedulingStrategy = ResourceAwareSchedulingStrategy.RANDOM;

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

    @Config("node-scheduler.max-pending-splits-per-task")
    @LegacyConfig({"node-scheduler.max-pending-splits-per-node-per-task", "node-scheduler.max-pending-splits-per-node-per-stage"})
    public NodeSchedulerConfig setMaxPendingSplitsPerTask(int maxPendingSplitsPerTask)
    {
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        return this;
    }

    public int getMaxPendingSplitsPerTask()
    {
        return maxPendingSplitsPerTask;
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

    @Min(1)
    public int getMaxUnacknowledgedSplitsPerTask()
    {
        return maxUnacknowledgedSplitsPerTask;
    }

    @Config("node-scheduler.max-unacknowledged-splits-per-task")
    @ConfigDescription("Maximum number of leaf splits not yet delivered to a given task")
    public NodeSchedulerConfig setMaxUnacknowledgedSplitsPerTask(int maxUnacknowledgedSplitsPerTask)
    {
        this.maxUnacknowledgedSplitsPerTask = maxUnacknowledgedSplitsPerTask;
        return this;
    }

    public ResourceAwareSchedulingStrategy getResourceAwareSchedulingStrategy()
    {
        return resourceAwareSchedulingStrategy;
    }

    @Config("experimental.resource-aware-scheduling-strategy")
    public NodeSchedulerConfig setResourceAwareSchedulingStrategy(ResourceAwareSchedulingStrategy resourceAwareSchedulingStrategy)
    {
        this.resourceAwareSchedulingStrategy = resourceAwareSchedulingStrategy;
        return this;
    }

    public enum ResourceAwareSchedulingStrategy
    {
        RANDOM,
        TTL
    }
}
