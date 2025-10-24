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
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

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
    private int maxSplitsPerTask = 10;
    private boolean scheduleSplitsBasedOnTaskLoad;
    private int maxPendingSplitsPerTask = 10;
    private int maxUnacknowledgedSplitsPerTask = 500;
    private String networkTopology = NetworkTopologyType.LEGACY;
    private NodeSelectionHashStrategy nodeSelectionHashStrategy = NodeSelectionHashStrategy.MODULAR_HASHING;
    private int minVirtualNodeCount = 1000;
    private ResourceAwareSchedulingStrategy resourceAwareSchedulingStrategy = ResourceAwareSchedulingStrategy.RANDOM;
    private int maxPreferredNodes = 2;

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
    @ConfigDescription("The number of splits weighted at the standard split weight that can be assigned and queued for each task")
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
    @ConfigDescription("The number of splits weighted at the standard split weight that are allowed to be scheduled on each node")
    public NodeSchedulerConfig setMaxSplitsPerNode(int maxSplitsPerNode)
    {
        this.maxSplitsPerNode = maxSplitsPerNode;
        return this;
    }

    public int getMaxSplitsPerTask()
    {
        return maxSplitsPerTask;
    }

    @Config("node-scheduler.max-splits-per-task")
    @ConfigDescription("The number of splits weighted at the standard split weight that are allowed to be scheduled for each task " +
            "when scheduling splits based on the task load.")
    public NodeSchedulerConfig setMaxSplitsPerTask(int maxSplitsPerTask)
    {
        this.maxSplitsPerTask = maxSplitsPerTask;
        return this;
    }

    public boolean isScheduleSplitsBasedOnTaskLoad()
    {
        return scheduleSplitsBasedOnTaskLoad;
    }

    @Config("node-scheduler.schedule-splits-based-on-task-load")
    @ConfigDescription("Schedule splits based on task load, rather than on the node load")
    public NodeSchedulerConfig setScheduleSplitsBasedOnTaskLoad(boolean scheduleSplitsBasedOnTaskLoad)
    {
        this.scheduleSplitsBasedOnTaskLoad = scheduleSplitsBasedOnTaskLoad;
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

    public NodeSelectionHashStrategy getNodeSelectionHashStrategy()
    {
        return nodeSelectionHashStrategy;
    }

    @Config("node-scheduler.node-selection-hash-strategy")
    @ConfigDescription("Hashing strategy used for node selection when scheduling splits to nodes. Options are MODULAR_HASHING, CONSISTENT_HASHING")
    public NodeSchedulerConfig setNodeSelectionHashStrategy(NodeSelectionHashStrategy nodeSelectionHashStrategy)
    {
        this.nodeSelectionHashStrategy = nodeSelectionHashStrategy;
        return this;
    }

    public int getMinVirtualNodeCount()
    {
        return minVirtualNodeCount;
    }

    @Config("node-scheduler.consistent-hashing-min-virtual-node-count")
    @ConfigDescription("When CONSISTENT_HASHING node selection hash strategy is used, the minimum number of virtual node count. Default 1000. " +
            "The actual virtual node count is guaranteed to be larger than this number. When number is smaller than physical node count, " +
            "physical node is used, otherwise the smallest multiplier of physical node count that is greater than this value is used.")
    public NodeSchedulerConfig setMinVirtualNodeCount(int minVirtualNodeCount)
    {
        this.minVirtualNodeCount = minVirtualNodeCount;
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

    @Min(1)
    public int getMaxPreferredNodes()
    {
        return maxPreferredNodes;
    }

    @Config("node-scheduler.max-preferred-nodes")
    public NodeSchedulerConfig setMaxPreferredNodes(int maxPreferredNodes)
    {
        this.maxPreferredNodes = maxPreferredNodes;
        return this;
    }

    public enum ResourceAwareSchedulingStrategy
    {
        RANDOM,
        TTL
    }
}
