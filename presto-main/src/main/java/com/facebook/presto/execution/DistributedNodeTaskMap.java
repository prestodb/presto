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
package com.facebook.presto.execution;

import com.facebook.drift.client.DriftClient;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.resourcemanager.ForResourceManager;
import com.facebook.presto.resourcemanager.NodeTaskState;
import com.facebook.presto.resourcemanager.ResourceManagerClient;
import com.facebook.presto.resourcemanager.ResourceManagerConfig;
import com.facebook.presto.util.PeriodicTaskExecutor;
import com.google.common.collect.ImmutableMap;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class DistributedNodeTaskMap
        implements NodeTaskMap
{
    public static final NodeTaskState EMPTY_NODE_TASK_STATE = new NodeTaskState(PartitionedSplitsInfo.forZeroSplits(), 0, 0);
    private final DriftClient<ResourceManagerClient> resourceManagerClient;
    private final long nodeTaskMapFetchIntervalMillis;
    private final PeriodicTaskExecutor nodeTaskMapUpdater;

    private final AtomicReference<Map<String, NodeTaskState>> nodeTaskStateMap;

    @Inject
    public DistributedNodeTaskMap(
            @ForResourceManager DriftClient<ResourceManagerClient> resourceManagerClient,
            @ForResourceManager ScheduledExecutorService executorService,
            ResourceManagerConfig resourceManagerConfig)
    {
        this.resourceManagerClient = requireNonNull(resourceManagerClient, "resourceManagerClient is null");
        this.nodeTaskMapFetchIntervalMillis = requireNonNull(resourceManagerConfig, "resourceManagerConfig is null").getNodeTaskMapFetchInterval().toMillis();
        nodeTaskStateMap = new AtomicReference<>(ImmutableMap.of());
        this.nodeTaskMapUpdater = new PeriodicTaskExecutor(nodeTaskMapFetchIntervalMillis, executorService, () -> nodeTaskStateMap.set(updateNodeTaskStateMap()));
    }

    @PostConstruct
    public void init()
    {
        nodeTaskMapUpdater.start();
    }

    @PreDestroy
    public void stop()
    {
        nodeTaskMapUpdater.stop();
    }

    @Override
    public void addTask(InternalNode node, RemoteTask task)
    {
        // no op
    }

    @Override
    public PartitionedSplitsInfo getPartitionedSplitsOnNode(InternalNode node)
    {
        return nodeTaskStateMap.get().getOrDefault(node.getNodeIdentifier(), EMPTY_NODE_TASK_STATE).getPartitionedSplitsInfo();
    }

    @Override
    public long getNodeTotalMemoryUsageInBytes(InternalNode node)
    {
        return nodeTaskStateMap.get().getOrDefault(node.getNodeIdentifier(), EMPTY_NODE_TASK_STATE).getTotalMemoryUsageInBytes();
    }

    @Override
    public double getNodeCpuUtilizationPercentage(InternalNode node)
    {
        return nodeTaskStateMap.get().getOrDefault(node.getNodeIdentifier(), EMPTY_NODE_TASK_STATE).getCpuUtilizationPercentage();
    }

    @Override
    public NodeStatsTracker createTaskStatsTracker(InternalNode node, TaskId taskId)
    {
        return NOOP_NODE_STATS_TRACKER;
    }

    private Map<String, NodeTaskState> updateNodeTaskStateMap()
    {
        return resourceManagerClient.get().getNodeTaskStates();
    }

    public static final NodeStatsTracker NOOP_NODE_STATS_TRACKER = new NodeStatsTracker()
    {
        @Override
        public void setPartitionedSplits(PartitionedSplitsInfo partitionedSplits)
        {
            // no op
        }

        @Override
        public void setMemoryUsage(long memoryUsage)
        {
            // no op
        }

        @Override
        public void setCpuUsage(long age, long cpuUsage)
        {
            // no op
        }
    };
}
