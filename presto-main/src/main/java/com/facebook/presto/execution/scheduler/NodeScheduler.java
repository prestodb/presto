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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleNodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.TopologyAwareNodeSelector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SplitContext;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.facebook.airlift.concurrent.MoreFutures.whenAnyCompleteCancelOthers;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NodeScheduler
{
    private final NetworkLocationCache networkLocationCache;
    private final List<CounterStat> topologicalSplitCounters;
    private final List<String> networkLocationSegmentNames;
    private final InternalNodeManager nodeManager;
    private final NodeSelectionStats nodeSelectionStats;
    private final int minCandidates;
    private final boolean includeCoordinator;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final NodeTaskMap nodeTaskMap;
    private final boolean useNetworkTopology;
    private final Duration nodeMapRefreshInterval;

    @Inject
    public NodeScheduler(NetworkTopology networkTopology, InternalNodeManager nodeManager, NodeSelectionStats nodeSelectionStats, NodeSchedulerConfig config, NodeTaskMap nodeTaskMap)
    {
        this(new NetworkLocationCache(networkTopology), networkTopology, nodeManager, nodeSelectionStats, config, nodeTaskMap, new Duration(5, SECONDS));
    }

    public NodeScheduler(
            NetworkLocationCache networkLocationCache,
            NetworkTopology networkTopology,
            InternalNodeManager nodeManager,
            NodeSelectionStats nodeSelectionStats,
            NodeSchedulerConfig config,
            NodeTaskMap nodeTaskMap,
            Duration nodeMapRefreshInterval)
    {
        this.networkLocationCache = networkLocationCache;
        this.nodeManager = nodeManager;
        this.nodeSelectionStats = requireNonNull(nodeSelectionStats, "nodeSelectionStats is null");
        this.minCandidates = config.getMinCandidates();
        this.includeCoordinator = config.isIncludeCoordinator();
        this.maxSplitsPerNode = config.getMaxSplitsPerNode();
        this.maxPendingSplitsPerTask = config.getMaxPendingSplitsPerTask();
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        checkArgument(maxSplitsPerNode >= maxPendingSplitsPerTask, "maxSplitsPerNode must be > maxPendingSplitsPerTask");
        this.useNetworkTopology = !config.getNetworkTopology().equals(NetworkTopologyType.LEGACY);

        ImmutableList.Builder<CounterStat> builder = ImmutableList.builder();
        if (useNetworkTopology) {
            networkLocationSegmentNames = ImmutableList.copyOf(networkTopology.getLocationSegmentNames());
            for (int i = 0; i < networkLocationSegmentNames.size() + 1; i++) {
                builder.add(new CounterStat());
            }
        }
        else {
            networkLocationSegmentNames = ImmutableList.of();
        }
        topologicalSplitCounters = builder.build();
        this.nodeMapRefreshInterval = requireNonNull(nodeMapRefreshInterval, "nodeMapRefreshInterval is null");
    }

    @PreDestroy
    public void stop()
    {
        networkLocationCache.stop();
    }

    public Map<String, CounterStat> getTopologicalSplitCounters()
    {
        ImmutableMap.Builder<String, CounterStat> counters = ImmutableMap.builder();
        for (int i = 0; i < topologicalSplitCounters.size(); i++) {
            counters.put(i == 0 ? "all" : networkLocationSegmentNames.get(i - 1), topologicalSplitCounters.get(i));
        }
        return counters.build();
    }

    public NodeSelector createNodeSelector(ConnectorId connectorId)
    {
        return createNodeSelector(connectorId, Integer.MAX_VALUE);
    }

    public NodeSelector createNodeSelector(ConnectorId connectorId, int maxTasksPerStage)
    {
        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the the split is about to be scheduled
        Supplier<NodeMap> nodeMap = nodeMapRefreshInterval.toMillis() > 0 ?
                memoizeWithExpiration(createNodeMapSupplier(connectorId), nodeMapRefreshInterval.toMillis(), MILLISECONDS) : createNodeMapSupplier(connectorId);

        if (useNetworkTopology) {
            return new TopologyAwareNodeSelector(
                    nodeManager,
                    nodeSelectionStats,
                    nodeTaskMap,
                    includeCoordinator,
                    nodeMap,
                    minCandidates,
                    maxSplitsPerNode,
                    maxPendingSplitsPerTask,
                    topologicalSplitCounters,
                    networkLocationSegmentNames,
                    networkLocationCache);
        }
        else {
            return new SimpleNodeSelector(nodeManager, nodeSelectionStats, nodeTaskMap, includeCoordinator, nodeMap, minCandidates, maxSplitsPerNode, maxPendingSplitsPerTask, maxTasksPerStage);
        }
    }

    private Supplier<NodeMap> createNodeMapSupplier(ConnectorId connectorId)
    {
        return () -> {
            ImmutableMap.Builder<String, InternalNode> byNodeId = ImmutableMap.builder();
            ImmutableSetMultimap.Builder<HostAddress, InternalNode> byHostAndPort = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<InetAddress, InternalNode> byHost = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<NetworkLocation, InternalNode> workersByNetworkPath = ImmutableSetMultimap.builder();

            Set<InternalNode> nodes;
            if (connectorId != null) {
                nodes = nodeManager.getActiveConnectorNodes(connectorId);
            }
            else {
                nodes = nodeManager.getNodes(ACTIVE);
            }

            Set<String> coordinatorNodeIds = nodeManager.getCoordinators().stream()
                    .map(InternalNode::getNodeIdentifier)
                    .collect(toImmutableSet());

            for (InternalNode node : nodes) {
                byNodeId.put(node.getNodeIdentifier(), node);
                if (useNetworkTopology && (includeCoordinator || !coordinatorNodeIds.contains(node.getNodeIdentifier()))) {
                    NetworkLocation location = networkLocationCache.get(node.getHostAndPort());
                    for (int i = 0; i <= location.getSegments().size(); i++) {
                        workersByNetworkPath.put(location.subLocation(0, i), node);
                    }
                }
                try {
                    byHostAndPort.put(node.getHostAndPort(), node);

                    InetAddress host = InetAddress.getByName(node.getInternalUri().getHost());
                    byHost.put(host, node);
                }
                catch (UnknownHostException e) {
                    // ignore
                }
            }

            return new NodeMap(byNodeId.build(), byHostAndPort.build(), byHost.build(), workersByNetworkPath.build(), coordinatorNodeIds);
        };
    }

    public static List<InternalNode> selectNodes(int limit, ResettableRandomizedIterator<InternalNode> candidates)
    {
        checkArgument(limit > 0, "limit must be at least 1");

        ImmutableList.Builder<InternalNode> selectedNodes = ImmutableList.builderWithExpectedSize(min(limit, candidates.size()));
        int selectedCount = 0;
        while (selectedCount < limit && candidates.hasNext()) {
            selectedNodes.add(candidates.next());
            selectedCount++;
        }

        return selectedNodes.build();
    }

    public static ResettableRandomizedIterator<InternalNode> randomizedNodes(NodeMap nodeMap, boolean includeCoordinator, Set<InternalNode> excludedNodes)
    {
        ImmutableList<InternalNode> nodes = nodeMap.getNodesByHostAndPort().values().stream()
                .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .filter(node -> !excludedNodes.contains(node))
                .collect(toImmutableList());
        return new ResettableRandomizedIterator<>(nodes);
    }

    public static List<InternalNode> selectExactNodes(NodeMap nodeMap, List<HostAddress> hosts, boolean includeCoordinator)
    {
        Set<InternalNode> chosen = new LinkedHashSet<>();
        Set<String> coordinatorIds = nodeMap.getCoordinatorNodeIds();

        for (HostAddress host : hosts) {
            nodeMap.getNodesByHostAndPort().get(host).stream()
                    .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
                    .forEach(chosen::add);

            InetAddress address;
            try {
                address = host.toInetAddress();
            }
            catch (UnknownHostException e) {
                // skip hosts that don't resolve
                continue;
            }

            // consider a split with a host without a port as being accessible by all nodes in that host
            if (!host.hasPort()) {
                nodeMap.getNodesByHost().get(address).stream()
                        .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
                        .forEach(chosen::add);
            }
        }

        // if the chosen set is empty and the host is the coordinator, force pick the coordinator
        if (chosen.isEmpty() && !includeCoordinator) {
            for (HostAddress host : hosts) {
                // In the code below, before calling `chosen::add`, it could have been checked that
                // `coordinatorIds.contains(node.getNodeIdentifier())`. But checking the condition isn't necessary
                // because every node satisfies it. Otherwise, `chosen` wouldn't have been empty.

                nodeMap.getNodesByHostAndPort().get(host).stream()
                        .forEach(chosen::add);

                InetAddress address;
                try {
                    address = host.toInetAddress();
                }
                catch (UnknownHostException e) {
                    // skip hosts that don't resolve
                    continue;
                }

                // consider a split with a host without a port as being accessible by all nodes in that host
                if (!host.hasPort()) {
                    nodeMap.getNodesByHost().get(address).stream()
                            .forEach(chosen::add);
                }
            }
        }

        return ImmutableList.copyOf(chosen);
    }

    public static SplitPlacementResult selectDistributionNodes(
            NodeMap nodeMap,
            NodeTaskMap nodeTaskMap,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask,
            Set<Split> splits,
            List<RemoteTask> existingTasks,
            BucketNodeMap bucketNodeMap,
            NodeSelectionStats nodeSelectionStats)
    {
        Multimap<InternalNode, Split> assignments = HashMultimap.create();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        Set<InternalNode> blockedNodes = new HashSet<>();
        for (Split split : splits) {
            // node placement is forced by the bucket to node map
            InternalNode node = bucketNodeMap.getAssignedNode(split).get();
            boolean isCacheable = bucketNodeMap.isSplitCacheable(split);

            // if node is full, don't schedule now, which will push back on the scheduling of splits
            if (assignmentStats.getTotalSplitCount(node) < maxSplitsPerNode ||
                    assignmentStats.getQueuedSplitCountForStage(node) < maxPendingSplitsPerTask) {
                if (isCacheable) {
                    split = new Split(split.getConnectorId(), split.getTransactionHandle(), split.getConnectorSplit(), split.getLifespan(), new SplitContext(true));
                    nodeSelectionStats.incrementBucketedPreferredNodeSelectedCount();
                }
                else {
                    nodeSelectionStats.incrementBucketedNonPreferredNodeSelectedCount();
                }
                assignments.put(node, split);
                assignmentStats.addAssignedSplit(node);
            }
            else {
                blockedNodes.add(node);
            }
        }

        ListenableFuture<?> blocked = toWhenHasSplitQueueSpaceFuture(blockedNodes, existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        return new SplitPlacementResult(blocked, ImmutableMultimap.copyOf(assignments));
    }

    public static int calculateLowWatermark(int maxPendingSplitsPerTask)
    {
        return (int) Math.ceil(maxPendingSplitsPerTask / 2.0);
    }

    public static ListenableFuture<?> toWhenHasSplitQueueSpaceFuture(Set<InternalNode> blockedNodes, List<RemoteTask> existingTasks, int spaceThreshold)
    {
        if (blockedNodes.isEmpty()) {
            return immediateFuture(null);
        }
        Map<String, RemoteTask> nodeToTaskMap = new HashMap<>();
        for (RemoteTask task : existingTasks) {
            nodeToTaskMap.put(task.getNodeId(), task);
        }
        List<ListenableFuture<?>> blockedFutures = blockedNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .map(nodeToTaskMap::get)
                .filter(Objects::nonNull)
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(spaceThreshold))
                .collect(toImmutableList());
        if (blockedFutures.isEmpty()) {
            return immediateFuture(null);
        }
        return whenAnyCompleteCancelOthers(blockedFutures);
    }

    public static ListenableFuture<?> toWhenHasSplitQueueSpaceFuture(List<RemoteTask> existingTasks, int spaceThreshold)
    {
        if (existingTasks.isEmpty()) {
            return immediateFuture(null);
        }
        List<ListenableFuture<?>> stateChangeFutures = existingTasks.stream()
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(spaceThreshold))
                .collect(toImmutableList());
        return whenAnyCompleteCancelOthers(stateChangeFutures);
    }
}
