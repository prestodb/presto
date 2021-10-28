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
import com.facebook.presto.Session;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleNodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelector;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelectorConfig;
import com.facebook.presto.execution.scheduler.nodeSelection.TopologyAwareNodeSelector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManager;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.facebook.airlift.concurrent.MoreFutures.whenAnyCompleteCancelOthers;
import static com.facebook.presto.SystemSessionProperties.getMaxUnacknowledgedSplitsPerTask;
import static com.facebook.presto.SystemSessionProperties.getResourceAwareSchedulingStrategy;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.ResourceAwareSchedulingStrategy;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.ResourceAwareSchedulingStrategy.TTL;
import static com.facebook.presto.metadata.InternalNode.NodeStatus.ALIVE;
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
    private final NodeTtlFetcherManager nodeTtlFetcherManager;
    private final QueryManager queryManager;
    private final SimpleTtlNodeSelectorConfig simpleTtlNodeSelectorConfig;

    @Inject
    public NodeScheduler(
            NetworkTopology networkTopology,
            InternalNodeManager nodeManager,
            NodeSelectionStats nodeSelectionStats,
            NodeSchedulerConfig config,
            NodeTaskMap nodeTaskMap,
            NodeTtlFetcherManager nodeTtlFetcherManager,
            QueryManager queryManager,
            SimpleTtlNodeSelectorConfig simpleTtlNodeSelectorConfig)
    {
        this(new NetworkLocationCache(networkTopology),
                networkTopology,
                nodeManager,
                nodeSelectionStats,
                config,
                nodeTaskMap,
                new Duration(5, SECONDS),
                nodeTtlFetcherManager,
                queryManager,
                simpleTtlNodeSelectorConfig);
    }

    public NodeScheduler(
            NetworkLocationCache networkLocationCache,
            NetworkTopology networkTopology,
            InternalNodeManager nodeManager,
            NodeSelectionStats nodeSelectionStats,
            NodeSchedulerConfig config,
            NodeTaskMap nodeTaskMap,
            Duration nodeMapRefreshInterval,
            NodeTtlFetcherManager nodeTtlFetcherManager,
            QueryManager queryManager,
            SimpleTtlNodeSelectorConfig simpleTtlNodeSelectorConfig)
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
        this.nodeTtlFetcherManager = requireNonNull(nodeTtlFetcherManager, "nodeTtlFetcherManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.simpleTtlNodeSelectorConfig = requireNonNull(simpleTtlNodeSelectorConfig, "simpleTtlNodeSelectorConfig is null");
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

    public NodeSelector createNodeSelector(Session session, ConnectorId connectorId)
    {
        return createNodeSelector(session, connectorId, Integer.MAX_VALUE);
    }

    public NodeSelector createNodeSelector(Session session, ConnectorId connectorId, int maxTasksPerStage)
    {
        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the the split is about to be scheduled
        Supplier<NodeMap> nodeMap = nodeMapRefreshInterval.toMillis() > 0 ?
                memoizeWithExpiration(createNodeMapSupplier(connectorId), nodeMapRefreshInterval.toMillis(), MILLISECONDS) : createNodeMapSupplier(connectorId);

        int maxUnacknowledgedSplitsPerTask = getMaxUnacknowledgedSplitsPerTask(requireNonNull(session, "session is null"));
        ResourceAwareSchedulingStrategy resourceAwareSchedulingStrategy = getResourceAwareSchedulingStrategy(session);
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
                    maxUnacknowledgedSplitsPerTask,
                    topologicalSplitCounters,
                    networkLocationSegmentNames,
                    networkLocationCache);
        }

        SimpleNodeSelector simpleNodeSelector = new SimpleNodeSelector(
                nodeManager,
                nodeSelectionStats,
                nodeTaskMap,
                includeCoordinator,
                nodeMap,
                minCandidates,
                maxSplitsPerNode,
                maxPendingSplitsPerTask,
                maxUnacknowledgedSplitsPerTask,
                maxTasksPerStage);

        if (resourceAwareSchedulingStrategy == TTL) {
            return new SimpleTtlNodeSelector(
                    simpleNodeSelector,
                    simpleTtlNodeSelectorConfig,
                    nodeTaskMap,
                    nodeMap,
                    minCandidates,
                    includeCoordinator,
                    maxSplitsPerNode,
                    maxPendingSplitsPerTask,
                    maxTasksPerStage,
                    nodeTtlFetcherManager,
                    queryManager,
                    session);
        }

        return simpleNodeSelector;
    }

    private Supplier<NodeMap> createNodeMapSupplier(ConnectorId connectorId)
    {
        return () -> {
            ImmutableMap.Builder<String, InternalNode> activeNodesByNodeId = ImmutableMap.builder();
            ImmutableSetMultimap.Builder<NetworkLocation, InternalNode> activeWorkersByNetworkPath = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<HostAddress, InternalNode> allNodesByHostAndPort = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<InetAddress, InternalNode> allNodesByHost = ImmutableSetMultimap.builder();

            Set<InternalNode> activeNodes;
            Set<InternalNode> allNodes;
            if (connectorId != null) {
                activeNodes = nodeManager.getActiveConnectorNodes(connectorId).stream().filter(node -> !node.isResourceManager()).collect(toImmutableSet());
                allNodes = nodeManager.getAllConnectorNodes(connectorId).stream().filter(node -> !node.isResourceManager()).collect(toImmutableSet());
            }
            else {
                activeNodes = nodeManager.getNodes(ACTIVE).stream().filter(node -> !node.isResourceManager()).collect(toImmutableSet());
                allNodes = activeNodes;
            }

            Set<String> coordinatorNodeIds = nodeManager.getCoordinators().stream()
                    .map(InternalNode::getNodeIdentifier)
                    .collect(toImmutableSet());

            for (InternalNode node : allNodes) {
                if (node.getNodeStatus() == ALIVE) {
                    activeNodesByNodeId.put(node.getNodeIdentifier(), node);
                    if (useNetworkTopology && (includeCoordinator || !coordinatorNodeIds.contains(node.getNodeIdentifier()))) {
                        NetworkLocation location = networkLocationCache.get(node.getHostAndPort());
                        for (int i = 0; i <= location.getSegments().size(); i++) {
                            activeWorkersByNetworkPath.put(location.subLocation(0, i), node);
                        }
                    }
                }
                try {
                    allNodesByHostAndPort.put(node.getHostAndPort(), node);
                    InetAddress host = InetAddress.getByName(node.getInternalUri().getHost());
                    allNodesByHost.put(host, node);
                }
                catch (UnknownHostException e) {
                    // ignore
                }
            }

            return new NodeMap(activeNodesByNodeId.build(), activeWorkersByNetworkPath.build(), coordinatorNodeIds, new LinkedList<>(activeNodes), new LinkedList<>(allNodes), allNodesByHost.build(), allNodesByHostAndPort.build());
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
        ImmutableList<InternalNode> nodes = nodeMap.getActiveNodes().stream()
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
            nodeMap.getAllNodesByHostAndPort().get(host).stream()
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
                nodeMap.getAllNodesByHost().get(address).stream()
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

                nodeMap.getAllNodesByHostAndPort().get(host).stream()
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
                    nodeMap.getAllNodesByHost().get(address).stream()
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
            int maxUnacknowledgedSplitsPerTask,
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
            if (!isDistributionNodeFull(assignmentStats, node, maxSplitsPerNode, maxPendingSplitsPerTask, maxUnacknowledgedSplitsPerTask)) {
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

    private static boolean isDistributionNodeFull(NodeAssignmentStats assignmentStats, InternalNode node, int maxSplitsPerNode, int maxPendingSplitsPerTask, int maxUnacknowledgedSplitsPerTask)
    {
        return assignmentStats.getUnacknowledgedSplitCountForStage(node) >= maxUnacknowledgedSplitsPerTask ||
                (assignmentStats.getTotalSplitCount(node) >= maxSplitsPerNode && assignmentStats.getQueuedSplitCountForStage(node) >= maxPendingSplitsPerTask);
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
