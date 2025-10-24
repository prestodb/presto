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
import com.facebook.airlift.units.Duration;
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
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.ttl.nodettlfetchermanagers.NodeTtlFetcherManager;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import static com.facebook.airlift.concurrent.MoreFutures.whenAnyCompleteCancelOthers;
import static com.facebook.presto.SystemSessionProperties.getMaxUnacknowledgedSplitsPerTask;
import static com.facebook.presto.SystemSessionProperties.getResourceAwareSchedulingStrategy;
import static com.facebook.presto.SystemSessionProperties.isScheduleSplitsBasedOnTaskLoad;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.ResourceAwareSchedulingStrategy;
import static com.facebook.presto.execution.scheduler.NodeSchedulerConfig.ResourceAwareSchedulingStrategy.TTL;
import static com.facebook.presto.execution.scheduler.NodeSelectionHashStrategy.CONSISTENT_HASHING;
import static com.facebook.presto.metadata.InternalNode.NodeStatus.ALIVE;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.lang.Math.addExact;
import static java.lang.Math.ceil;
import static java.lang.Math.min;
import static java.lang.String.format;
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
    private final long maxSplitsWeightPerNode;
    private final long maxSplitsWeightPerTask;
    private final long maxPendingSplitsWeightPerTask;
    private final NodeTaskMap nodeTaskMap;
    private final boolean useNetworkTopology;
    private final Duration nodeMapRefreshInterval;
    private final NodeTtlFetcherManager nodeTtlFetcherManager;
    private final QueryManager queryManager;
    private final SimpleTtlNodeSelectorConfig simpleTtlNodeSelectorConfig;
    private final NodeSelectionHashStrategy nodeSelectionHashStrategy;
    private final int minVirtualNodeCount;
    private final int maxPreferredNodes;

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
        int maxSplitsPerNode = config.getMaxSplitsPerNode();
        int maxPendingSplitsPerTask = config.getMaxPendingSplitsPerTask();
        checkArgument(maxSplitsPerNode >= maxPendingSplitsPerTask, "maxSplitsPerNode must be > maxPendingSplitsPerTask");
        this.maxSplitsWeightPerNode = SplitWeight.rawValueForStandardSplitCount(maxSplitsPerNode);
        this.maxSplitsWeightPerTask = SplitWeight.rawValueForStandardSplitCount(config.getMaxSplitsPerTask());
        this.maxPendingSplitsWeightPerTask = SplitWeight.rawValueForStandardSplitCount(maxPendingSplitsPerTask);
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
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
        this.nodeSelectionHashStrategy = config.getNodeSelectionHashStrategy();
        this.minVirtualNodeCount = config.getMinVirtualNodeCount();
        this.maxPreferredNodes = config.getMaxPreferredNodes();
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
        return createNodeSelector(session, connectorId, Integer.MAX_VALUE, Optional.empty());
    }

    public NodeSelector createNodeSelector(Session session, ConnectorId connectorId, Optional<Predicate<Node>> filterNodePredicate)
    {
        return createNodeSelector(session, connectorId, Integer.MAX_VALUE, filterNodePredicate);
    }

    public NodeSelector createNodeSelector(Session session, ConnectorId connectorId, int maxTasksPerStage)
    {
        return createNodeSelector(session, connectorId, maxTasksPerStage, Optional.empty());
    }

    public NodeSelector createNodeSelector(Session session, ConnectorId connectorId, int maxTasksPerStage, Optional<Predicate<Node>> filterNodePredicate)
    {
        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the split is about to be scheduled
        Supplier<NodeMap> nodeMap = nodeMapRefreshInterval.toMillis() > 0 ?
                memoizeWithExpiration(createNodeMapSupplier(connectorId, filterNodePredicate), nodeMapRefreshInterval.toMillis(), MILLISECONDS) : createNodeMapSupplier(connectorId, filterNodePredicate);

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
                    maxSplitsWeightPerNode,
                    maxPendingSplitsWeightPerTask,
                    maxUnacknowledgedSplitsPerTask,
                    topologicalSplitCounters,
                    networkLocationSegmentNames,
                    networkLocationCache,
                    maxPreferredNodes);
        }

        SimpleNodeSelector simpleNodeSelector = new SimpleNodeSelector(
                nodeManager,
                nodeSelectionStats,
                nodeTaskMap,
                includeCoordinator,
                isScheduleSplitsBasedOnTaskLoad(session),
                nodeMap,
                minCandidates,
                maxSplitsWeightPerNode,
                maxSplitsWeightPerTask,
                maxPendingSplitsWeightPerTask,
                maxUnacknowledgedSplitsPerTask,
                maxTasksPerStage,
                maxPreferredNodes);

        if (resourceAwareSchedulingStrategy == TTL) {
            return new SimpleTtlNodeSelector(
                    simpleNodeSelector,
                    simpleTtlNodeSelectorConfig,
                    nodeTaskMap,
                    nodeMap,
                    minCandidates,
                    includeCoordinator,
                    maxSplitsWeightPerNode,
                    maxPendingSplitsWeightPerTask,
                    maxTasksPerStage,
                    nodeTtlFetcherManager,
                    queryManager,
                    session);
        }

        return simpleNodeSelector;
    }

    private Supplier<NodeMap> createNodeMapSupplier(ConnectorId connectorId, Optional<Predicate<Node>> nodeFilterPredicate)
    {
        return () -> {
            ImmutableMap.Builder<String, InternalNode> activeNodesByNodeId = ImmutableMap.builder();
            ImmutableSetMultimap.Builder<NetworkLocation, InternalNode> activeWorkersByNetworkPath = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<HostAddress, InternalNode> allNodesByHostAndPort = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<InetAddress, InternalNode> allNodesByHost = ImmutableSetMultimap.builder();
            Predicate<Node> resourceManagerFilterPredicate = node -> !node.isResourceManager();
            Predicate<Node> finalNodeFilterPredicate = resourceManagerFilterPredicate.and(nodeFilterPredicate.orElse(node -> true));
            List<InternalNode> activeNodes;
            List<InternalNode> allNodes;
            if (connectorId != null) {
                activeNodes = nodeManager.getActiveConnectorNodes(connectorId).stream().filter(finalNodeFilterPredicate).collect(toImmutableList());
                allNodes = nodeManager.getAllConnectorNodes(connectorId).stream().filter(finalNodeFilterPredicate).collect(toImmutableList());
            }
            else {
                activeNodes = nodeManager.getNodes(ACTIVE).stream().filter(finalNodeFilterPredicate).collect(toImmutableList());
                allNodes = activeNodes;
            }

            Set<String> coordinatorNodeIds = nodeManager.getCoordinators().stream()
                    .map(InternalNode::getNodeIdentifier)
                    .collect(toImmutableSet());

            Optional<ConsistentHashingNodeProvider> consistentHashingNodeProvider = Optional.empty();
            if (nodeSelectionHashStrategy == CONSISTENT_HASHING) {
                int weight = (int) ceil(1.0 * minVirtualNodeCount / activeNodes.size());
                consistentHashingNodeProvider = Optional.of(ConsistentHashingNodeProvider.create(activeNodes, weight));
            }

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

            return new NodeMap(
                    activeNodesByNodeId.build(),
                    activeWorkersByNetworkPath.build(),
                    coordinatorNodeIds,
                    activeNodes,
                    allNodes,
                    allNodesByHost.build(),
                    allNodesByHostAndPort.build(),
                    consistentHashingNodeProvider);
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

            // consider a split with a host without a port as being accessible by all nodes in that host
            if (!host.hasPort()) {
                InetAddress address;
                try {
                    address = host.toInetAddress();
                }
                catch (UnknownHostException e) {
                    // skip hosts that don't resolve
                    continue;
                }

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

                chosen.addAll(nodeMap.getAllNodesByHostAndPort().get(host));

                // consider a split with a host without a port as being accessible by all nodes in that host
                if (!host.hasPort()) {
                    InetAddress address;
                    try {
                        address = host.toInetAddress();
                    }
                    catch (UnknownHostException e) {
                        // skip hosts that don't resolve
                        continue;
                    }

                    chosen.addAll(nodeMap.getAllNodesByHost().get(address));
                }
            }
        }

        return ImmutableList.copyOf(chosen);
    }

    public static SplitPlacementResult selectDistributionNodes(
            NodeMap nodeMap,
            NodeTaskMap nodeTaskMap,
            long maxSplitsWeightPerNode,
            long maxPendingSplitsWeightPerTask,
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
            Optional<InternalNode> optionalNode = bucketNodeMap.getAssignedNode(split);
            if (!optionalNode.isPresent()) {
                throw new PrestoException(NO_NODES_AVAILABLE, format("No assignment for split in bucketNodeMap. Split Info: %s", split.getConnectorSplit().getInfoMap()));
            }
            InternalNode node = optionalNode.get();
            boolean isCacheable = bucketNodeMap.isSplitCacheable(split);
            SplitWeight splitWeight = split.getSplitWeight();

            // if node is full, don't schedule now, which will push back on the scheduling of splits
            if (canAssignSplitToDistributionNode(assignmentStats, node, maxSplitsWeightPerNode, maxPendingSplitsWeightPerTask, maxUnacknowledgedSplitsPerTask, splitWeight)) {
                if (isCacheable) {
                    split = new Split(split.getConnectorId(), split.getTransactionHandle(), split.getConnectorSplit(), split.getLifespan(), new SplitContext(true));
                    nodeSelectionStats.incrementBucketedPreferredNodeSelectedCount();
                }
                else {
                    nodeSelectionStats.incrementBucketedNonPreferredNodeSelectedCount();
                }
                assignments.put(node, split);
                assignmentStats.addAssignedSplit(node, splitWeight);
            }
            else {
                blockedNodes.add(node);
            }
        }

        ListenableFuture<?> blocked = toWhenHasSplitQueueSpaceFuture(blockedNodes, existingTasks, calculateLowWatermark(maxPendingSplitsWeightPerTask));
        return new SplitPlacementResult(blocked, ImmutableMultimap.copyOf(assignments));
    }

    private static boolean canAssignSplitToDistributionNode(NodeAssignmentStats assignmentStats, InternalNode node, long maxSplitsWeightPerNode, long maxPendingSplitsWeightPerTask, int maxUnacknowledgedSplitsPerTask, SplitWeight splitWeight)
    {
        return assignmentStats.getUnacknowledgedSplitCountForStage(node) < maxUnacknowledgedSplitsPerTask &&
                (canAssignSplitBasedOnWeight(assignmentStats.getTotalSplitsWeight(node), maxSplitsWeightPerNode, splitWeight) ||
                        canAssignSplitBasedOnWeight(assignmentStats.getQueuedSplitsWeightForStage(node), maxPendingSplitsWeightPerTask, splitWeight));
    }

    public static boolean canAssignSplitBasedOnWeight(long currentWeight, long weightLimit, SplitWeight splitWeight)
    {
        // Nodes or tasks that are configured to accept any splits (ie: weightLimit > 0) should always accept at least one split when
        // empty (ie: currentWeight == 0) to ensure that forward progress can be made if split weights are huge
        return addExact(currentWeight, splitWeight.getRawValue()) <= weightLimit || (currentWeight == 0 && weightLimit > 0);
    }

    public static long calculateLowWatermark(long maxPendingSplitsWeightPerTask)
    {
        return (long) Math.ceil(maxPendingSplitsWeightPerTask * 0.5);
    }

    public static ListenableFuture<?> toWhenHasSplitQueueSpaceFuture(Set<InternalNode> blockedNodes, List<RemoteTask> existingTasks, long weightSpaceThreshold)
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
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(weightSpaceThreshold))
                .collect(toImmutableList());
        if (blockedFutures.isEmpty()) {
            return immediateFuture(null);
        }
        return whenAnyCompleteCancelOthers(blockedFutures);
    }

    public static ListenableFuture<?> toWhenHasSplitQueueSpaceFuture(List<RemoteTask> existingTasks, long weightSpaceThreshold)
    {
        if (existingTasks.isEmpty()) {
            return immediateFuture(null);
        }
        List<ListenableFuture<?>> stateChangeFutures = existingTasks.stream()
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(weightSpaceThreshold))
                .collect(toImmutableList());
        return whenAnyCompleteCancelOthers(stateChangeFutures);
    }
}
