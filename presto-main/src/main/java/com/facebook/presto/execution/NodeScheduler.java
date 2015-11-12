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

import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.NodeSchedulerConfig.LEGACY_NETWORK_TOPOLOGY;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NodeScheduler
{
    private static final Logger log = Logger.get(NodeScheduler.class);

    private final LoadingCache<HostAddress, NetworkLocation> networkLocationCache;
    private final List<CounterStat> topologicalSplitCounters;
    private final List<String> networkLocationSegmentNames;
    private final NodeManager nodeManager;
    private final int minCandidates;
    private final boolean includeCoordinator;
    private final int maxSplitsPerNode;
    private final int maxSplitsPerNodePerTaskWhenFull;
    private final NodeTaskMap nodeTaskMap;
    private final boolean doubleScheduling;
    private final boolean useNetworkTopology;

    @Inject
    public NodeScheduler(NetworkTopology networkTopology, NodeManager nodeManager, NodeSchedulerConfig config, NodeTaskMap nodeTaskMap)
    {
        this.nodeManager = nodeManager;
        this.minCandidates = config.getMinCandidates();
        this.includeCoordinator = config.isIncludeCoordinator();
        this.doubleScheduling = config.isMultipleTasksPerNodeEnabled();
        this.maxSplitsPerNode = config.getMaxSplitsPerNode();
        this.maxSplitsPerNodePerTaskWhenFull = config.getMaxPendingSplitsPerNodePerTask();
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        checkArgument(maxSplitsPerNode > maxSplitsPerNodePerTaskWhenFull, "maxSplitsPerNode must be > maxSplitsPerNodePerTaskWhenFull");
        this.useNetworkTopology = !config.getNetworkTopology().equals(LEGACY_NETWORK_TOPOLOGY);

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

        networkLocationCache = CacheBuilder.newBuilder()
                .expireAfterWrite(1, TimeUnit.DAYS)
                .refreshAfterWrite(12, TimeUnit.HOURS)
                .build(new CacheLoader<HostAddress, NetworkLocation>()
                {
                    @Override
                    public NetworkLocation load(HostAddress address)
                            throws Exception
                    {
                        return networkTopology.locate(address);
                    }
                });
    }

    public Map<String, CounterStat> getTopologicalSplitCounters()
    {
        ImmutableMap.Builder<String, CounterStat> counters = ImmutableMap.builder();
        for (int i = 0; i < topologicalSplitCounters.size(); i++) {
            counters.put(i == 0 ? "all" : networkLocationSegmentNames.get(i - 1), topologicalSplitCounters.get(i));
        }
        return counters.build();
    }

    public NodeSelector createNodeSelector(String dataSourceName)
    {
        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the the split is about to be scheduled
        Supplier<NodeMap> nodeMap = Suppliers.memoizeWithExpiration(() -> {
            ImmutableSetMultimap.Builder<HostAddress, Node> byHostAndPort = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<InetAddress, Node> byHost = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<NetworkLocation, Node> workersByNetworkPath = ImmutableSetMultimap.builder();

            Set<Node> nodes;
            if (dataSourceName != null) {
                nodes = nodeManager.getActiveDatasourceNodes(dataSourceName);
            }
            else {
                nodes = nodeManager.getNodes(ACTIVE);
            }

            Set<String> coordinatorNodeIds = nodeManager.getCoordinators().stream()
                    .map(Node::getNodeIdentifier)
                    .collect(toImmutableSet());

            for (Node node : nodes) {
                if (useNetworkTopology && (includeCoordinator || !coordinatorNodeIds.contains(node.getNodeIdentifier()))) {
                    try {
                        NetworkLocation location = networkLocationCache.get(node.getHostAndPort());
                        for (int i = 0; i <= location.getSegments().size(); i++) {
                            workersByNetworkPath.put(location.subLocation(0, i), node);
                        }
                    }
                    catch (ExecutionException | UncheckedExecutionException e) {
                        log.error(e, "Failed to locate network location of %s", node.getHostAndPort());
                    }
                }
                try {
                    byHostAndPort.put(node.getHostAndPort(), node);

                    InetAddress host = InetAddress.getByName(node.getHttpUri().getHost());
                    byHost.put(host, node);
                }
                catch (UnknownHostException e) {
                    // ignore
                }
            }

            return new NodeMap(byHostAndPort.build(), byHost.build(), workersByNetworkPath.build(), coordinatorNodeIds);
        }, 5, TimeUnit.SECONDS);

        if (useNetworkTopology) {
            return new TopologyAwareNodeSelector(nodeMap);
        }
        else {
            return new SimpleNodeSelector(nodeMap);
        }
    }

    public class TopologyAwareNodeSelector
            implements NodeSelector
    {
        private final AtomicReference<Supplier<NodeMap>> nodeMap;

        public TopologyAwareNodeSelector(Supplier<NodeMap> nodeMap)
        {
            this.nodeMap = new AtomicReference<>(nodeMap);
        }

        @Override
        public void lockDownNodes()
        {
            nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
        }

        @Override
        public List<Node> allNodes()
        {
            return ImmutableList.copyOf(nodeMap.get().get().getNodesByHostAndPort().values());
        }

        @Override
        public Node selectCurrentNode()
        {
            // TODO: this is a hack to force scheduling on the coordinator
            return nodeManager.getCurrentNode();
        }

        @Override
        public List<Node> selectRandomNodes(int limit)
        {
            return selectNodes(limit, randomizedNodes(nodeMap.get().get(), includeCoordinator), doubleScheduling);
        }

        @Override
        public Multimap<Node, Split> computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
        {
            NodeMap nodeMap = this.nodeMap.get().get();
            Collection<Node> allNodes = nodeMap.getNodesByHostAndPort().values();
            Multimap<Node, Split> assignment = HashMultimap.create();
            NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

            int[] topologicCounters = new int[topologicalSplitCounters.size()];
            Set<NetworkLocation> filledLocations = new HashSet<>();
            for (Split split : splits) {
                if (!split.isRemotelyAccessible()) {
                    List<Node> candidateNodes = selectExactNodes(nodeMap, split.getAddresses(), includeCoordinator);
                    if (candidateNodes.isEmpty()) {
                        log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getNodesByHost().keys());
                        throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                    }
                    Node chosenNode = bestNodeSplitCount(candidateNodes.iterator(), minCandidates, maxSplitsPerNodePerTaskWhenFull, assignmentStats);
                    if (chosenNode != null) {
                        assignment.put(chosenNode, split);
                        assignmentStats.addAssignedSplit(chosenNode);
                    }
                    continue;
                }

                Node chosenNode = null;
                int depth = networkLocationSegmentNames.size();
                int chosenDepth = 0;
                List<NetworkLocation> locations = new ArrayList<>();
                for (HostAddress host : split.getAddresses()) {
                    try {
                        locations.add(networkLocationCache.get(host));
                    }
                    catch (ExecutionException | UncheckedExecutionException e) {
                        // Skip addresses we can't locate
                    }
                }
                if (locations.isEmpty()) {
                    // Add the root location
                    locations.add(new NetworkLocation());
                    depth = 0;
                }
                // Try each address at progressively shallower network locations
                for (int i = depth; i >= 0 && chosenNode == null; i--) {
                    for (NetworkLocation location : locations) {
                        location = location.subLocation(0, i);
                        if (filledLocations.contains(location)) {
                            continue;
                        }
                        Set<Node> nodes = nodeMap.getWorkersByNetworkPath().get(location);
                        double queueFraction = (1.0 + i) / (1.0 + depth);
                        chosenNode = bestNodeSplitCount(new ResettableRandomizedIterator<>(nodes), minCandidates, (int) Math.ceil(queueFraction * maxSplitsPerNodePerTaskWhenFull), assignmentStats);
                        if (chosenNode != null) {
                            chosenDepth = i;
                            break;
                        }
                        filledLocations.add(location);
                    }
                }
                if (chosenNode != null) {
                    assignment.put(chosenNode, split);
                    assignmentStats.addAssignedSplit(chosenNode);
                    topologicCounters[chosenDepth]++;
                }
            }
            for (int i = 0; i < topologicCounters.length; i++) {
                if (topologicCounters[i] > 0) {
                    topologicalSplitCounters.get(i).update(topologicCounters[i]);
                }
            }
            return assignment;
        }

        @Nullable
        private Node bestNodeSplitCount(Iterator<Node> candidates, int minCandidatesWhenFull, int maxSplitsPerNodePerTaskWhenFull, NodeAssignmentStats assignmentStats)
        {
            Node bestQueueNotFull = null;
            int min = Integer.MAX_VALUE;
            int fullCandidatesConsidered = 0;

            while (candidates.hasNext() && (fullCandidatesConsidered < minCandidatesWhenFull || bestQueueNotFull == null)) {
                Node node = candidates.next();
                if (assignmentStats.getTotalSplitCount(node) < maxSplitsPerNode) {
                    return node;
                }
                fullCandidatesConsidered++;
                int totalSplitCount = assignmentStats.getTotalQueuedSplitCount(node);
                if (totalSplitCount < min && totalSplitCount < maxSplitsPerNodePerTaskWhenFull) {
                    bestQueueNotFull = node;
                }
            }
            return bestQueueNotFull;
        }
    }

    public class SimpleNodeSelector
            implements NodeSelector
    {
        private final AtomicReference<Supplier<NodeMap>> nodeMap;

        public SimpleNodeSelector(Supplier<NodeMap> nodeMap)
        {
            this.nodeMap = new AtomicReference<>(nodeMap);
        }

        @Override
        public void lockDownNodes()
        {
            nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
        }

        @Override
        public List<Node> allNodes()
        {
            return ImmutableList.copyOf(nodeMap.get().get().getNodesByHostAndPort().values());
        }

        @Override
        public Node selectCurrentNode()
        {
            // TODO: this is a hack to force scheduling on the coordinator
            return nodeManager.getCurrentNode();
        }

        @Override
        public List<Node> selectRandomNodes(int limit)
        {
            return selectNodes(limit, randomizedNodes(nodeMap.get().get(), includeCoordinator), doubleScheduling);
        }

        @Override
        public Multimap<Node, Split> computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
        {
            Multimap<Node, Split> assignment = HashMultimap.create();
            NodeMap nodeMap = this.nodeMap.get().get();
            NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

            ResettableRandomizedIterator<Node> randomCandidates = randomizedNodes(nodeMap, includeCoordinator);
            for (Split split : splits) {
                randomCandidates.reset();

                List<Node> candidateNodes;
                if (!split.isRemotelyAccessible()) {
                    candidateNodes = selectExactNodes(nodeMap, split.getAddresses(), includeCoordinator);
                }
                else {
                    candidateNodes = selectNodes(minCandidates, randomCandidates, doubleScheduling);
                }
                if (candidateNodes.isEmpty()) {
                    log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getNodesByHost().keys());
                    throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                }

                Node chosenNode = null;
                int min = Integer.MAX_VALUE;

                for (Node node : candidateNodes) {
                    int totalSplitCount = assignmentStats.getTotalSplitCount(node);
                    if (totalSplitCount < min && totalSplitCount < maxSplitsPerNode) {
                        chosenNode = node;
                        min = totalSplitCount;
                    }
                }
                if (chosenNode == null) {
                    for (Node node : candidateNodes) {
                        int totalSplitCount = assignmentStats.getTotalQueuedSplitCount(node);
                        if (totalSplitCount < min && totalSplitCount < maxSplitsPerNodePerTaskWhenFull) {
                            chosenNode = node;
                            min = totalSplitCount;
                        }
                    }
                }
                if (chosenNode != null) {
                    assignment.put(chosenNode, split);
                    assignmentStats.addAssignedSplit(chosenNode);
                }
            }
            return assignment;
        }
    }

    private static List<Node> selectNodes(int limit, Iterator<Node> candidates, boolean doubleScheduling)
    {
        checkArgument(limit > 0, "limit must be at least 1");

        List<Node> selected = new ArrayList<>(limit);
        while (selected.size() < limit && candidates.hasNext()) {
            selected.add(candidates.next());
        }

        if (doubleScheduling && !selected.isEmpty()) {
            // Cycle the nodes until we reach the limit
            int uniqueNodes = selected.size();
            int i = 0;
            while (selected.size() < limit) {
                if (i >= uniqueNodes) {
                    i = 0;
                }
                selected.add(selected.get(i));
                i++;
            }
        }
        return selected;
    }

    private static ResettableRandomizedIterator<Node> randomizedNodes(NodeMap nodeMap, boolean includeCoordinator)
    {
        ImmutableList<Node> nodes = nodeMap.getNodesByHostAndPort().values().stream()
                .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .collect(toImmutableList());
        return new ResettableRandomizedIterator<>(nodes);
    }

    private static List<Node> selectExactNodes(NodeMap nodeMap, List<HostAddress> hosts, boolean includeCoordinator)
    {
        Set<Node> chosen = new LinkedHashSet<>();
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

    public static class NodeMap
    {
        private final SetMultimap<HostAddress, Node> nodesByHostAndPort;
        private final SetMultimap<InetAddress, Node> nodesByHost;
        private final SetMultimap<NetworkLocation, Node> workersByNetworkPath;
        private final Set<String> coordinatorNodeIds;

        public NodeMap(SetMultimap<HostAddress, Node> nodesByHostAndPort,
                SetMultimap<InetAddress, Node> nodesByHost,
                SetMultimap<NetworkLocation, Node> workersByNetworkPath,
                Set<String> coordinatorNodeIds)
        {
            this.nodesByHostAndPort = nodesByHostAndPort;
            this.nodesByHost = nodesByHost;
            this.workersByNetworkPath = workersByNetworkPath;
            this.coordinatorNodeIds = coordinatorNodeIds;
        }

        public SetMultimap<HostAddress, Node> getNodesByHostAndPort()
        {
            return nodesByHostAndPort;
        }

        public SetMultimap<InetAddress, Node> getNodesByHost()
        {
            return nodesByHost;
        }

        public SetMultimap<NetworkLocation, Node> getWorkersByNetworkPath()
        {
            return workersByNetworkPath;
        }

        public Set<String> getCoordinatorNodeIds()
        {
            return coordinatorNodeIds;
        }
    }
}
