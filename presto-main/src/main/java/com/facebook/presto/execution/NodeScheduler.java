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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import io.airlift.log.Logger;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class NodeScheduler
{
    private static final Logger log = Logger.get(NodeScheduler.class);

    private final NodeManager nodeManager;
    private final AtomicLong scheduleLocal = new AtomicLong();
    private final AtomicLong scheduleRandom = new AtomicLong();
    private final int minCandidates;
    private final boolean includeCoordinator;
    private final int maxSplitsPerNode;
    private final int maxSplitsPerNodePerTaskWhenFull;
    private final NodeTaskMap nodeTaskMap;
    private final boolean doubleScheduling;

    @Inject
    public NodeScheduler(NodeManager nodeManager, NodeSchedulerConfig config, NodeTaskMap nodeTaskMap)
    {
        this.nodeManager = nodeManager;
        this.minCandidates = config.getMinCandidates();
        this.includeCoordinator = config.isIncludeCoordinator();
        this.doubleScheduling = config.isMultipleTasksPerNodeEnabled();
        this.maxSplitsPerNode = config.getMaxSplitsPerNode();
        this.maxSplitsPerNodePerTaskWhenFull = config.getMaxPendingSplitsPerNodePerTask();
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        checkArgument(maxSplitsPerNode > maxSplitsPerNodePerTaskWhenFull, "maxSplitsPerNode must be > maxSplitsPerNodePerTaskWhenFull");
    }

    @Managed
    public long getScheduleLocal()
    {
        return scheduleLocal.get();
    }

    @Managed
    public long getScheduleRandom()
    {
        return scheduleRandom.get();
    }

    @Managed
    public void reset()
    {
        scheduleLocal.set(0);
        scheduleRandom.set(0);
    }

    public NodeSelector createNodeSelector(String dataSourceName)
    {
        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the the split is about to be scheduled
        Supplier<NodeMap> nodeMap = Suppliers.memoizeWithExpiration(() -> {
            ImmutableSetMultimap.Builder<HostAddress, Node> byHostAndPort = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<InetAddress, Node> byHost = ImmutableSetMultimap.builder();

            Set<Node> nodes;
            if (dataSourceName != null) {
                nodes = nodeManager.getActiveDatasourceNodes(dataSourceName);
            }
            else {
                nodes = nodeManager.getNodes(ACTIVE);
            }

            for (Node node : nodes) {
                try {
                    byHostAndPort.put(node.getHostAndPort(), node);

                    InetAddress host = InetAddress.getByName(node.getHttpUri().getHost());
                    byHost.put(host, node);
                }
                catch (UnknownHostException e) {
                    // ignore
                }
            }

            Set<String> coordinatorNodeIds = nodeManager.getCoordinators().stream()
                    .map(Node::getNodeIdentifier)
                    .collect(toImmutableSet());

            return new NodeMap(byHostAndPort.build(), byHost.build(), coordinatorNodeIds);
        }, 5, TimeUnit.SECONDS);

        return new NodeSelector(nodeMap);
    }

    public class NodeSelector
    {
        private final AtomicReference<Supplier<NodeMap>> nodeMap;

        public NodeSelector(Supplier<NodeMap> nodeMap)
        {
            this.nodeMap = new AtomicReference<>(nodeMap);
        }

        public void lockDownNodes()
        {
            nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
        }

        public List<Node> allNodes()
        {
            return ImmutableList.copyOf(nodeMap.get().get().getNodesByHostAndPort().values());
        }

        public Node selectCurrentNode()
        {
            // TODO: this is a hack to force scheduling on the coordinator
            return nodeManager.getCurrentNode();
        }

        public List<Node> selectRandomNodes(int limit)
        {
            return selectNodes(limit, randomizedNodes());
        }

        private List<Node> selectNodes(int limit, Iterator<Node> candidates)
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

        /**
         * Identifies the nodes for running the specified splits.
         *
         * @param splits the splits that need to be assigned to nodes
         * @return a multimap from node to splits only for splits for which we could identify a node to schedule on.
         * If we cannot find an assignment for a split, it is not included in the map.
         */
        public Multimap<Node, Split> computeAssignments(Set<Split> splits, Iterable<RemoteTask> existingTasks)
        {
            Multimap<Node, Split> assignment = HashMultimap.create();
            Map<Node, Integer> assignmentCount = new HashMap<>();
            // pre-populate the assignment counts with zeros. This makes getOrDefault() faster
            for (Node node : nodeMap.get().get().getNodesByHostAndPort().values()) {
                assignmentCount.put(node, 0);
            }

            // maintain a temporary local cache of partitioned splits on the node
            Map<Node, Integer> splitCountByNode = new HashMap<>();

            Map<String, Integer> queuedSplitCountByNode = new HashMap<>();

            for (RemoteTask task : existingTasks) {
                String nodeId = task.getNodeId();
                queuedSplitCountByNode.put(nodeId, queuedSplitCountByNode.getOrDefault(nodeId, 0) + task.getQueuedPartitionedSplitCount());
            }

            ResettableRandomizedIterator<Node> randomCandidates = randomizedNodes();
            for (Split split : splits) {
                randomCandidates.reset();

                List<Node> candidateNodes;
                NodeMap nodeMap = this.nodeMap.get().get();
                if (!split.isRemotelyAccessible()) {
                    candidateNodes = selectNodesBasedOnHint(nodeMap, split.getAddresses());
                }
                else {
                    candidateNodes = selectNodes(minCandidates, randomCandidates);
                }
                if (candidateNodes.isEmpty()) {
                    log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getNodesByHost().keys());
                    throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                }

                // compute and cache number of splits currently assigned to each node
                // NOTE: This does not use the Stream API for performance reasons.
                for (Node node : candidateNodes) {
                    if (!splitCountByNode.containsKey(node)) {
                        splitCountByNode.put(node, nodeTaskMap.getPartitionedSplitsOnNode(node));
                    }
                }

                Node chosenNode = null;
                int min = Integer.MAX_VALUE;

                for (Node node : candidateNodes) {
                    int totalSplitCount = assignmentCount.getOrDefault(node, 0) + splitCountByNode.get(node);

                    if (totalSplitCount < min && totalSplitCount < maxSplitsPerNode) {
                        chosenNode = node;
                        min = totalSplitCount;
                    }
                }
                if (chosenNode == null) {
                    for (Node node : candidateNodes) {
                        int assignedSplitCount = assignmentCount.getOrDefault(node, 0);
                        int queuedSplitCount = queuedSplitCountByNode.getOrDefault(node.getNodeIdentifier(), 0);
                        int totalSplitCount = queuedSplitCount + assignedSplitCount;
                        if (totalSplitCount < min && totalSplitCount < maxSplitsPerNodePerTaskWhenFull) {
                            chosenNode = node;
                            min = totalSplitCount;
                        }
                    }
                }
                if (chosenNode != null) {
                    assignment.put(chosenNode, split);
                    assignmentCount.put(chosenNode, assignmentCount.getOrDefault(chosenNode, 0) + 1);
                }
            }
            return assignment;
        }

        private ResettableRandomizedIterator<Node> randomizedNodes()
        {
            NodeMap nodeMap = this.nodeMap.get().get();
            ImmutableList<Node> nodes = nodeMap.getNodesByHostAndPort().values().stream()
                    .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                    .collect(toImmutableList());
            return new ResettableRandomizedIterator<>(nodes);
        }

        private List<Node> selectNodesBasedOnHint(NodeMap nodeMap, List<HostAddress> addresses)
        {
            Set<Node> chosen = new LinkedHashSet<>(minCandidates);
            Set<String> coordinatorIds = nodeMap.getCoordinatorNodeIds();

            for (HostAddress hint : addresses) {
                nodeMap.getNodesByHostAndPort().get(hint).stream()
                        .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
                        .filter(chosen::add)
                        .forEach(node -> scheduleLocal.incrementAndGet());

                InetAddress address;
                try {
                    address = hint.toInetAddress();
                }
                catch (UnknownHostException e) {
                    // skip addresses that don't resolve
                    continue;
                }

                // consider a split with a host hint without a port as being accessible by all nodes in that host
                if (!hint.hasPort()) {
                    nodeMap.getNodesByHost().get(address).stream()
                            .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
                            .filter(chosen::add)
                            .forEach(node -> scheduleLocal.incrementAndGet());
                }
            }

            // if the chosen set is empty and the hint includes the coordinator, force pick the coordinator
            if (chosen.isEmpty() && !includeCoordinator) {
                for (HostAddress hint : addresses) {
                    // In the code below, before calling `chosen::add`, it could have been checked that
                    // `coordinatorIds.contains(node.getNodeIdentifier())`. But checking the condition isn't necessary
                    // because every node satisfies it. Otherwise, `chosen` wouldn't have been empty.

                    nodeMap.getNodesByHostAndPort().get(hint).stream()
                            .forEach(chosen::add);

                    InetAddress address;
                    try {
                        address = hint.toInetAddress();
                    }
                    catch (UnknownHostException e) {
                        // skip addresses that don't resolve
                        continue;
                    }

                    // consider a split with a host hint without a port as being accessible by all nodes in that host
                    if (!hint.hasPort()) {
                        nodeMap.getNodesByHost().get(address).stream()
                                .forEach(chosen::add);
                    }
                }
            }

            return ImmutableList.copyOf(chosen);
        }
    }

    private static class NodeMap
    {
        private final SetMultimap<HostAddress, Node> nodesByHostAndPort;
        private final SetMultimap<InetAddress, Node> nodesByHost;
        private final Set<String> coordinatorNodeIds;

        public NodeMap(SetMultimap<HostAddress, Node> nodesByHostAndPort,
                SetMultimap<InetAddress, Node> nodesByHost,
                Set<String> coordinatorNodeIds)
        {
            this.nodesByHostAndPort = nodesByHostAndPort;
            this.nodesByHost = nodesByHost;
            this.coordinatorNodeIds = coordinatorNodeIds;
        }

        private SetMultimap<HostAddress, Node> getNodesByHostAndPort()
        {
            return nodesByHostAndPort;
        }

        public SetMultimap<InetAddress, Node> getNodesByHost()
        {
            return nodesByHost;
        }

        public Set<String> getCoordinatorNodeIds()
        {
            return coordinatorNodeIds;
        }
    }
}
