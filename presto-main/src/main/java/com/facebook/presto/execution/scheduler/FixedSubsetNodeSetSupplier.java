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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.QueryId;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;

import javax.inject.Inject;
import javax.validation.constraints.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.facebook.presto.execution.scheduler.NodeSelectionHashStrategy.CONSISTENT_HASHING;
import static com.facebook.presto.metadata.InternalNode.NodeStatus.ALIVE;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Math.ceil;

/**
 * This WIP class is responsible for assigning a subset of the cluster nodes
 * to a requesting query.
 * The size of the subset is currently static, but it can by defined by configuration
 * or can be driven by fairness algorithm depending on query metadata.
 */
public class FixedSubsetNodeSetSupplier
        implements NodeSetSupplier, Closeable
{
    private static final Logger log = Logger.get(FixedSubsetNodeSetSupplier.class);
    private static final int NODES_PER_QUERY = 10;
    private final PriorityQueue<NodeSetAcquireRequest> pendingRequests;
    private final InternalNodeManager nodeManager;
    private final NetworkLocationCache networkLocationCache;
    // TODO: Use Set<InternalNode>
    private final Map<QueryId, List<InternalNode>> perQueryNodeAssignments;

    private final ScheduledFuture<?> scheduledFuture;

    @Inject
    public FixedSubsetNodeSetSupplier(
            InternalNodeManager nodeManager,
            NetworkTopology networkTopology,
            @ForNodeScheduler ScheduledExecutorService scheduledExecutorService)
    {
        this.nodeManager = nodeManager;
        this.networkLocationCache = new NetworkLocationCache(networkTopology);
        this.perQueryNodeAssignments = new HashMap<>();
        this.pendingRequests = new PriorityQueue<>();
        // start loop to check pending node requests every second
        scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(this::fulfillPendingRequests, 0, 5, TimeUnit.SECONDS);
    }

    private void fulfillPendingRequests()
    {
        log.info("Checking for pending requests.. count=%s", pendingRequests.size());
        if (pendingRequests.size() == 0) {
            return;
        }

        List<InternalNode> allocatedNodes = perQueryNodeAssignments.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        List<InternalNode> currentActiveWorkerNodesInCluster = nodeManager.getNodes(ACTIVE).stream()
                .filter(node -> !node.isResourceManager() && !node.isCoordinator() && !node.isCatalogServer())
                .collect(toImmutableList());

        List<InternalNode> avaialableNodes = currentActiveWorkerNodesInCluster.stream()
                .filter(node -> !allocatedNodes.contains(node))
                .collect(Collectors.toList());

        NodeSetAcquireRequest nodeSetAcquireRequest = pendingRequests.peek();
        while (nodeSetAcquireRequest != null && avaialableNodes.size() > 0) {
            try {
                List<InternalNode> nodesForQuery = new ArrayList<>();
                for (int i = 0; i < nodeSetAcquireRequest.getCount(); i++) {
                    nodesForQuery.add(avaialableNodes.remove(i));
                }
                // assign the nodes to this query
                perQueryNodeAssignments.put(nodeSetAcquireRequest.queryId, nodesForQuery);
                // Let the query scheduler know that query can now execute
                nodeSetAcquireRequest.getCompletableFuture().complete(null);
                log.info("Successfully fulfilled nodeAcquireRequest=%s", nodeSetAcquireRequest);

                // Move to the next request
                pendingRequests.poll();
                nodeSetAcquireRequest = pendingRequests.peek();
            }
            catch (Exception ex) {
                log.error("Unable to satisfy nodeAcquireRequest=%s", nodeSetAcquireRequest);
            }
        }
    }

    @Override
    public CompletableFuture<?> acquireNodes(QueryId queryId, int count)
    {
        CompletableFuture<?> completableFuture = new CompletableFuture<>();
        NodeSetAcquireRequest nodeSetAcquireRequest = new NodeSetAcquireRequest(queryId, count);
        nodeSetAcquireRequest.setCompletableFuture(completableFuture);
        pendingRequests.add(nodeSetAcquireRequest);
        return completableFuture;
    }

    @Override
    public CompletableFuture<?> releaseNodes(QueryId queryId, int count)
    {
        perQueryNodeAssignments.remove(queryId);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public synchronized Supplier<NodeSet> createNodeSetSupplierForQuery(
            QueryId queryId,
            ConnectorId connectorId,
            Optional<Predicate<Node>> nodeFilterPredicate,
            NodeSelectionHashStrategy nodeSelectionHashStrategy,
            boolean useNetworkTopology,
            int minVirtualNodeCount,
            boolean includeCoordinator)
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
                // TODO: Uncomment this functionality
                // allNodes = nodeManager.getAllConnectorNodes(connectorId).stream().filter(finalNodeFilterPredicate).collect(toImmutableList());
            }
            else {
                activeNodes = nodeManager.getNodes(ACTIVE).stream().filter(finalNodeFilterPredicate).collect(toImmutableList());
                // allNodes = activeNodes;
            }

            // Here we want to assign a subset of the active nodes to this query
            if (perQueryNodeAssignments.containsKey(queryId)) {
                activeNodes = activeNodes.stream().filter(internalNode -> perQueryNodeAssignments.get(queryId)
                        .contains(internalNode)).collect(Collectors.toList());
            }
            else {
                Set<InternalNode> alreadyAssignedNodes = perQueryNodeAssignments.values().stream()
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());
                activeNodes = activeNodes.stream().filter(internalNode -> !alreadyAssignedNodes.contains(internalNode))
                        .collect(Collectors.toList());
                // Check if nodes are available, then pick a subset
                if (activeNodes.size() > NODES_PER_QUERY) {
                    activeNodes = activeNodes.subList(0, NODES_PER_QUERY);
                }
                perQueryNodeAssignments.put(queryId, activeNodes);
            }
            // TODO: Remove this after enabling commented allNode computation in L93
            allNodes = activeNodes;

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

            return new NodeSet(
                    activeNodesByNodeId.build(),
                    activeWorkersByNetworkPath.build(),
                    nodeManager.getCoordinators(),
                    activeNodes,
                    allNodes,
                    allNodesByHost.build(),
                    allNodesByHostAndPort.build(),
                    consistentHashingNodeProvider);
        };
    }

    @Override
    public void close()
            throws IOException
    {
        scheduledFuture.cancel(true);
    }

    public static class NodeSetAcquireRequest
            implements Comparable
    {
        private QueryId queryId;
        private int count;

        public CompletableFuture<?> getCompletableFuture()
        {
            return completableFuture;
        }

        public void setCompletableFuture(CompletableFuture<?> completableFuture)
        {
            this.completableFuture = completableFuture;
        }

        private CompletableFuture<?> completableFuture;
        public NodeSetAcquireRequest(
                QueryId queryId,
                int count)
        {
            this.queryId = queryId;
            this.count = count;
        }

        public QueryId getQueryId()
        {
            return queryId;
        }

        public void setQueryId(QueryId queryId)
        {
            this.queryId = queryId;
        }

        public int getCount()
        {
            return count;
        }

        public void setCount(int count)
        {
            this.count = count;
        }

        @Override
        public int compareTo(@NotNull Object other)
        {
            NodeSetAcquireRequest otherRequest = (NodeSetAcquireRequest) other;
            if (otherRequest.getCount() == this.getCount()) {
                return 0;
            }
            return this.getCount() < otherRequest.getCount() ? -1 : 1;
        }
    }
}
