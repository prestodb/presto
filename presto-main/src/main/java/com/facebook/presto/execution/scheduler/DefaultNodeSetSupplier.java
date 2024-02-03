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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static com.facebook.presto.execution.scheduler.NodeSelectionHashStrategy.CONSISTENT_HASHING;
import static com.facebook.presto.metadata.InternalNode.NodeStatus.ALIVE;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Math.ceil;

public class DefaultNodeSetSupplier
        implements NodeSetSupplier
{
    private final InternalNodeManager nodeManager;
    private final NetworkLocationCache networkLocationCache;

    @Inject
    public DefaultNodeSetSupplier(
            InternalNodeManager nodeManager,
            NetworkTopology networkTopology)
    {
        this(nodeManager, new NetworkLocationCache(networkTopology));
    }

    public DefaultNodeSetSupplier(
            InternalNodeManager nodeManager,
            NetworkLocationCache networkLocationCache)
    {
        this.nodeManager = nodeManager;
        this.networkLocationCache = networkLocationCache;
    }

    @Override
    public CompletableFuture<?> acquireNodes(QueryId queryId, int count)
    {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> releaseNodes(QueryId queryId, int count)
    {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public Supplier<NodeSet> createNodeSetSupplierForQuery(QueryId queryId, ConnectorId connectorId, Optional<Predicate<Node>> nodeFilterPredicate, NodeSelectionHashStrategy nodeSelectionHashStrategy, boolean useNetworkTopology, int minVirtualNodeCount, boolean includeCoordinator)
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

            return new NodeSet(
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
}
