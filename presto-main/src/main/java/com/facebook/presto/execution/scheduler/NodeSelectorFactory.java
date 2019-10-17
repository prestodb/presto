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

import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.execution.scheduler.NodeSelectorFactoryConfig.NetworkTopologyType;
import static com.facebook.presto.spi.NodeState.ACTIVE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class NodeSelectorFactory
{
    private final NetworkLocationCache networkLocationCache;
    private final List<CounterStat> topologicalSplitCounters;
    private final List<String> networkLocationSegmentNames;
    private final InternalNodeManager nodeManager;
    private final int minCandidates;
    private final boolean includeCoordinator;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final NodeTaskMap nodeTaskMap;
    private final boolean useNetworkTopology;
    private final Duration nodeMapRefreshInterval;

    @Inject
    public NodeSelectorFactory(NetworkTopology networkTopology, InternalNodeManager nodeManager, NodeSelectorFactoryConfig config, NodeTaskMap nodeTaskMap)
    {
        this(new NetworkLocationCache(networkTopology), networkTopology, nodeManager, config, nodeTaskMap, new Duration(5, SECONDS));
    }

    public NodeSelectorFactory(
            NetworkLocationCache networkLocationCache,
            NetworkTopology networkTopology,
            InternalNodeManager nodeManager,
            NodeSelectorFactoryConfig config,
            NodeTaskMap nodeTaskMap,
            Duration nodeMapRefreshInterval)
    {
        this.networkLocationCache = networkLocationCache;
        this.nodeManager = nodeManager;
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
            return new SimpleNodeSelector(nodeManager, nodeTaskMap, includeCoordinator, nodeMap, minCandidates, maxSplitsPerNode, maxPendingSplitsPerTask, maxTasksPerStage);
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
}
