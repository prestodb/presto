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
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.scheduler.NodeScheduler.NEGATIVE_CACHE_DURATION;
import static com.facebook.presto.execution.scheduler.NodeScheduler.randomizedNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectExactNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Objects.requireNonNull;

public class TopologyAwareNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(TopologyAwareNodeSelector.class);

    private final NodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final boolean doubleScheduling;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final int maxSplitsPerNode;
    private final int maxSplitsPerNodePerTaskWhenFull;
    private final List<CounterStat> topologicalSplitCounters;
    private final List<String> networkLocationSegmentNames;
    private final LoadingCache<HostAddress, NetworkLocation> networkLocationCache;
    private final Cache<HostAddress, Boolean> negativeNetworkLocationCache;

    public TopologyAwareNodeSelector(
            NodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            boolean doubleScheduling,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            int maxSplitsPerNode,
            int maxSplitsPerNodePerTaskWhenFull,
            List<CounterStat> topologicalSplitCounters,
            List<String> networkLocationSegmentNames,
            LoadingCache<HostAddress, NetworkLocation> networkLocationCache,
            Cache<HostAddress, Boolean> negativeNetworkLocationCache)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.doubleScheduling = doubleScheduling;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.maxSplitsPerNodePerTaskWhenFull = maxSplitsPerNodePerTaskWhenFull;
        this.topologicalSplitCounters = requireNonNull(topologicalSplitCounters, "topologicalSplitCounters is null");
        this.networkLocationSegmentNames = requireNonNull(networkLocationSegmentNames, "networkLocationSegmentNames is null");
        this.networkLocationCache = requireNonNull(networkLocationCache, "networkLocationCache is null");
        this.negativeNetworkLocationCache = requireNonNull(negativeNetworkLocationCache, "negativeNetworkLocationCache is null");
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
                    if (negativeNetworkLocationCache.getIfPresent(host) != null) {
                        continue;
                    }
                    locations.add(networkLocationCache.get(host));
                }
                catch (ExecutionException | UncheckedExecutionException e) {
                    // Skip addresses we can't locate
                    negativeNetworkLocationCache.put(host, true);
                    log.warn(e, "Unable to determine location of %s. Will attempt again in %s", host, NEGATIVE_CACHE_DURATION);
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
