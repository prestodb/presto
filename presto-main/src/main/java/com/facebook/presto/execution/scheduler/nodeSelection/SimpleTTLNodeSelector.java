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
package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.InternalNodeInfo;
import com.facebook.presto.execution.scheduler.NodeAssignmentStats;
import com.facebook.presto.execution.scheduler.NodeMap;
import com.facebook.presto.execution.scheduler.ResettableRandomizedIterator;
import com.facebook.presto.execution.scheduler.SplitPlacementResult;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.ttl.ConfidenceBasedTTLInfo;
import com.facebook.presto.ttl.NodeTTL;
import com.facebook.presto.ttl.TTLFetcherManager;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static com.facebook.presto.spi.StandardErrorCode.NODE_SELECTION_NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.MILLIS;

public class SimpleTTLNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(SimpleTTLNodeSelector.class);
    private final Optional<TTLFetcherManager> ttlFetcherManager;
    private final Session session;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final NodeTaskMap nodeTaskMap;
    private final int minCandidates;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final int maxUnacknowledgedSplitsPerTask;
    private final int maxTasksPerStage;
    private final boolean includeCoordinator;
    private final SimpleNodeSelector simpleNodeSelector;
    private final QueryManager queryManager;

    public SimpleTTLNodeSelector(InternalNodeManager nodeManager,
            NodeSelectionStats nodeSelectionStats,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask,
            int maxUnacknowledgedSplitsPerTask,
            int maxTasksPerStage,
            Optional<TTLFetcherManager> ttlFetcherManager,
            QueryManager queryManager,
            Session session)
    {
        this.simpleNodeSelector = new SimpleNodeSelector(nodeManager,
                nodeSelectionStats,
                nodeTaskMap,
                includeCoordinator,
                nodeMap,
                minCandidates,
                maxSplitsPerNode,
                maxPendingSplitsPerTask,
                maxUnacknowledgedSplitsPerTask,
                maxTasksPerStage);

        this.ttlFetcherManager = ttlFetcherManager;
        this.session = session;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.nodeTaskMap = nodeTaskMap;
        this.minCandidates = minCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        this.maxUnacknowledgedSplitsPerTask = maxUnacknowledgedSplitsPerTask;
        checkArgument(maxUnacknowledgedSplitsPerTask > 0, "maxUnacknowledgedSplitsPerTask must be > 0, found: %s", maxUnacknowledgedSplitsPerTask);
        this.maxTasksPerStage = maxTasksPerStage;
        this.includeCoordinator = includeCoordinator;
        this.queryManager = queryManager;
    }

    @Override
    public void lockDownNodes()
    {
        simpleNodeSelector.lockDownNodes();
    }

    @Override
    public List<InternalNode> getActiveNodes()
    {
        return simpleNodeSelector.getActiveNodes();
    }

    @Override
    public List<InternalNode> getAllNodes()
    {
        return simpleNodeSelector.getAllNodes();
    }

    @Override
    public InternalNode selectCurrentNode()
    {
        return simpleNodeSelector.selectCurrentNode();
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        log.info("TTL_SELECTRANDOMNODES");
        // if there is no ttlFetcherManager or we don't have estimated execution time for the query, don't use TTL
        if (!ttlFetcherManager.isPresent() || !session.getResourceEstimates().getExecutionTime().isPresent()) {
            return simpleNodeSelector.selectRandomNodes(limit, excludedNodes);
        }

        Map<InternalNode, NodeTTL> nodeTTLInfo = ttlFetcherManager.get().getAllTTLs();

        Map<InternalNode, Optional<ConfidenceBasedTTLInfo>> ttlInfo = nodeTTLInfo
                .entrySet()
                .stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getTTLInfo()
                                .stream()
                                .min(Comparator.comparing(ConfidenceBasedTTLInfo::getExpiryInstant))));

        NodeMap nodeMap = this.nodeMap.get().get();
        List<InternalNode> nodes = nodeMap.getActiveNodes();

        Duration estimatedExecutionTimeRemaining = getEstimatedExecutionTimeRemaining();
        ImmutableList<InternalNode> eligibleNodes = nodes.stream().filter(ttlInfo::containsKey)
                .filter(node -> includeCoordinator || !node.isCoordinator())
                .filter(node -> !excludedNodes.contains(node))
                .filter(node -> ttlInfo.get(node).isPresent())
                .filter(node -> isTTLEnough(ttlInfo.get(node).get(), estimatedExecutionTimeRemaining))
                .collect(toImmutableList());

        return selectNodes(limit, new ResettableRandomizedIterator<>(eligibleNodes));
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
    {
        log.info("TTL_COMPUTEASSIGNMENTS");
        if (!ttlFetcherManager.isPresent() || !session.getResourceEstimates().getExecutionTime().isPresent()) {
            return simpleNodeSelector.computeAssignments(splits, existingTasks);
        }

        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeMap nodeMap = this.nodeMap.get().get();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        NodeSelection randomNodeSelection = new TTLRandomNodeSelection(
                nodeMap,
                includeCoordinator,
                minCandidates,
                maxTasksPerStage,
                existingTasks,
                ttlFetcherManager.get(),
                getEstimatedExecutionTimeRemaining());

        Set<InternalNode> blockedExactNodes = new HashSet<>();
        boolean splitWaitingForAnyNode = false;

        OptionalInt preferredNodeCount = OptionalInt.empty();
        for (Split split : splits) {
            List<InternalNode> candidateNodes;
            if (split.getNodeSelectionStrategy() == NodeSelectionStrategy.NO_PREFERENCE) {
                candidateNodes = randomNodeSelection.pickNodes(split);
            }
            else {
                throw new PrestoException(NODE_SELECTION_NOT_SUPPORTED, format("Unsupported node selection strategy for TTL scheduling %s", split.getNodeSelectionStrategy()));
            }

            if (candidateNodes.isEmpty()) {
                log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getActiveNodes());
                throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            Optional<InternalNodeInfo> chosenNodeInfo = simpleNodeSelector.chooseLeastBusyNode(candidateNodes, assignmentStats::getTotalSplitCount, preferredNodeCount, maxSplitsPerNode, assignmentStats);
            if (!chosenNodeInfo.isPresent()) {
                chosenNodeInfo = simpleNodeSelector.chooseLeastBusyNode(candidateNodes, assignmentStats::getQueuedSplitCountForStage, preferredNodeCount, maxPendingSplitsPerTask, assignmentStats);
            }

            if (chosenNodeInfo.isPresent()) {
                split = new Split(
                        split.getConnectorId(),
                        split.getTransactionHandle(),
                        split.getConnectorSplit(),
                        split.getLifespan(),
                        new SplitContext(chosenNodeInfo.get().isCacheable()));

                InternalNode chosenNode = chosenNodeInfo.get().getInternalNode();
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode);
            }
            else {
                splitWaitingForAnyNode = true;
            }
        }

        ListenableFuture<?> blocked;
        if (splitWaitingForAnyNode) {
            blocked = toWhenHasSplitQueueSpaceFuture(existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        }
        else {
            blocked = toWhenHasSplitQueueSpaceFuture(blockedExactNodes, existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        }
        return new SplitPlacementResult(blocked, assignment);
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        return simpleNodeSelector.computeAssignments(splits, existingTasks, bucketNodeMap);
    }

    private boolean isTTLEnough(ConfidenceBasedTTLInfo ttlInfo, Duration estimatedExecutionTime)
    {
        Instant expiryTime = ttlInfo.getExpiryInstant();
        long timeRemaining = MILLIS.between(Instant.now(), expiryTime);
        return new Duration(Math.max(timeRemaining, 0), TimeUnit.MILLISECONDS).compareTo(estimatedExecutionTime) >= 0;
    }

    private Duration getEstimatedExecutionTimeRemaining()
    {
        double totalEstimatedExecutionTime = session.getResourceEstimates().getExecutionTime().get().getValue(TimeUnit.MILLISECONDS);
        double elapsedExecutionTime = queryManager.getQueryInfo(session.getQueryId()).getQueryStats().getExecutionTime().getValue(TimeUnit.MILLISECONDS);
        double estimatedExecutionTimeRemaining = Math.max(totalEstimatedExecutionTime - elapsedExecutionTime, 0);

        log.info("TTL_REMAININGEXECUTIONTIME %s", estimatedExecutionTimeRemaining);
        return new Duration(estimatedExecutionTimeRemaining, TimeUnit.MILLISECONDS);
    }
}
