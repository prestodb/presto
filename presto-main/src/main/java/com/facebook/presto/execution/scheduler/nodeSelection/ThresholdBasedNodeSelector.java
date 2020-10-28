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
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.InternalNodeInfo;
import com.facebook.presto.execution.scheduler.NodeAssignmentStats;
import com.facebook.presto.execution.scheduler.NodeMap;
import com.facebook.presto.execution.scheduler.ResettableRandomizedIterator;
import com.facebook.presto.execution.scheduler.SplitPlacementResult;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitContext;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectExactNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionUtils.sortedNodes;
import static com.facebook.presto.spi.StandardErrorCode.NODE_SELECTION_NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ThresholdBasedNodeSelector
        extends SimpleNodeSelector
{
    private static final Logger log = Logger.get(ThresholdBasedNodeSelector.class);

    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final int maxTasksPerStage;

    public ThresholdBasedNodeSelector(InternalNodeManager nodeManager, NodeSelectionStats nodeSelectionStats, NodeTaskMap nodeTaskMap, boolean includeCoordinator, Supplier<NodeMap> nodeMap, int minCandidates, int maxSplitsPerNode, int maxPendingSplitsPerTask, int maxTasksPerStage)
    {
        super(nodeManager, nodeSelectionStats, nodeTaskMap, includeCoordinator, nodeMap, minCandidates, maxSplitsPerNode, maxPendingSplitsPerTask, maxTasksPerStage);
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        this.maxTasksPerStage = maxTasksPerStage;
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        NodeMap nodeMap = this.nodeMap.get().get();
        SetMultimap<HostAddress, InternalNode> nodesByHostAndPort = nodeMap.getNodesByHostAndPort();
        int size = nodesByHostAndPort.size() - (includeCoordinator ? 0 : nodeMap.getCoordinatorNodeIds().size()) - excludedNodes.size();
        List<InternalNode> nodes = nodesByHostAndPort.values().stream()
                .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .filter(node -> !excludedNodes.contains(node))
                .filter(node -> Math.max(0, 1 - (double) nodeTaskMap.getMemoryUsageOnNode(node) / (double) node.getNodeMemory().orElse(Long.MAX_VALUE)) > 0.3)
                .collect(toList());

        return selectNodes(limit, new ResettableRandomizedIterator<>(nodes));
    }

    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
    {
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeMap nodeMap = this.nodeMap.get().get();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        NodeSelection randomNodeSelection = new ThresholdNodeSelection(nodeMap, includeCoordinator, minCandidates, maxTasksPerStage, existingTasks, nodeTaskMap);
        Set<InternalNode> blockedExactNodes = new HashSet<>();
        boolean splitWaitingForAnyNode = false;

        // todo identify if sorting will cause bottleneck
        List<HostAddress> sortedCandidates = sortedNodes(nodeMap);
        OptionalInt preferredNodeCount = OptionalInt.empty();
        for (Split split : splits) {
            List<InternalNode> candidateNodes;
            switch (split.getNodeSelectionStrategy()) {
                case HARD_AFFINITY:
                    candidateNodes = selectExactNodes(nodeMap, split.getPreferredNodes(sortedCandidates), includeCoordinator);
                    preferredNodeCount = OptionalInt.of(candidateNodes.size());
                    break;
                case SOFT_AFFINITY:
                    candidateNodes = selectExactNodes(nodeMap, split.getPreferredNodes(sortedCandidates), includeCoordinator);
                    preferredNodeCount = OptionalInt.of(candidateNodes.size());
                    candidateNodes = ImmutableList.<InternalNode>builder().addAll(candidateNodes).addAll(randomNodeSelection.pickNodes(split)).build();
                    break;
                case NO_PREFERENCE:
                    candidateNodes = randomNodeSelection.pickNodes(split);
                    break;
                default:
                    throw new PrestoException(NODE_SELECTION_NOT_SUPPORTED, format("Unsupported node selection strategy %s", split.getNodeSelectionStrategy()));
            }

            if (candidateNodes.isEmpty()) {
                log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getNodesByHost().keys());
                throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            Optional<InternalNodeInfo> chosenNodeInfo = chooseLeastBusyNode(candidateNodes, assignmentStats::getTotalSplitCount, preferredNodeCount, maxSplitsPerNode);
            if (!chosenNodeInfo.isPresent()) {
                chosenNodeInfo = chooseLeastBusyNode(candidateNodes, assignmentStats::getQueuedSplitCountForStage, preferredNodeCount, maxPendingSplitsPerTask);
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
                if (split.getNodeSelectionStrategy() != HARD_AFFINITY) {
                    splitWaitingForAnyNode = true;
                }
                // Exact node set won't matter, if a split is waiting for any node
                else if (!splitWaitingForAnyNode) {
                    blockedExactNodes.addAll(candidateNodes);
                }
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
}
