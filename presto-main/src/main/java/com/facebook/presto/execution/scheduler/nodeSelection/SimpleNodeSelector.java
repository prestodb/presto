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
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.scheduler.InternalNodeInfo;
import com.facebook.presto.execution.scheduler.NodeAssignmentStats;
import com.facebook.presto.execution.scheduler.NodeMap;
import com.facebook.presto.execution.scheduler.SplitPlacementResult;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitContext;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToIntFunction;

import static com.facebook.presto.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static com.facebook.presto.execution.scheduler.NodeScheduler.randomizedNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectDistributionNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectExactNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static com.facebook.presto.metadata.InternalNode.NodeStatus.DEAD;
import static com.facebook.presto.spi.StandardErrorCode.NODE_SELECTION_NOT_SUPPORTED;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class SimpleNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(SimpleNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final NodeSelectionStats nodeSelectionStats;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final int maxUnacknowledgedSplitsPerTask;
    private final int maxTasksPerStage;

    public SimpleNodeSelector(
            InternalNodeManager nodeManager,
            NodeSelectionStats nodeSelectionStats,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask,
            int maxUnacknowledgedSplitsPerTask,
            int maxTasksPerStage)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeSelectionStats = requireNonNull(nodeSelectionStats, "nodeSelectionStats is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        this.maxUnacknowledgedSplitsPerTask = maxUnacknowledgedSplitsPerTask;
        checkArgument(maxUnacknowledgedSplitsPerTask > 0, "maxUnacknowledgedSplitsPerTask must be > 0, found: %s", maxUnacknowledgedSplitsPerTask);
        this.maxTasksPerStage = maxTasksPerStage;
    }

    @Override
    public void lockDownNodes()
    {
        nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
    }

    @Override
    public List<InternalNode> getActiveNodes()
    {
        return ImmutableList.copyOf(nodeMap.get().get().getActiveNodes());
    }

    @Override
    public List<InternalNode> getAllNodes()
    {
        return ImmutableList.copyOf(nodeMap.get().get().getAllNodes());
    }

    @Override
    public InternalNode selectCurrentNode()
    {
        // TODO: this is a hack to force scheduling on the coordinator
        return nodeManager.getCurrentNode();
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        return selectNodes(limit, randomizedNodes(nodeMap.get().get(), includeCoordinator, excludedNodes));
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks)
    {
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeMap nodeMap = this.nodeMap.get().get();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        List<InternalNode> eligibleNodes = getEligibleNodes(maxTasksPerStage, nodeMap, existingTasks);
        NodeSelection randomNodeSelection = new RandomNodeSelection(eligibleNodes, minCandidates);
        Set<InternalNode> blockedExactNodes = new HashSet<>();
        boolean splitWaitingForAnyNode = false;

        List<HostAddress> activeCandidates = nodeMap.getActiveNodes().stream()
                .map(InternalNode::getHostAndPort)
                .collect(toImmutableList());

        List<HostAddress> allCandidates = nodeMap.getAllNodes().stream()
                .map(InternalNode::getHostAndPort)
                .collect(toImmutableList());

        OptionalInt preferredNodeCount = OptionalInt.empty();
        for (Split split : splits) {
            List<InternalNode> candidateNodes;
            switch (split.getNodeSelectionStrategy()) {
                case HARD_AFFINITY:
                    candidateNodes = selectExactNodes(nodeMap, split.getPreferredNodes(activeCandidates), includeCoordinator);
                    preferredNodeCount = OptionalInt.of(candidateNodes.size());
                    break;
                case SOFT_AFFINITY:
                    candidateNodes = selectExactNodes(nodeMap, split.getPreferredNodes(allCandidates), includeCoordinator);
                    preferredNodeCount = OptionalInt.of(candidateNodes.size());
                    candidateNodes = ImmutableList.<InternalNode>builder()
                            .addAll(candidateNodes)
                            .addAll(randomNodeSelection.pickNodes(split))
                            .build();
                    break;
                case NO_PREFERENCE:
                    candidateNodes = randomNodeSelection.pickNodes(split);
                    break;
                default:
                    throw new PrestoException(NODE_SELECTION_NOT_SUPPORTED, format("Unsupported node selection strategy %s", split.getNodeSelectionStrategy()));
            }

            if (candidateNodes.isEmpty()) {
                log.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMap.getActiveNodes());
                throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
            }

            Optional<InternalNodeInfo> chosenNodeInfo = chooseLeastBusyNode(candidateNodes, assignmentStats::getTotalSplitCount, preferredNodeCount, maxSplitsPerNode, assignmentStats);
            if (!chosenNodeInfo.isPresent()) {
                chosenNodeInfo = chooseLeastBusyNode(candidateNodes, assignmentStats::getQueuedSplitCountForStage, preferredNodeCount, maxPendingSplitsPerTask, assignmentStats);
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

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        return selectDistributionNodes(nodeMap.get().get(), nodeTaskMap, maxSplitsPerNode, maxPendingSplitsPerTask, maxUnacknowledgedSplitsPerTask, splits, existingTasks, bucketNodeMap, nodeSelectionStats);
    }

    protected Optional<InternalNodeInfo> chooseLeastBusyNode(List<InternalNode> candidateNodes, ToIntFunction<InternalNode> splitCountProvider, OptionalInt preferredNodeCount, int maxSplitCount, NodeAssignmentStats assignmentStats)
    {
        int min = Integer.MAX_VALUE;
        InternalNode chosenNode = null;
        for (int i = 0; i < candidateNodes.size(); i++) {
            InternalNode node = candidateNodes.get(i);
            if (node.getNodeStatus() == DEAD) {
                // Node is down. Do not schedule split. Skip it.
                if (preferredNodeCount.isPresent() && i < preferredNodeCount.getAsInt()) {
                    nodeSelectionStats.incrementPreferredNonAliveNodeSkippedCount();
                }
                continue;
            }

            if (assignmentStats.getUnacknowledgedSplitCountForStage(node) >= maxUnacknowledgedSplitsPerTask) {
                continue;
            }
            int splitCount = splitCountProvider.applyAsInt(node);

            // choose the preferred node first as long as they're not busy
            if (preferredNodeCount.isPresent() && i < preferredNodeCount.getAsInt() && splitCount < maxSplitCount) {
                if (i == 0) {
                    nodeSelectionStats.incrementPrimaryPreferredNodeSelectedCount();
                }
                else {
                    nodeSelectionStats.incrementNonPrimaryPreferredNodeSelectedCount();
                }
                return Optional.of(new InternalNodeInfo(node, true));
            }
            // fallback to choosing the least busy nodes
            if (splitCount < min && splitCount < maxSplitCount) {
                chosenNode = node;
                min = splitCount;
            }
        }
        if (chosenNode == null) {
            return Optional.empty();
        }
        nodeSelectionStats.incrementNonPreferredNodeSelectedCount();
        return Optional.of(new InternalNodeInfo(chosenNode, false));
    }

    private List<InternalNode> getEligibleNodes(int limit, NodeMap nodeMap, List<RemoteTask> existingTasks)
    {
        List<InternalNode> existingNodes = existingTasks.stream()
                .map(remoteTask -> nodeMap.getActiveNodesByNodeId().get(remoteTask.getNodeId()))
                // nodes may sporadically disappear from the nodeMap if the announcement is delayed
                .filter(Objects::nonNull)
                .collect(toList());

        int alreadySelectedNodeCount = existingNodes.size();
        int nodeCount = nodeMap.getActiveNodesByNodeId().size();

        if (alreadySelectedNodeCount < limit && alreadySelectedNodeCount < nodeCount) {
            List<InternalNode> moreNodes = selectNodes(limit - alreadySelectedNodeCount, randomizedNodes(nodeMap, includeCoordinator, newHashSet(existingNodes)));
            existingNodes.addAll(moreNodes);
        }
        verify(existingNodes.stream().allMatch(Objects::nonNull), "existingNodes list must not contain any nulls");
        return existingNodes;
    }
}
