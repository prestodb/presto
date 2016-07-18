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
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.NodePartitionMap;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.scheduler.NodeScheduler.randomizedNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectDistributionNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectExactNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Objects.requireNonNull;

public class SimpleNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(SimpleNodeSelector.class);

    private final NodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final boolean doubleScheduling;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerNodePerStageWhenFull;

    public SimpleNodeSelector(
            NodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            boolean doubleScheduling,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            int maxSplitsPerNode,
            int maxPendingSplitsPerNodePerStageWhenFull)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.doubleScheduling = doubleScheduling;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.maxPendingSplitsPerNodePerStageWhenFull = maxPendingSplitsPerNodePerStageWhenFull;
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
                // min is guaranteed to be MAX_VALUE at this line
                for (Node node : candidateNodes) {
                    int totalSplitCount = assignmentStats.getQueuedSplitCountForStage(node);
                    if (totalSplitCount < min && totalSplitCount < maxPendingSplitsPerNodePerStageWhenFull) {
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

    @Override
    public Multimap<Node, Split> computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, NodePartitionMap partitioning)
    {
        return selectDistributionNodes(nodeMap.get().get(), nodeTaskMap, maxSplitsPerNode, maxPendingSplitsPerNodePerStageWhenFull, splits, existingTasks, partitioning);
    }
}
