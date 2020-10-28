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

import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.scheduler.NodeMap;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.HostAddress;
import com.google.common.base.Supplier;
import com.google.common.collect.SetMultimap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class ScoreBasedBucketsNodeSelector
        extends SimpleNodeSelector
{
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;

    public ScoreBasedBucketsNodeSelector(InternalNodeManager nodeManager, NodeSelectionStats nodeSelectionStats, NodeTaskMap nodeTaskMap, boolean includeCoordinator, Supplier<NodeMap> nodeMap, int minCandidates, int maxSplitsPerNode, int maxPendingSplitsPerTask, int maxTasksPerStage)
    {
        super(nodeManager, nodeSelectionStats, nodeTaskMap, includeCoordinator, nodeMap, minCandidates, maxSplitsPerNode, maxPendingSplitsPerTask, maxTasksPerStage);
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        NodeMap nodeMap = this.nodeMap.get().get();
        SetMultimap<HostAddress, InternalNode> nodesByHostAndPort = nodeMap.getNodesByHostAndPort();

        List<InternalNode> nodes = new ArrayList<>(nodesByHostAndPort.values());
        Collections.shuffle(nodes);

//        List<InternalNodeScore> collect = nodes.stream()
//                .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
//                .filter(node -> !excludedNodes.contains(node))
//                .map(node -> {
//                    double availableMemoryUtil = Math.max(0, 100 - 100 * nodeTaskMap.getMemoryUsageOnNode(node) / node.getNodeMemory().orElse(Long.MAX_VALUE));
//                    return new InternalNodeScore(node, availableMemoryUtil);
//                }).sorted(Comparator.reverseOrder()).collect(toList());

        nodes = nodes.stream()
                .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .filter(node -> !excludedNodes.contains(node))
                .map(node -> {
                    double availableMemoryUtil = Math.max(0, 100 - 100 * nodeTaskMap.getMemoryUsageOnNode(node) / node.getNodeMemory().orElse(Long.MAX_VALUE));
                    return new InternalNodeScore(node, availableMemoryUtil);
                })
                .sorted(Comparator.reverseOrder())
                .map(node -> node.internalNode)
                .collect(toList());

        return nodes.size() >= limit ? nodes.subList(0, limit) : nodes;
    }

    class InternalNodeScore
            implements Comparable<InternalNodeScore>
    {
        private InternalNode internalNode;
        private Double score;

        public InternalNodeScore(InternalNode internalNode, Double score)
        {
            this.internalNode = requireNonNull(internalNode, "nodeTaskMap is null");
            this.score = requireNonNull(score, "score is null");
        }

        @Override
        public int compareTo(InternalNodeScore other)
        {
            return score.compareTo(other.score);
        }
    }
}
