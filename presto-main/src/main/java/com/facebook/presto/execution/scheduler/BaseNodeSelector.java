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
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.HostAddress;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyCompleteCancelOthers;
import static java.lang.Math.min;

abstract class BaseNodeSelector
{
    public static List<InternalNode> selectNodes(int limit, ResettableRandomizedIterator<InternalNode> candidates)
    {
        checkArgument(limit > 0, "limit must be at least 1");

        List<InternalNode> selected = new ArrayList<>(min(limit, candidates.size()));
        while (selected.size() < limit && candidates.hasNext()) {
            selected.add(candidates.next());
        }

        return selected;
    }

    public static ResettableRandomizedIterator<InternalNode> randomizedNodes(NodeMap nodeMap, boolean includeCoordinator, Set<InternalNode> excludedNodes)
    {
        ImmutableList<InternalNode> nodes = nodeMap.getNodesByHostAndPort().values().stream()
                .filter(node -> includeCoordinator || !nodeMap.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .filter(node -> !excludedNodes.contains(node))
                .collect(toImmutableList());
        return new ResettableRandomizedIterator<>(nodes);
    }

    public static List<InternalNode> selectExactNodes(NodeMap nodeMap, List<HostAddress> hosts, boolean includeCoordinator)
    {
        Set<InternalNode> chosen = new LinkedHashSet<>();
        Set<String> coordinatorIds = nodeMap.getCoordinatorNodeIds();

        for (HostAddress host : hosts) {
            nodeMap.getNodesByHostAndPort().get(host).stream()
                    .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
                    .forEach(chosen::add);

            InetAddress address;
            try {
                address = host.toInetAddress();
            }
            catch (UnknownHostException e) {
                // skip hosts that don't resolve
                continue;
            }

            // consider a split with a host without a port as being accessible by all nodes in that host
            if (!host.hasPort()) {
                nodeMap.getNodesByHost().get(address).stream()
                        .filter(node -> includeCoordinator || !coordinatorIds.contains(node.getNodeIdentifier()))
                        .forEach(chosen::add);
            }
        }

        // if the chosen set is empty and the host is the coordinator, force pick the coordinator
        if (chosen.isEmpty() && !includeCoordinator) {
            for (HostAddress host : hosts) {
                // In the code below, before calling `chosen::add`, it could have been checked that
                // `coordinatorIds.contains(node.getNodeIdentifier())`. But checking the condition isn't necessary
                // because every node satisfies it. Otherwise, `chosen` wouldn't have been empty.

                nodeMap.getNodesByHostAndPort().get(host).stream()
                        .forEach(chosen::add);

                InetAddress address;
                try {
                    address = host.toInetAddress();
                }
                catch (UnknownHostException e) {
                    // skip hosts that don't resolve
                    continue;
                }

                // consider a split with a host without a port as being accessible by all nodes in that host
                if (!host.hasPort()) {
                    nodeMap.getNodesByHost().get(address).stream()
                            .forEach(chosen::add);
                }
            }
        }

        return ImmutableList.copyOf(chosen);
    }

    protected SplitPlacementResult selectDistributionNodes(
            NodeMap nodeMap,
            NodeTaskMap nodeTaskMap,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask,
            Set<Split> splits,
            List<RemoteTask> existingTasks,
            BucketNodeMap bucketNodeMap)
    {
        Multimap<InternalNode, Split> assignments = HashMultimap.create();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        Set<InternalNode> blockedNodes = new HashSet<>();
        for (Split split : splits) {
            // node placement is forced by the bucket to node map
            InternalNode node = bucketNodeMap.getAssignedNode(split).get();

            // if node is full, don't schedule now, which will push back on the scheduling of splits
            if (assignmentStats.getTotalSplitCount(node) < maxSplitsPerNode ||
                    assignmentStats.getQueuedSplitCountForStage(node) < maxPendingSplitsPerTask) {
                assignments.put(node, split);
                assignmentStats.addAssignedSplit(node);
            }
            else {
                blockedNodes.add(node);
            }
        }

        ListenableFuture<?> blocked = toWhenHasSplitQueueSpaceFuture(blockedNodes, existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        return new SplitPlacementResult(blocked, ImmutableMultimap.copyOf(assignments));
    }

    protected int calculateLowWatermark(int maxPendingSplitsPerTask)
    {
        return (int) Math.ceil(maxPendingSplitsPerTask / 2.0);
    }

    protected ListenableFuture<?> toWhenHasSplitQueueSpaceFuture(Set<InternalNode> blockedNodes, List<RemoteTask> existingTasks, int spaceThreshold)
    {
        if (blockedNodes.isEmpty()) {
            return immediateFuture(null);
        }
        Map<String, RemoteTask> nodeToTaskMap = new HashMap<>();
        for (RemoteTask task : existingTasks) {
            nodeToTaskMap.put(task.getNodeId(), task);
        }
        List<ListenableFuture<?>> blockedFutures = blockedNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .map(nodeToTaskMap::get)
                .filter(Objects::nonNull)
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(spaceThreshold))
                .collect(toImmutableList());
        if (blockedFutures.isEmpty()) {
            return immediateFuture(null);
        }
        return whenAnyCompleteCancelOthers(blockedFutures);
    }

    protected ListenableFuture<?> toWhenHasSplitQueueSpaceFuture(List<RemoteTask> existingTasks, int spaceThreshold)
    {
        if (existingTasks.isEmpty()) {
            return immediateFuture(null);
        }
        List<ListenableFuture<?>> stateChangeFutures = existingTasks.stream()
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(spaceThreshold))
                .collect(toImmutableList());
        return whenAnyCompleteCancelOthers(stateChangeFutures);
    }
}
