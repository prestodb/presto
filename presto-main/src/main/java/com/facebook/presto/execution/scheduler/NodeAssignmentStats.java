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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class NodeAssignmentStats
{
    private final NodeTaskMap nodeTaskMap;
    private final Map<InternalNode, Integer> nodeTotalSplitCount;
    private final Map<String, PendingSplitInfo> stageQueuedSplitInfo;

    public NodeAssignmentStats(NodeTaskMap nodeTaskMap, NodeMap nodeMap, List<RemoteTask> existingTasks)
    {
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        int nodeMapSize = requireNonNull(nodeMap, "nodeMap is null").getActiveNodes().size();
        this.nodeTotalSplitCount = new HashMap<>(nodeMapSize);
        this.stageQueuedSplitInfo = new HashMap<>(nodeMapSize);

        for (RemoteTask task : existingTasks) {
            checkArgument(stageQueuedSplitInfo.put(task.getNodeId(), new PendingSplitInfo(task.getQueuedPartitionedSplitCount(), task.getUnacknowledgedPartitionedSplitCount())) == null, "A single stage may not have multiple tasks running on the same node");
        }

        // pre-populate the assignment counts with zeros
        if (existingTasks.size() < nodeMapSize) {
            Function<String, PendingSplitInfo> createEmptySplitInfo = (ignored) -> new PendingSplitInfo(0, 0);
            for (InternalNode node : nodeMap.getActiveNodes()) {
                stageQueuedSplitInfo.computeIfAbsent(node.getNodeIdentifier(), createEmptySplitInfo);
            }
        }
    }

    public int getTotalSplitCount(InternalNode node)
    {
        int nodeTotalSplits = nodeTotalSplitCount.computeIfAbsent(node, nodeTaskMap::getPartitionedSplitsOnNode);
        PendingSplitInfo stageInfo = stageQueuedSplitInfo.get(node.getNodeIdentifier());
        return nodeTotalSplits + (stageInfo == null ? 0 : stageInfo.getAssignedSplitCount());
    }

    public int getQueuedSplitCountForStage(InternalNode node)
    {
        PendingSplitInfo stageInfo = stageQueuedSplitInfo.get(node.getNodeIdentifier());
        return stageInfo == null ? 0 : stageInfo.getQueuedSplitCount();
    }

    public int getUnacknowledgedSplitCountForStage(InternalNode node)
    {
        PendingSplitInfo stageInfo = stageQueuedSplitInfo.get(node.getNodeIdentifier());
        return stageInfo == null ? 0 : stageInfo.getUnacknowledgedSplitCount();
    }

    public void addAssignedSplit(InternalNode node)
    {
        String nodeId = node.getNodeIdentifier();
        // Avoids the extra per-invocation lambda allocation of computeIfAbsent since assigning a split to an existing task more common than creating a new task
        PendingSplitInfo stageInfo = stageQueuedSplitInfo.get(nodeId);
        if (stageInfo == null) {
            stageInfo = new PendingSplitInfo(0, 0);
            stageQueuedSplitInfo.put(nodeId, stageInfo);
        }
        stageInfo.addAssignedSplit();
    }

    private static final class PendingSplitInfo
    {
        private final int queuedSplitCount;
        private final int unacknowledgedSplitCount;
        private int assignedSplits;

        private PendingSplitInfo(int queuedSplitCount, int unacknowledgedSplitCount)
        {
            this.queuedSplitCount = queuedSplitCount;
            this.unacknowledgedSplitCount = unacknowledgedSplitCount;
        }

        public int getAssignedSplitCount()
        {
            return assignedSplits;
        }

        public int getQueuedSplitCount()
        {
            return queuedSplitCount + assignedSplits;
        }

        public int getUnacknowledgedSplitCount()
        {
            return unacknowledgedSplitCount + assignedSplits;
        }

        public void addAssignedSplit()
        {
            assignedSplits++;
        }
    }
}
