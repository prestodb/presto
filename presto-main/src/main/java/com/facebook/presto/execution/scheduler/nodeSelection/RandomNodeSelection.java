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

import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.NodeMap;
import com.facebook.presto.execution.scheduler.ResettableRandomizedIterator;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.execution.scheduler.NodeScheduler.randomizedNodes;
import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RandomNodeSelection
        implements NodeSelection
{
    private final boolean includeCoordinator;
    private final int minCandidates;

    private ResettableRandomizedIterator<InternalNode> randomCandidates;

    public RandomNodeSelection(
            NodeMap nodeMap,
            boolean includeCoordinator,
            int minCandidates,
            int maxTasksPerStage,
            List<RemoteTask> existingTasks)
    {
        requireNonNull(nodeMap, "nodeMap is null");
        requireNonNull(existingTasks, "existingTasks is null");

        this.includeCoordinator = includeCoordinator;
        this.minCandidates = minCandidates;
        this.randomCandidates = getRandomCandidates(maxTasksPerStage, nodeMap, existingTasks);
    }

    @Override
    public List<InternalNode> pickNodes(Split split)
    {
        randomCandidates.reset();
        return selectNodes(minCandidates, randomCandidates);
    }

    private ResettableRandomizedIterator<InternalNode> getRandomCandidates(int limit, NodeMap nodeMap, List<RemoteTask> existingTasks)
    {
        List<InternalNode> existingNodes = existingTasks.stream()
                .map(remoteTask -> nodeMap.getNodesByNodeId().get(remoteTask.getNodeId()))
                // nodes may sporadically disappear from the nodeMap if the announcement is delayed
                .filter(Objects::nonNull)
                .collect(toList());

        int alreadySelectedNodeCount = existingNodes.size();
        int nodeCount = nodeMap.getNodesByNodeId().size();

        if (alreadySelectedNodeCount < limit && alreadySelectedNodeCount < nodeCount) {
            List<InternalNode> moreNodes = selectNodes(limit - alreadySelectedNodeCount, randomizedNodes(nodeMap, includeCoordinator, newHashSet(existingNodes)));
            existingNodes.addAll(moreNodes);
        }
        verify(existingNodes.stream().allMatch(Objects::nonNull), "existingNodes list must not contain any nulls");
        return new ResettableRandomizedIterator<>(existingNodes);
    }
}
