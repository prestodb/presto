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
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.scheduler.NodeMap;
import com.facebook.presto.execution.scheduler.ResettableRandomizedIterator;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.ttl.ConfidenceBasedTTLInfo;
import com.facebook.presto.ttl.NodeTTL;
import com.facebook.presto.ttl.TTLFetcherManager;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.execution.scheduler.NodeScheduler.selectNodes;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class TTLRandomNodeSelection
        implements NodeSelection
{
    private static final Logger log = Logger.get(TTLRandomNodeSelection.class);
    private final boolean includeCoordinator;
    private final int minCandidates;

    private final ResettableRandomizedIterator<InternalNode> randomCandidates;
    private final TTLFetcherManager ttlFetcherManager;
    private final Duration estimatedExecutionTime;

    public TTLRandomNodeSelection(
            NodeMap nodeMap,
            boolean includeCoordinator,
            int minCandidates,
            int maxTasksPerStage,
            List<RemoteTask> existingTasks,
            TTLFetcherManager ttlFetcherManager,
            Duration estimatedExecutionTime)
    {
        requireNonNull(nodeMap, "nodeMap is null");
        requireNonNull(existingTasks, "existingTasks is null");

        this.includeCoordinator = includeCoordinator;
        this.minCandidates = minCandidates;
        this.ttlFetcherManager = ttlFetcherManager;
        this.estimatedExecutionTime = estimatedExecutionTime;
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
                .map(remoteTask -> nodeMap.getActiveNodesByNodeId().get(remoteTask.getNodeId()))
                // nodes may sporadically disappear from the nodeMap if the announcement is delayed
                .filter(Objects::nonNull)
                .collect(toList());

        int alreadySelectedNodeCount = existingNodes.size();

        List<InternalNode> nodes = nodeMap.getActiveNodes();

        Map<InternalNode, NodeTTL> nodeTTLInfo = ttlFetcherManager.getAllTTLs();

        Map<InternalNode, Optional<ConfidenceBasedTTLInfo>> ttlInfo = nodeTTLInfo
                .entrySet()
                .stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getTTLInfo()
                                .stream()
                                .min(Comparator.comparing(ConfidenceBasedTTLInfo::getExpiryInstant))));

        ImmutableList<InternalNode> eligibleNodes = nodes.stream().filter(ttlInfo::containsKey)
                .filter(node -> includeCoordinator || !node.isCoordinator())
                .filter(node -> !existingNodes.contains(node))
                .filter(node -> ttlInfo.get(node).isPresent())
                .filter(node -> isTTLEnough(ttlInfo.get(node).get(), estimatedExecutionTime))
                .collect(toImmutableList());

        log.info("TTL_ELIGIBLENODES %s (limit %s, alreadySelectedNodeCount %s)", eligibleNodes.size(), limit, alreadySelectedNodeCount);
        if (alreadySelectedNodeCount < limit && eligibleNodes.size() > 0) {
            List<InternalNode> moreNodes = selectNodes(limit - alreadySelectedNodeCount, new ResettableRandomizedIterator<>(eligibleNodes));
            existingNodes.addAll(moreNodes);
        }
        verify(existingNodes.stream().allMatch(Objects::nonNull), "existingNodes list must not contain any nulls");
        return new ResettableRandomizedIterator<>(existingNodes);
    }

    private boolean isTTLEnough(ConfidenceBasedTTLInfo ttlInfo, Duration estimatedExecutionTime)
    {
        Instant expiryTime = ttlInfo.getExpiryInstant();
        long timeRemaining = MILLIS.between(Instant.now(), expiryTime);
        return new Duration(Math.max(timeRemaining, 0), TimeUnit.MILLISECONDS).compareTo(estimatedExecutionTime) >= 0;
    }
}
