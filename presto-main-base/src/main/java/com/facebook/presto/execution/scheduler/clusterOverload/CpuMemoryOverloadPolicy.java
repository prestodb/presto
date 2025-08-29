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

package com.facebook.presto.execution.scheduler.clusterOverload;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.ClusterOverloadConfig;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.NodeLoadMetrics;
import jakarta.inject.Inject;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.NodeState.ACTIVE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * A policy that checks if cluster is overloaded based on CPU or memory metrics.
 * Supports two modes of operation:
 * - Percentage-based: Checks if the percentage of overloaded workers exceeds a threshold
 * - Count-based: Checks if the absolute count of overloaded workers exceeds a threshold
 */
public class CpuMemoryOverloadPolicy
        implements ClusterOverloadPolicy
{
    private static final Logger log = Logger.get(CpuMemoryOverloadPolicy.class);

    private final double allowedOverloadWorkersPct;
    private final double allowedOverloadWorkersCnt;
    private final String policyType;

    @Inject
    public CpuMemoryOverloadPolicy(ClusterOverloadConfig config)
    {
        this.allowedOverloadWorkersPct = config.getAllowedOverloadWorkersPct();
        this.allowedOverloadWorkersCnt = config.getAllowedOverloadWorkersCnt();
        this.policyType = requireNonNull(config.getOverloadPolicyType(), "policyType is null");
    }

    @Override
    public boolean isClusterOverloaded(InternalNodeManager nodeManager)
    {
        Set<InternalNode> activeNodes = nodeManager.getNodes(ACTIVE);
        if (activeNodes.isEmpty()) {
            return false;
        }

        OverloadStats stats = collectOverloadStats(activeNodes, nodeManager);
        return evaluateOverload(stats, activeNodes.size());
    }

    private OverloadStats collectOverloadStats(Set<InternalNode> activeNodes, InternalNodeManager nodeManager)
    {
        Set<String> overloadedNodeIds = new HashSet<>();
        int overloadedWorkersCnt = 0;

        for (InternalNode node : activeNodes) {
            Optional<NodeLoadMetrics> metricsOptional = nodeManager.getNodeLoadMetrics(node.getNodeIdentifier());
            String nodeId = node.getNodeIdentifier();

            if (!metricsOptional.isPresent()) {
                continue;
            }

            NodeLoadMetrics metrics = metricsOptional.get();

            // Check for CPU overload
            if (metrics.getCpuOverload()) {
                overloadedNodeIds.add(String.format("%s (CPU overloaded)", nodeId));
                overloadedWorkersCnt++;
                continue;
            }

            // Check for memory overload
            if (metrics.getMemoryOverload()) {
                overloadedNodeIds.add(String.format("%s (Memory overloaded)", nodeId));
                overloadedWorkersCnt++;
            }
        }

        return new OverloadStats(overloadedNodeIds, overloadedWorkersCnt);
    }

    private boolean evaluateOverload(OverloadStats stats, int totalNodes)
    {
        boolean isOverloaded;

        if (ClusterOverloadConfig.OVERLOAD_POLICY_PCT_BASED.equals(policyType)) {
            double overloadedWorkersPct = (double) stats.getOverloadedWorkersCnt() / totalNodes;
            isOverloaded = overloadedWorkersPct > allowedOverloadWorkersPct;

            if (isOverloaded) {
                logOverload(
                        String.format("%s%% of workers are overloaded (threshold: %s%%)",
                                format("%.2f", overloadedWorkersPct * 100),
                                format("%.2f", allowedOverloadWorkersPct * 100)),
                        stats.getOverloadedNodeIds());
            }
        }
        else if (ClusterOverloadConfig.OVERLOAD_POLICY_CNT_BASED.equals(policyType)) {
            isOverloaded = stats.getOverloadedWorkersCnt() > allowedOverloadWorkersCnt;

            if (isOverloaded) {
                logOverload(
                        String.format("%s workers are overloaded (threshold: %s workers)",
                                stats.getOverloadedWorkersCnt(), allowedOverloadWorkersCnt),
                        stats.getOverloadedNodeIds());
            }
        }
        else {
            throw new IllegalStateException("Unknown cluster overload policy type: " + policyType);
        }

        return isOverloaded;
    }

    private void logOverload(String message, Set<String> overloadedNodeIds)
    {
        log.warn("Cluster is overloaded: " + message);
        if (!overloadedNodeIds.isEmpty()) {
            log.warn("Overloaded nodes: %s", String.join(", ", overloadedNodeIds));
        }
    }

    @Override
    public String getName()
    {
        return "cpu-memory-overload-" +
                (ClusterOverloadConfig.OVERLOAD_POLICY_PCT_BASED.equals(policyType) ? "pct" : "cnt");
    }

    // Helper class to encapsulate overload statistics
    private static class OverloadStats
    {
        private final Set<String> overloadedNodeIds;
        private final int overloadedWorkersCnt;

        public OverloadStats(Set<String> overloadedNodeIds, int overloadedWorkersCnt)
        {
            this.overloadedNodeIds = overloadedNodeIds;
            this.overloadedWorkersCnt = overloadedWorkersCnt;
        }

        public Set<String> getOverloadedNodeIds()
        {
            return overloadedNodeIds;
        }

        public int getOverloadedWorkersCnt()
        {
            return overloadedWorkersCnt;
        }
    }
}
