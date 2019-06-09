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

package com.facebook.presto.cost;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class StatsAndCosts
{
    private static final StatsAndCosts EMPTY = new StatsAndCosts(ImmutableMap.of(), ImmutableMap.of());

    private final Map<PlanNodeId, PlanNodeStatsEstimate> stats;
    private final Map<PlanNodeId, PlanCostEstimate> costs;

    public static StatsAndCosts empty()
    {
        return EMPTY;
    }

    @JsonCreator
    public StatsAndCosts(
            @JsonProperty("stats") Map<PlanNodeId, PlanNodeStatsEstimate> stats,
            @JsonProperty("costs") Map<PlanNodeId, PlanCostEstimate> costs)
    {
        this.stats = ImmutableMap.copyOf(requireNonNull(stats, "stats is null"));
        this.costs = ImmutableMap.copyOf(requireNonNull(costs, "costs is null"));
    }

    @JsonProperty
    public Map<PlanNodeId, PlanNodeStatsEstimate> getStats()
    {
        return stats;
    }

    @JsonProperty
    public Map<PlanNodeId, PlanCostEstimate> getCosts()
    {
        return costs;
    }

    public StatsAndCosts getForSubplan(PlanNode root)
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> filteredStats = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanCostEstimate> filteredCosts = ImmutableMap.builder();
        for (PlanNode node : planIterator) {
            if (stats.containsKey(node.getId())) {
                filteredStats.put(node.getId(), stats.get(node.getId()));
            }
            if (costs.containsKey(node.getId())) {
                filteredCosts.put(node.getId(), costs.get(node.getId()));
            }
        }
        return new StatsAndCosts(filteredStats.build(), filteredCosts.build());
    }

    public static StatsAndCosts create(PlanNode root, StatsProvider statsProvider, CostProvider costProvider)
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> stats = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanCostEstimate> costs = ImmutableMap.builder();
        for (PlanNode node : planIterator) {
            stats.put(node.getId(), statsProvider.getStats(node));
            costs.put(node.getId(), costProvider.getCost(node));
        }
        return new StatsAndCosts(stats.build(), costs.build());
    }
}
