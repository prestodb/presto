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
package com.facebook.presto.sql.planner;

import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Plan
{
    private final PlanNode root;
    private final TypeProvider types;
    private final StatsAndCosts statsAndCosts;

    public Plan(PlanNode root, TypeProvider types, StatsAndCosts statsAndCosts)
    {
        this.root = requireNonNull(root, "root is null");
        this.types = requireNonNull(types, "types is null");
        this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public TypeProvider getTypes()
    {
        return types;
    }

    public StatsAndCosts getStatsAndCosts()
    {
        return statsAndCosts;
    }

    public Map<PlanNodeId, PlanNode> getPlanIdNodeMap()
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);
        ImmutableMap.Builder<PlanNodeId, PlanNode> planNodeMap = ImmutableMap.builder();
        for (PlanNode node : planIterator) {
            planNodeMap.put(node.getId(), node);
        }
        return planNodeMap.build();
    }
}
