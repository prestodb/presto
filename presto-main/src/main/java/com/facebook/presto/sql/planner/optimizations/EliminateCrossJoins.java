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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.joins.JoinGraph;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkState;

public class EliminateCrossJoins
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!SystemSessionProperties.isJoinReorderingEnabled(session)) {
            return plan;
        }

        List<JoinGraph> joinGraphs = JoinGraph.buildFrom(plan);
        if (joinGraphs.size() != 1) {
            // we support only single join graph
            return plan;
        }
        JoinGraph graph = joinGraphs.get(0);

        List<Integer> joinOrder = getJoinOrder(graph);

        return plan;
    }

    /**
     * Given JoinGraph determine the order of joins between graph nodes
     * by traversing JoinGraph. Any graph traversal algorithm could be used
     * here (like BFS or DFS), but we use PriorityQueue to preserve
     * original JoinOrder as mush as it is possible. PriorityQueue returns
     * next nodes to join in order of their occurrence in original Plan.
     */
    public static List<Integer> getJoinOrder(JoinGraph graph)
    {
        ImmutableList.Builder<PlanNode> joinOrder = ImmutableList.builder();

        Map<PlanNodeId, Integer> priorities = new HashMap<>();
        for (int i = 0; i < graph.size(); i++) {
            priorities.put(graph.getNode(i).getId(), i);
        }

        PriorityQueue<PlanNode> nodesToVisit = new PriorityQueue<>(
                graph.size(),
                (Comparator<PlanNode>) (node1, node2) -> priorities.get(node1.getId()).compareTo(priorities.get(node2.getId())));
        Set<PlanNode> visited = new HashSet<>();

        nodesToVisit.add(graph.getNode(0));

        while (!nodesToVisit.isEmpty()) {
            PlanNode node = nodesToVisit.poll();
            if (!visited.contains(node)) {
                visited.add(node);
                joinOrder.add(node);
                for (JoinGraph.Edge edge : graph.getEdges(node)) {
                    nodesToVisit.add(edge.getTargetNode());
                }
            }

            if (nodesToVisit.isEmpty() && visited.size() < graph.size()) {
                // disconnected graph, find new starting point
                Optional<PlanNode> firstNotVisitedNode = graph.getNodes().stream()
                        .filter(graphNode -> !visited.contains(graphNode))
                        .findFirst();
                if (firstNotVisitedNode.isPresent()) {
                    nodesToVisit.add(firstNotVisitedNode.get());
                }
            }
        }

        checkState(visited.size() == graph.size());
        return joinOrder.build().stream()
                .map(node -> priorities.get(node.getId()))
                .collect(toImmutableList());
    }
}
