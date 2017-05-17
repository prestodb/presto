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
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.sql.planner.iterative.rule.EliminateCrossJoins.buildJoinTree;
import static com.facebook.presto.sql.planner.iterative.rule.EliminateCrossJoins.getJoinOrder;
import static com.facebook.presto.sql.planner.iterative.rule.EliminateCrossJoins.isOriginalOrder;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@Deprecated
public class EliminateCrossJoins
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            Map<Symbol, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        if (!SystemSessionProperties.isJoinReorderingEnabled(session)) {
            return plan;
        }

        List<JoinGraph> joinGraphs = JoinGraph.buildFrom(plan);

        for (int i = joinGraphs.size() - 1; i >= 0; i--) {
            JoinGraph graph = joinGraphs.get(i);
            List<Integer> joinOrder = getJoinOrder(graph);
            if (isOriginalOrder(joinOrder)) {
                continue;
            }

            plan = rewriteWith(new Rewriter(idAllocator, graph, joinOrder), plan);
        }

        return plan;
    }

    private class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final JoinGraph graph;
        private final List<Integer> joinOrder;

        public Rewriter(PlanNodeIdAllocator idAllocator, JoinGraph graph, List<Integer> joinOrder)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.graph = requireNonNull(graph, "graph is null");
            this.joinOrder = ImmutableList.copyOf(requireNonNull(joinOrder, "joinOrder is null"));
            checkState(joinOrder.size() >= 2);
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<PlanNode> context)
        {
            if (!Objects.equals(node.getId(), graph.getRootId())) {
                return context.defaultRewrite(node, context.get());
            }

            return buildJoinTree(node.getOutputSymbols(), graph, joinOrder, idAllocator);
        }
    }
}
