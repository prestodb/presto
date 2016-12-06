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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.NullLiteral;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;
import static java.util.Objects.requireNonNull;

/**
 * Converts Single Distinct Aggregation into GroupBy
 *
 * Rewrite if and only if
 *  1 all aggregation functions have a single common distinct mask symbol
 *  2 all aggregation functions have mask
 *
 * Rewrite MarkDistinctNode into AggregationNode(use DistinctSymbols as GroupBy)
 * Add ProjectNode on top of the new AggregationNode, which adds null assignment for mask
 * All unused mask will be removed by PruneUnreferencedOutputs
 * Remove Distincts in the original AggregationNode
 */
@Deprecated
public class SingleDistinctOptimizer
        implements PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return PlanRewriter.rewriteWith(new Optimizer(idAllocator), plan, Optional.empty()).getPlanNode();
    }

    private static class Optimizer
            extends PlanRewriter<Optional<Symbol>, Boolean>
    {
        private final PlanNodeIdAllocator idAllocator;

        private Optimizer(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public Result<Boolean> visitAggregation(AggregationNode node, RewriteContext<Optional<Symbol>, Boolean> context)
        {
            // optimize if and only if
            // all aggregation functions have a single common distinct mask symbol
            // AND all aggregation functions have mask
            Set<Symbol> masks = ImmutableSet.copyOf(node.getMasks().values());
            if (masks.size() != 1 || node.getMasks().size() != node.getAggregations().size()) {
                return context.defaultRewrite(node, Optional.empty());
            }

            if (node.getAggregations().values().stream().map(FunctionCall::getFilter).anyMatch(Optional::isPresent)) {
                // Skip if any aggregation contains a filter
                return context.defaultRewrite(node, Optional.empty());
            }

            Result<Boolean> source = context.rewrite(node.getSource(), Optional.of(Iterables.getOnlyElement(masks)));
            if (source.getPayload() == null || !source.getPayload()) {
                return context.defaultRewrite(node, Optional.empty());
            }

            Map<Symbol, FunctionCall> aggregations = ImmutableMap.copyOf(Maps.transformValues(node.getAggregations(), call -> new FunctionCall(call.getName(), call.getWindow(), false, call.getArguments())));

            return new Result<>(
                    new AggregationNode(
                            idAllocator.getNextId(),
                            source.getPlanNode(),
                            aggregations,
                            node.getFunctions(),
                            Collections.emptyMap(),
                            node.getGroupingSets(),
                            node.getStep(),
                            node.getHashSymbol(),
                            node.getGroupIdSymbol()),
                    false);
        }

        @Override
        public Result<Boolean> visitMarkDistinct(MarkDistinctNode node, RewriteContext<Optional<Symbol>, Boolean> context)
        {
            Optional<Symbol> mask = context.get();

            if (!mask.isPresent() || !mask.get().equals(node.getMarkerSymbol())) {
                return context.defaultRewrite(node, Optional.empty());
            }

            AggregationNode aggregationNode = new AggregationNode(
                    idAllocator.getNextId(),
                    context.rewrite(node.getSource(), Optional.empty()).getPlanNode(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    Collections.emptyMap(),
                    ImmutableList.of(node.getDistinctSymbols()),
                    SINGLE,
                    node.getHashSymbol(),
                    Optional.empty());

            Assignments assignments = Assignments.builder()
                    .putIdentities(aggregationNode.getOutputSymbols())
                    .put(mask.get(), new NullLiteral()) // add null assignment for mask
                    .build();
            return new Result(
                    new ProjectNode(
                            idAllocator.getNextId(),
                            aggregationNode,
                            assignments),
                    true);
        }
    }
}
