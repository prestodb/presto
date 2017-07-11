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
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer can rewrite correlated no aggregation subquery to inner join in a way described here:
 * From:
 * <pre>
 * - Lateral (with correlation list: [B])
 *   - (input) plan which produces symbols: [A, B]
 *   - (subquery)
 *     - Filter(B = C AND D < 5)
 *       - plan which produces symbols: [C, D]
 * </pre>
 * to:
 * <pre>
 *   - Join(INNER, B = C)
 *       - (input) plan which produces symbols: [A, B]
 *       - Filter(D < 5)
 *          - plan which produces symbols: [C, D]
 * </pre>
 * <p>
 * Note only conjunction predicates in FilterNode are supported
 */
public class TransformCorrelatedNoAggregationSubqueryToJoin
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
        return rewriteWith(new Rewriter(idAllocator), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private final PlanNodeIdAllocator idAllocator;

        public Rewriter(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitLateralJoin(LateralJoinNode node, RewriteContext<PlanNode> context)
        {
            LateralJoinNode rewrittenNode = (LateralJoinNode) context.defaultRewrite(node, context.get());
            if (!rewrittenNode.getCorrelation().isEmpty()) {
                return rewriteNoAggregationSubquery(rewrittenNode);
            }
            return rewrittenNode;
        }

        private PlanNode rewriteNoAggregationSubquery(LateralJoinNode lateral)
        {
            List<Symbol> correlation = lateral.getCorrelation();
            PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(idAllocator, noLookup());
            Optional<DecorrelatedNode> source = decorrelator.decorrelateFilters(lateral.getSubquery(), correlation);
            if (!source.isPresent()) {
                return lateral;
            }

            return new JoinNode(
                    idAllocator.getNextId(),
                    JoinNode.Type.INNER,
                    lateral.getInput(),
                    source.get().getNode(),
                    ImmutableList.of(),
                    lateral.getOutputSymbols(),
                    source.get().getCorrelatedPredicates(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
        }
    }
}
