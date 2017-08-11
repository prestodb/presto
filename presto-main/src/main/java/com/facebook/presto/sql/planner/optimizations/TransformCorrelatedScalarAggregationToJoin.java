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
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.util.MorePredicates.isInstanceOfAny;
import static java.util.Objects.requireNonNull;

/**
 * Scalar aggregation is aggregation with GROUP BY 'a constant' (or empty GROUP BY).
 * It always returns single row in Presto.
 * <p>
 * This optimizer can rewrite correlated scalar aggregation subquery to left outer join in a way described here:
 * https://github.com/prestodb/presto/wiki/Correlated-subqueries
 * <p>
 * From:
 * <pre>
 * - LateralJoin (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (subquery) Aggregation(GROUP BY (); functions: [sum(F), count(), ...]
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 * to:
 * <pre>
 * - Aggregation(GROUP BY A, B, C, U; functions: [sum(F), count(non_null), ...]
 *   - Join(LEFT_OUTER, D = C)
 *     - AssignUniqueId(adds symbol U)
 *       - (input) plan which produces symbols: [A, B, C]
 *         - Filter(E > 5)
 *           - projection which adds no null symbol used for count() function
 *             - plan which produces symbols: [D, E, F]
 * </pre>
 * <p>
 * Note only conjunction predicates in FilterNode are supported
 */
@Deprecated
public class TransformCorrelatedScalarAggregationToJoin
        implements PlanOptimizer
{
    private final FunctionRegistry functionRegistry;

    public TransformCorrelatedScalarAggregationToJoin(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode plan,
            Session session,
            Map<Symbol, Type> types,
            SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(idAllocator, symbolAllocator, functionRegistry), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final FunctionRegistry functionRegistry;

        public Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, FunctionRegistry functionRegistry)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
        }

        @Override
        public PlanNode visitLateralJoin(LateralJoinNode node, RewriteContext<PlanNode> context)
        {
            LateralJoinNode rewrittenNode = (LateralJoinNode) context.defaultRewrite(node, context.get());
            if (!rewrittenNode.getCorrelation().isEmpty()) {
                Optional<AggregationNode> aggregation = searchFrom(rewrittenNode.getSubquery())
                        .where(AggregationNode.class::isInstance)
                        .recurseOnlyWhen(isInstanceOfAny(ProjectNode.class, EnforceSingleRowNode.class))
                        .findFirst();
                if (aggregation.isPresent() && aggregation.get().getGroupingKeys().isEmpty()) {
                    ScalarAggregationToJoinRewriter scalarAggregationToJoinRewriter = new ScalarAggregationToJoinRewriter(functionRegistry, symbolAllocator, idAllocator, noLookup());
                    return scalarAggregationToJoinRewriter.rewriteScalarAggregation(rewrittenNode, aggregation.get());
                }
            }
            return rewrittenNode;
        }
    }
}
