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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * Exists is modeled as:
 * <pre>
 *     - Project($0 > 0)
 *       - Aggregation(COUNT(*))
 *         - Limit(1)
 *           -- subquery
 * </pre>
 */
public class TransformExistsApplyToScalarApply
        implements PlanOptimizer
{
    private final Metadata metadata;

    public TransformExistsApplyToScalarApply(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(idAllocator, symbolAllocator, metadata), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>

    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final Metadata metadata;

        public Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Metadata metadata)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<PlanNode> context)
        {
            if (node.getSubqueryAssignments().size() != 1) {
                return context.defaultRewrite(node);
            }

            Expression expression = getOnlyElement(node.getSubqueryAssignments().values());
            if (!(expression instanceof ExistsPredicate)) {
                return context.defaultRewrite(node);
            }

            PlanNode input = context.rewrite(node.getInput());
            PlanNode subquery = context.rewrite(node.getSubquery());

            subquery = new LimitNode(idAllocator.getNextId(), subquery, 1, false);

            FunctionRegistry functionRegistry = metadata.getFunctionRegistry();
            QualifiedName countFunction = QualifiedName.of("count");
            Symbol count = symbolAllocator.newSymbol(countFunction.toString(), BIGINT);
            subquery = new AggregationNode(
                    idAllocator.getNextId(),
                    subquery,
                    ImmutableMap.of(count, new FunctionCall(countFunction, ImmutableList.of())),
                    ImmutableMap.of(count, functionRegistry.resolveFunction(countFunction, ImmutableList.of())),
                    ImmutableMap.of(),
                    ImmutableList.of(ImmutableList.of()),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty());

            ComparisonExpression countGreaterThanZero = new ComparisonExpression(GREATER_THAN, count.toSymbolReference(), new Cast(new LongLiteral("0"), BIGINT.toString()));

            Symbol existsSymbol = getOnlyElement(node.getSubqueryAssignments().keySet());
            subquery = new ProjectNode(
                    idAllocator.getNextId(),
                    subquery,
                    ImmutableMap.of(existsSymbol, countGreaterThanZero));

            return new ApplyNode(
                    node.getId(),
                    input,
                    subquery,
                    ImmutableMap.of(existsSymbol, existsSymbol.toSymbolReference()),
                    node.getCorrelation());
        }
    }
}
