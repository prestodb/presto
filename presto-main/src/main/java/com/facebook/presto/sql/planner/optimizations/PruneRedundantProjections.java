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
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.ExpressionNodeInliner;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * Removes projections that share identical expression derivations
 * <p>
 * Example: Will prune out either $a1 or $a2, even if the two projections are separated by other non-interfering Nodes.
 * Project (1) $a1 := $b1 + f($b2), $a2 := $b1 + $b3
 * Project (2) $b1 := $b1, $b2 := $b2, $b3 := f($b2)
 */
public class PruneRedundantProjections
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        return PlanRewriter.rewriteWith(new Rewriter(), plan).getPlanNode();
    }

    private static class Rewriter
            extends PlanRewriter<Void, Map<Symbol, Expression>>
    {
        @Override
        protected Result<Map<Symbol, Expression>> visitPlan(PlanNode node, RewriteContext<Void, Map<Symbol, Expression>> context)
        {
            // Return the expanded expressions from all sources
            return context.defaultRewrite(node, assignments -> assignments.stream()
                    .flatMap(assignment -> assignment.entrySet().stream())
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
        }

        @Override
        public Result<Map<Symbol, Expression>> visitProject(ProjectNode node, RewriteContext<Void, Map<Symbol, Expression>> context)
        {
            Result<Map<Symbol, Expression>> result = context.rewrite(node.getSource());

            Map<? extends Expression, Expression> sourceExpandedExpressions = result.getPayload().entrySet().stream()
                    .collect(toMap(entry -> entry.getKey().toQualifiedNameReference(), Map.Entry::getValue));

            // Include all of the source expanded expressions in the return payload
            ImmutableMap.Builder<Symbol, Expression> expandedExpressions = ImmutableMap.<Symbol, Expression>builder()
                    .putAll(result.getPayload());

            // Index of expanded Expressions to actual expressions that can be used for the projection
            Map<Expression, Expression> computedExpressions = new HashMap<>();

            ImmutableMap.Builder<Symbol, Expression> newAssignments = ImmutableMap.builder();

            // Projection may not refer to all source outputs, so make them available
            // for consideration if they have an expanded form
            for (Symbol sourceOutputSymbol : result.getPlanNode().getOutputSymbols()) {
                Expression expanded = result.getPayload().get(sourceOutputSymbol);
                if (expanded != null) {
                    computedExpressions.putIfAbsent(expanded, sourceOutputSymbol.toQualifiedNameReference());
                }
            }

            for (Map.Entry<Symbol, Expression> projection : node.getAssignments().entrySet()) {
                if (!DeterminismEvaluator.isDeterministic(projection.getValue())) {
                    newAssignments.put(projection.getKey(), projection.getValue());
                    continue;
                }

                Expression expanded = ExpressionTreeRewriter.rewriteWith(new ExpressionNodeInliner(sourceExpandedExpressions), projection.getValue());

                // Only include the expanded form if this is not an identity projection
                if (!projection.getKey().toQualifiedNameReference().equals(projection.getValue())) {
                    expandedExpressions.put(projection.getKey(), expanded);
                }

                Expression previousExpression = computedExpressions.putIfAbsent(expanded, projection.getValue());

                // Rewrite in terms of the previous expression if possible
                newAssignments.put(projection.getKey(), (previousExpression == null) ? projection.getValue() : previousExpression);
            }

            return new Result<>(new ProjectNode(node.getId(), result.getPlanNode(), newAssignments.build()), expandedExpressions.build());
        }
    }
}
