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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Map;

import static com.facebook.presto.sql.planner.ExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableMap;
import static java.util.Objects.requireNonNull;

/**
 * Calls {@link ApplyNodeRewriter#rewriteApply(ApplyNode)}} for Apply which produces given Expression
 */
public abstract class ApplyNodeRewriter
        extends SimplePlanRewriter<Void>
{
    protected SymbolReference reference;

    public ApplyNodeRewriter(SymbolReference reference)
    {
        this.reference = requireNonNull(reference, "reference is null");
    }

    @Override
    public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
    {
        Expression expression = replaceExpression(
                reference,
                mapAssignmentSymbolsToExpression(node.getAssignments()));

        if (expression instanceof SymbolReference) {
            reference = (SymbolReference) expression;
            return context.defaultRewrite(node, context.get());
        }
        else {
            return node;
        }
    }

    @Override
    public PlanNode visitApply(ApplyNode node, RewriteContext<Void> context)
    {
        if (node.getSubquery().getOutputSymbols().contains(Symbol.from(reference))) {
            return rewriteApply(node);
        }
        return context.defaultRewrite(node, context.get());
    }

    protected abstract PlanNode rewriteApply(ApplyNode node);

    protected static Map<Expression, Expression> mapAssignmentSymbolsToExpression(Map<Symbol, Expression> assignments)
    {
        return assignments.entrySet().stream()
                .collect(toImmutableMap(e -> e.getKey().toSymbolReference(), Map.Entry::getValue));
    }
}
