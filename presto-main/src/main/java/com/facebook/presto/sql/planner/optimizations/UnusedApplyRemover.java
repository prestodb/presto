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

import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.ExpressionExtractor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.List;

/**
 * Removes ApplyNode which subquery produces given Expression (valueList of InPredicate or SymbolReference) only if
 * that Expression is not used.
 */
public class UnusedApplyRemover
        extends ApplyNodeRewriter
{
    public static PlanNode pruneUnreferencedApplyNodes(List<Expression> removedExpressions, PlanNode node)
    {
        for (Expression removedExpression : removedExpressions) {
            for (Symbol symbol : DependencyExtractor.extractUnique(removedExpression)) {
                node = SimplePlanRewriter.rewriteWith(new UnusedApplyRemover(symbol.toSymbolReference()), node, null);
            }
        }
        return node;
    }

    public UnusedApplyRemover(SymbolReference symbolReference)
    {
        super(symbolReference);
    }

    @Override
    protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
    {
        if (usesSymbol(node, Symbol.from(reference))) {
            return node;
        }
        return context.defaultRewrite(node);
    }

    @Override
    public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
    {
        if (usesSymbol(node, Symbol.from(reference))) {
            return node;
        }
        return context.defaultRewrite(node);
    }

    @Override
    protected PlanNode rewriteApply(ApplyNode node)
    {
        return node.getInput();
    }

    private static boolean usesSymbol(PlanNode node, Symbol symbol)
    {
        return ExpressionExtractor.extractExpressionsNonRecursive(node).stream()
                .anyMatch(nodeExpression -> DependencyExtractor.extractUnique(nodeExpression).contains(symbol));
    }
}
