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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionCallRewriter;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isFunctionCallRewriterEnabled;
import static java.util.Objects.requireNonNull;

/**
 * Rule set that applies pluggable function call rewrites from connectors.
 * This allows connectors to transform specific function calls into alternative implementations.
 */
public class FunctionCallRewriting
        extends RowExpressionRewriteRuleSet
{
    public FunctionCallRewriting(
            Supplier<Set<FunctionCallRewriter>> rewritersSupplier,
            StandardFunctionResolution functionResolution)
    {
        super(createRewriter(rewritersSupplier, functionResolution));
    }

    private static PlanRowExpressionRewriter createRewriter(
            Supplier<Set<FunctionCallRewriter>> rewritersSupplier,
            StandardFunctionResolution functionResolution)
    {
        requireNonNull(rewritersSupplier, "rewritersSupplier is null");
        requireNonNull(functionResolution, "functionResolution is null");

        return (expression, context) -> rewrite(
                expression,
                context.getSession(),
                context.getVariableAllocator(),
                context.getIdAllocator(),
                rewritersSupplier,
                functionResolution);
    }

    @Override
    public boolean isRewriterEnabled(Session session)
    {
        return isFunctionCallRewriterEnabled(session);
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectRowExpressionRewriteRule(),
                filterRowExpressionRewriteRule());
    }

    private static RowExpression rewrite(
            RowExpression expression,
            Session session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Supplier<Set<FunctionCallRewriter>> rewritersSupplier,
            StandardFunctionResolution functionResolution)
    {
        Set<FunctionCallRewriter> rewriters = rewritersSupplier.get();
        if (rewriters.isEmpty()) {
            return expression;
        }

        return RowExpressionTreeRewriter.rewriteWith(
                new CallExpressionRewriter(session, rewriters, variableAllocator, idAllocator, functionResolution),
                expression);
    }

    private static class CallExpressionRewriter
            extends RowExpressionRewriter<Void>
    {
        private final Session session;
        private final Set<FunctionCallRewriter> rewriters;
        private final VariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final StandardFunctionResolution functionResolution;

        CallExpressionRewriter(
                Session session,
                Set<FunctionCallRewriter> rewriters,
                VariableAllocator variableAllocator,
                PlanNodeIdAllocator idAllocator,
                StandardFunctionResolution functionResolution)
        {
            this.session = requireNonNull(session, "session is null");
            this.rewriters = requireNonNull(rewriters, "rewriters is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Void unused, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            for (FunctionCallRewriter rewriter : rewriters) {
                Optional<RowExpression> result = rewriter.rewrite(node, session.toConnectorSession(), variableAllocator, idAllocator, functionResolution);
                if (result.isPresent()) {
                    return result.get();
                }
            }
            return null;
        }
    }
}
