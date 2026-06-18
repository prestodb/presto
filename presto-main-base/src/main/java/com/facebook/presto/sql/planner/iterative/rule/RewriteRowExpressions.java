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
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.expressions.ExpressionOptimizerManager;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.annotations.VisibleForTesting;

import static com.facebook.presto.SystemSessionProperties.getExpressionOptimizerInRowExpressionRewrite;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static java.util.Objects.requireNonNull;

/**
 * A rule set that rewrites row expressions using a custom expression optimizer.
 */
public class RewriteRowExpressions
        extends RowExpressionRewriteRuleSet
{
    public RewriteRowExpressions(ExpressionOptimizerManager expressionOptimizerManager)
    {
        super(new Rewriter(expressionOptimizerManager));
    }

    @Override
    public boolean isRewriterEnabled(Session session)
    {
        return !getExpressionOptimizerInRowExpressionRewrite(session).isEmpty();
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final ExpressionOptimizerManager expressionOptimizerManager;

        public Rewriter(ExpressionOptimizerManager expressionOptimizerManager)
        {
            this.expressionOptimizerManager = requireNonNull(expressionOptimizerManager, "expressionOptimizerManager is null");
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return rewrite(expression, context.getSession());
        }

        private RowExpression rewrite(RowExpression expression, Session session)
        {
            if (getExpressionOptimizerInRowExpressionRewrite(session).isEmpty()) {
                return expression;
            }
            ExpressionOptimizer optimizer = expressionOptimizerManager.getExpressionOptimizer(getExpressionOptimizerInRowExpressionRewrite(session));
            return optimizer.optimize(expression, OPTIMIZED, session.toConnectorSession());
        }
    }

    @VisibleForTesting
    public static RowExpression rewrite(RowExpression expression, Session session, ExpressionOptimizerManager expressionOptimizerManager)
    {
        return new Rewriter(expressionOptimizerManager).rewrite(expression, session);
    }
}
