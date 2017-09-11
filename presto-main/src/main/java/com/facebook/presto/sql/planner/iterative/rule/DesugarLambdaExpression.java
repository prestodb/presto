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

import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.AggregationExpressionRewrite;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.FilterExpressionRewrite;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.JoinExpressionRewrite;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.ProjectExpressionRewrite;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.TableScanExpressionRewrite;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.ValuesExpressionRewrite;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class DesugarLambdaExpression
{
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                new ProjectExpressionRewrite(DesugarLambdaExpression::rewrite),
                new AggregationExpressionRewrite(DesugarLambdaExpression::rewrite),
                new FilterExpressionRewrite(DesugarLambdaExpression::rewrite),
                new TableScanExpressionRewrite(DesugarLambdaExpression::rewrite),
                new JoinExpressionRewrite(DesugarLambdaExpression::rewrite),
                new ValuesExpressionRewrite(DesugarLambdaExpression::rewrite));
    }

    private static Expression rewrite(Expression expression, Rule.Context context)
    {
        return LambdaCaptureDesugaringRewriter.rewrite(expression, context.getSymbolAllocator().getTypes(), context.getSymbolAllocator());
    }
}
