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

import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class DesugarLambdaExpression
        extends RowExpressionRewriteRuleSet
{
    public DesugarLambdaExpression()
    {
        super(DesugarLambdaExpression::rewrite);
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectRowExpressionRewriteRule(),
                aggregationRowExpressionRewriteRule(),
                filterRowExpressionRewriteRule(),
                joinRowExpressionRewriteRule(),
                valueRowExpressionRewriteRule());
    }

    private static RowExpression rewrite(RowExpression expression, Rule.Context context)
    {
        return LambdaCaptureDesugaringRowExpressionRewriter.rewrite(expression, context.getVariableAllocator());
    }
}
