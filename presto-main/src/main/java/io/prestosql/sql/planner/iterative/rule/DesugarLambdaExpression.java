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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableSet;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.tree.Expression;

import java.util.Set;

public class DesugarLambdaExpression
        extends ExpressionRewriteRuleSet
{
    public DesugarLambdaExpression()
    {
        super(DesugarLambdaExpression::rewrite);
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectExpressionRewrite(),
                aggregationExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite());
    }

    private static Expression rewrite(Expression expression, Rule.Context context)
    {
        return LambdaCaptureDesugaringRewriter.rewrite(expression, context.getSymbolAllocator().getTypes(), context.getSymbolAllocator());
    }
}
