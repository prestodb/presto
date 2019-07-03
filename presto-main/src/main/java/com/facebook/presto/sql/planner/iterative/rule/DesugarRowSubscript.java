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

import com.facebook.presto.sql.planner.DesugarRowSubscriptRewriter;
import com.facebook.presto.sql.planner.TypeAnalyzer;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DesugarRowSubscript
        extends ExpressionRewriteRuleSet
{
    public DesugarRowSubscript(TypeAnalyzer typeAnalyzer)
    {
        super(createRewrite(typeAnalyzer));
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

    private static ExpressionRewriter createRewrite(TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        return (expression, context) -> DesugarRowSubscriptRewriter.rewrite(expression, context.getSession(), typeAnalyzer,
                context.getSymbolAllocator());
    }
}
