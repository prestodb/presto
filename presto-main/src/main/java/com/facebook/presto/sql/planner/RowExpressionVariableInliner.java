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
package com.facebook.presto.sql.planner;

import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public final class RowExpressionVariableInliner
        extends RowExpressionRewriter<Void>
{
    private final Set<String> excludedNames = new HashSet<>();
    private final Function<VariableReferenceExpression, ? extends RowExpression> mapping;

    private RowExpressionVariableInliner(Function<VariableReferenceExpression, ? extends RowExpression> mapping)
    {
        this.mapping = mapping;
    }

    public static RowExpression inlineVariables(Function<VariableReferenceExpression, ? extends RowExpression> mapping, RowExpression expression)
    {
        return RowExpressionTreeRewriter.rewriteWith(new RowExpressionVariableInliner(mapping), expression);
    }

    public static RowExpression inlineVariables(Map<VariableReferenceExpression, ? extends RowExpression> mapping, RowExpression expression)
    {
        return inlineVariables(mapping::get, expression);
    }

    @Override
    public RowExpression rewriteVariableReference(VariableReferenceExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
    {
        if (!excludedNames.contains(node.getName())) {
            RowExpression result = mapping.apply(node);
            checkState(result != null, "Cannot resolve symbol %s", node.getName());
            return result;
        }
        return null;
    }

    @Override
    public RowExpression rewriteLambda(LambdaDefinitionExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
    {
        checkArgument(!node.getArguments().stream().anyMatch(excludedNames::contains), "Lambda argument already contained in excluded names.");
        excludedNames.addAll(node.getArguments());
        RowExpression result = treeRewriter.defaultRewrite(node, context);
        excludedNames.removeAll(node.getArguments());
        return result;
    }
}
