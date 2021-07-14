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

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class FlattenNestedConcat
        extends ExpressionRewriteRuleSet
{
    private static final List<QualifiedName> REWRITE_ENABLED_FUNCTIONS = ImmutableList.of(QualifiedName.of("map_concat"), QualifiedName.of("array_concat"));

    public FlattenNestedConcat()
    {
        super((expression, context) -> flattenExpression(expression));
    }

    public static Expression flattenExpression(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private static class Visitor
            extends com.facebook.presto.sql.tree.ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (!REWRITE_ENABLED_FUNCTIONS.contains(node.getName())) {
                return null;
            }
            ImmutableList.Builder<Expression> rewriteArguments = ImmutableList.builder();
            boolean nested = false;
            for (Expression argument : node.getArguments()) {
                if (argument instanceof FunctionCall && ((FunctionCall) argument).getName().equals(node.getName())) {
                    nested = true;
                    rewriteArguments.addAll(((FunctionCall) argument).getArguments());
                }
                else {
                    rewriteArguments.add(argument);
                }
            }
            if (nested) {
                FunctionCall rewriteNode = new FunctionCall(node.getName(), rewriteArguments.build());
                return treeRewriter.rewrite(rewriteNode, context);
            }
            return null;
        }
    }
}
