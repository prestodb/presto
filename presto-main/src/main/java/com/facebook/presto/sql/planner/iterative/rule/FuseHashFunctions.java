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

import static com.google.common.collect.Iterables.getOnlyElement;

public class FuseHashFunctions
        extends ExpressionRewriteRuleSet
{
    public FuseHashFunctions()
    {
        super(createRewrite());
    }

    public static Expression rewrite(Expression expression)
    {
        return createRewrite().rewrite(expression, null);
    }

    private static ExpressionRewriter createRewrite()
    {
        return (expression, context) -> ExpressionTreeRewriter.rewriteWith(new com.facebook.presto.sql.tree.ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (node.getName().equals(QualifiedName.of("from_big_endian_64"))) {
                    return rewriteFromBigEndian64(node, context, treeRewriter);
                }
                return node;
            }
        }, expression);
    }

    private static Expression rewriteFromBigEndian64(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        Expression argument = treeRewriter.rewrite(getOnlyElement(node.getArguments()), context);
        if (argument instanceof FunctionCall) {
            FunctionCall function = (FunctionCall) argument;
            if (function.getName().equals(QualifiedName.of("xxhash64"))) {
                return rewriteXxhash64(function, context, treeRewriter);
            }
        }
        return node;
    }

    private static Expression rewriteXxhash64(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        Expression argument = treeRewriter.rewrite(getOnlyElement(node.getArguments()), context);
        if (argument instanceof FunctionCall) {
            FunctionCall function = (FunctionCall) argument;
            if (function.getName().equals(QualifiedName.of("to_big_endian_64"))) {
                argument = treeRewriter.rewrite(getOnlyElement(function.getArguments()), context);
            }
            return new FunctionCall(QualifiedName.of("$xxhash64"), ImmutableList.of(argument));
        }
        return node;
    }
}
