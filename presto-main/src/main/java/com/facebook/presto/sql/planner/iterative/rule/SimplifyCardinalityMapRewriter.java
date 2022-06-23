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
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Transforms:
 * <pre>
 * - Cardinality(Map_Values(map))
 *     - X
 * </pre>
 * Into:
 * <pre>
 * - Cardinality(map)
 *     - X
 * </pre>
 */
public class SimplifyCardinalityMapRewriter
{
    private static final Set<QualifiedName> MAP_FUNCTIONS = ImmutableSet.of(QualifiedName.of("map_values"), QualifiedName.of("map_keys"));

    private SimplifyCardinalityMapRewriter() {}

    public static Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();

            if (node.getName().equals(QualifiedName.of("cardinality"))) {
                for (Expression argument : node.getArguments()) {
                    if (argument instanceof FunctionCall) {
                        FunctionCall functionCall = (FunctionCall) argument;
                        if (MAP_FUNCTIONS.contains(functionCall.getName()) && functionCall.getArguments().size() == 1) {
                            rewrittenArguments.add(treeRewriter.rewrite(functionCall.getArguments().get(0), context));
                            continue;
                        }
                    }
                    rewrittenArguments.add(treeRewriter.rewrite(argument, context));
                }
                return newFunctionIfRewritten(node, rewrittenArguments);
            }
            for (Expression argument : node.getArguments()) {
                rewrittenArguments.add(treeRewriter.rewrite(argument, context));
            }
            return newFunctionIfRewritten(node, rewrittenArguments);
        }

        private Expression newFunctionIfRewritten(FunctionCall node, ImmutableList.Builder<Expression> rewrittenArguments)
        {
            if (!node.getArguments().equals(rewrittenArguments.build())) {
                return new FunctionCall(node.getName(), rewrittenArguments.build());
            }
            return node;
        }
    }
}
