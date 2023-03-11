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
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
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
    private static final Set<String> MAP_FUNCTIONS = ImmutableSet.of("map_values", "map_keys");

    private SimplifyCardinalityMapRewriter() {}

    public static RowExpression rewrite(RowExpression expression)
    {
        return RowExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private static class Visitor
            extends RowExpressionRewriter<Void>
    {
        @Override
        public RowExpression rewriteCall(CallExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            ImmutableList.Builder<RowExpression> rewrittenArguments = ImmutableList.builder();

            if (node.getDisplayName().equals("cardinality")) {
                for (RowExpression argument : node.getArguments()) {
                    if (argument instanceof CallExpression) {
                        CallExpression callExpression = (CallExpression) argument;
                        if (MAP_FUNCTIONS.contains(callExpression.getDisplayName()) && callExpression.getArguments().size() == 1) {
                            rewrittenArguments.add(treeRewriter.rewrite(callExpression.getArguments().get(0), context));
                            continue;
                        }
                    }
                    rewrittenArguments.add(treeRewriter.rewrite(argument, context));
                }
                return newFunctionIfRewritten(node, rewrittenArguments.build());
            }
            for (RowExpression argument : node.getArguments()) {
                rewrittenArguments.add(treeRewriter.rewrite(argument, context));
            }
            return newFunctionIfRewritten(node, rewrittenArguments.build());
        }

        private RowExpression newFunctionIfRewritten(CallExpression node, List<RowExpression> rewrittenArguments)
        {
            if (!node.getArguments().equals(rewrittenArguments)) {
                return new CallExpression(
                        node.getSourceLocation(),
                        node.getDisplayName(),
                        node.getFunctionHandle(),
                        node.getType(),
                        rewrittenArguments);
            }
            return node;
        }
    }
}
