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
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;

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
    private SimplifyCardinalityMapRewriter() {}

    public static Expression rewrite(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private static class Visitor
            extends ExpressionRewriter<Object>
    {
        @Override
        public Expression rewriteFunctionCall(FunctionCall node, Object context, ExpressionTreeRewriter<Object> treeRewriter)
        {
            List<Expression> rewrittenArguments = new ArrayList<>();
            for (Expression argument : node.getArguments()) {
                if (node.getName().equals(QualifiedName.of("cardinality")) &&
                        argument instanceof FunctionCall &&
                        ((FunctionCall) argument).getName().equals(QualifiedName.of("map_values")) &&
                        ((FunctionCall) argument).getArguments().size() > 0) {
                    rewrittenArguments.add(treeRewriter.rewrite(((FunctionCall) argument).getArguments().get(0), context));
                } else {
                    rewrittenArguments.add(treeRewriter.rewrite(argument, context));
                }
            }
            return new FunctionCall(node.getName(), rewrittenArguments);
        }
    }
}

