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

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;

import java.util.List;

import static com.facebook.presto.sql.ExpressionUtils.combinePredicates;
import static com.facebook.presto.sql.ExpressionUtils.extractPredicates;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class PushDownNegationsExpressionRewriter
{
    public static Expression pushDownNegations(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression);
    }

    private PushDownNegationsExpressionRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        @Override
        public Expression rewriteNotExpression(NotExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (node.getValue() instanceof LogicalBinaryExpression) {
                LogicalBinaryExpression child = (LogicalBinaryExpression) node.getValue();
                List<Expression> predicates = extractPredicates(child);
                List<Expression> negatedPredicates = predicates.stream().map(predicate -> treeRewriter.rewrite((Expression) new NotExpression(predicate), context)).collect(toImmutableList());
                return combinePredicates(child.getOperator().flip(), negatedPredicates);
            }
            else if (node.getValue() instanceof ComparisonExpression && ((ComparisonExpression) node.getValue()).getOperator() != IS_DISTINCT_FROM) {
                ComparisonExpression child = (ComparisonExpression) node.getValue();
                return new ComparisonExpression(child.getOperator().negate(), treeRewriter.rewrite(child.getLeft(), context), treeRewriter.rewrite(child.getRight(), context));
            }
            else if (node.getValue() instanceof NotExpression) {
                NotExpression child = (NotExpression) node.getValue();
                return treeRewriter.rewrite(child.getValue(), context);
            }

            return new NotExpression(treeRewriter.rewrite(node.getValue(), context));
        }
    }
}
