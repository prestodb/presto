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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.prestosql.sql.planner.DeterminismEvaluator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.LogicalBinaryExpression;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.ExpressionUtils.combinePredicates;
import static io.prestosql.sql.ExpressionUtils.extractPredicates;
import static io.prestosql.sql.tree.LogicalBinaryExpression.Operator.OR;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class ExtractCommonPredicatesExpressionRewriter
{
    public static Expression extractCommonPredicates(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new Visitor(), expression, NodeContext.ROOT_NODE);
    }

    private ExtractCommonPredicatesExpressionRewriter() {}

    private static class Visitor
            extends ExpressionRewriter<NodeContext>
    {
        @Override
        public Expression rewriteExpression(Expression node, NodeContext context, ExpressionTreeRewriter<NodeContext> treeRewriter)
        {
            if (context.isRootNode()) {
                return treeRewriter.rewrite(node, NodeContext.NOT_ROOT_NODE);
            }

            return null;
        }

        @Override
        public Expression rewriteLogicalBinaryExpression(LogicalBinaryExpression node, NodeContext context, ExpressionTreeRewriter<NodeContext> treeRewriter)
        {
            Expression expression = combinePredicates(
                    node.getOperator(),
                    extractPredicates(node.getOperator(), node).stream()
                            .map(subExpression -> treeRewriter.rewrite(subExpression, NodeContext.NOT_ROOT_NODE))
                            .collect(toImmutableList()));

            if (!(expression instanceof LogicalBinaryExpression)) {
                return expression;
            }

            Expression simplified = extractCommonPredicates((LogicalBinaryExpression) expression);

            // Prefer AND LogicalBinaryExpression at the root if possible
            if (context.isRootNode() && simplified instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) simplified).getOperator() == OR) {
                return distributeIfPossible((LogicalBinaryExpression) simplified);
            }

            return simplified;
        }

        private static Expression extractCommonPredicates(LogicalBinaryExpression node)
        {
            List<List<Expression>> subPredicates = getSubPredicates(node);

            Set<Expression> commonPredicates = ImmutableSet.copyOf(subPredicates.stream()
                    .map(Visitor::filterDeterministicPredicates)
                    .reduce(Sets::intersection)
                    .orElse(emptySet()));

            List<List<Expression>> uncorrelatedSubPredicates = subPredicates.stream()
                    .map(predicateList -> removeAll(predicateList, commonPredicates))
                    .collect(toImmutableList());

            LogicalBinaryExpression.Operator flippedOperator = node.getOperator().flip();

            List<Expression> uncorrelatedPredicates = uncorrelatedSubPredicates.stream()
                    .map(predicate -> combinePredicates(flippedOperator, predicate))
                    .collect(toImmutableList());
            Expression combinedUncorrelatedPredicates = combinePredicates(node.getOperator(), uncorrelatedPredicates);

            return combinePredicates(flippedOperator, ImmutableList.<Expression>builder()
                    .addAll(commonPredicates)
                    .add(combinedUncorrelatedPredicates)
                    .build());
        }

        private static List<List<Expression>> getSubPredicates(LogicalBinaryExpression expression)
        {
            return extractPredicates(expression.getOperator(), expression).stream()
                    .map(predicate -> predicate instanceof LogicalBinaryExpression ?
                            extractPredicates((LogicalBinaryExpression) predicate) : ImmutableList.of(predicate))
                    .collect(toImmutableList());
        }

        /**
         * Applies the boolean distributive property.
         * <p>
         * For example:
         * ( A & B ) | ( C & D ) => ( A | C ) & ( A | D ) & ( B | C ) & ( B | D)
         * <p>
         * Returns the original expression if the expression is non-deterministic or if the distribution will
         * expand the expression by too much.
         */
        private static Expression distributeIfPossible(LogicalBinaryExpression expression)
        {
            if (!DeterminismEvaluator.isDeterministic(expression)) {
                // Do not distribute boolean expressions if there are any non-deterministic elements
                // TODO: This can be optimized further if non-deterministic elements are not repeated
                return expression;
            }
            List<Set<Expression>> subPredicates = getSubPredicates(expression).stream()
                    .map(ImmutableSet::copyOf)
                    .collect(toList());

            int originalBaseExpressions = subPredicates.stream()
                    .mapToInt(Set::size)
                    .sum();

            int newBaseExpressions;
            try {
                newBaseExpressions = Math.multiplyExact(subPredicates.stream()
                        .mapToInt(Set::size)
                        .reduce(Math::multiplyExact)
                        .getAsInt(), subPredicates.size());
            }
            catch (ArithmeticException e) {
                // Integer overflow from multiplication means there are too many expressions
                return expression;
            }

            if (newBaseExpressions > originalBaseExpressions * 2) {
                // Do not distribute boolean expressions if it would create 2x more base expressions
                // (e.g. A, B, C, D from the above example). This is just an arbitrary heuristic to
                // avoid cross product expression explosion.
                return expression;
            }

            Set<List<Expression>> crossProduct = Sets.cartesianProduct(subPredicates);

            return combinePredicates(
                    expression.getOperator().flip(),
                    crossProduct.stream()
                            .map(expressions -> combinePredicates(expression.getOperator(), expressions))
                            .collect(toImmutableList()));
        }

        private static Set<Expression> filterDeterministicPredicates(List<Expression> predicates)
        {
            return predicates.stream()
                    .filter(DeterminismEvaluator::isDeterministic)
                    .collect(toSet());
        }

        private static <T> List<T> removeAll(Collection<T> collection, Collection<T> elementsToRemove)
        {
            return collection.stream()
                    .filter(element -> !elementsToRemove.contains(element))
                    .collect(toImmutableList());
        }
    }

    private enum NodeContext
    {
        ROOT_NODE,
        NOT_ROOT_NODE;

        boolean isRootNode()
        {
            return this == ROOT_NODE;
        }
    }
}
