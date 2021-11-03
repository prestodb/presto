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
package com.facebook.presto.sql;

import com.facebook.presto.sql.planner.ExpressionDeterminismEvaluator;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Deprecated
public final class ExpressionUtils
{
    private ExpressionUtils() {}

    public static List<Expression> extractConjuncts(Expression expression)
    {
        return extractPredicates(LogicalBinaryExpression.Operator.AND, expression);
    }

    public static List<Expression> extractDisjuncts(Expression expression)
    {
        return extractPredicates(LogicalBinaryExpression.Operator.OR, expression);
    }

    public static List<Expression> extractPredicates(LogicalBinaryExpression expression)
    {
        return extractPredicates(expression.getOperator(), expression);
    }

    public static List<Expression> extractPredicates(LogicalBinaryExpression.Operator operator, Expression expression)
    {
        if (expression instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) expression).getOperator() == operator) {
            LogicalBinaryExpression logicalBinaryExpression = (LogicalBinaryExpression) expression;
            return ImmutableList.<Expression>builder()
                    .addAll(extractPredicates(operator, logicalBinaryExpression.getLeft()))
                    .addAll(extractPredicates(operator, logicalBinaryExpression.getRight()))
                    .build();
        }

        return ImmutableList.of(expression);
    }

    public static Expression and(Expression... expressions)
    {
        return and(Arrays.asList(expressions));
    }

    public static Expression and(Collection<Expression> expressions)
    {
        return binaryExpression(LogicalBinaryExpression.Operator.AND, expressions);
    }

    public static Expression or(Expression... expressions)
    {
        return or(Arrays.asList(expressions));
    }

    public static Expression or(Collection<Expression> expressions)
    {
        return binaryExpression(LogicalBinaryExpression.Operator.OR, expressions);
    }

    public static Expression binaryExpression(LogicalBinaryExpression.Operator operator, Collection<Expression> expressions)
    {
        requireNonNull(operator, "operator is null");
        requireNonNull(expressions, "expressions is null");

        if (expressions.isEmpty()) {
            switch (operator) {
                case AND:
                    return TRUE_LITERAL;
                case OR:
                    return FALSE_LITERAL;
                default:
                    throw new IllegalArgumentException("Unsupported LogicalBinaryExpression operator");
            }
        }

        // Build balanced tree for efficient recursive processing that
        // preserves the evaluation order of the input expressions.
        //
        // The tree is built bottom up by combining pairs of elements into
        // binary AND expressions.
        //
        // Example:
        //
        // Initial state:
        //  a b c d e
        //
        // First iteration:
        //
        //  /\    /\   e
        // a  b  c  d
        //
        // Second iteration:
        //
        //    / \    e
        //  /\   /\
        // a  b c  d
        //
        //
        // Last iteration:
        //
        //      / \
        //    / \  e
        //  /\   /\
        // a  b c  d

        Queue<Expression> queue = new ArrayDeque<>(expressions);
        while (queue.size() > 1) {
            Queue<Expression> buffer = new ArrayDeque<>();

            // combine pairs of elements
            while (queue.size() >= 2) {
                buffer.add(new LogicalBinaryExpression(operator, queue.remove(), queue.remove()));
            }

            // if there's and odd number of elements, just append the last one
            if (!queue.isEmpty()) {
                buffer.add(queue.remove());
            }

            // continue processing the pairs that were just built
            queue = buffer;
        }

        return queue.remove();
    }

    public static Expression combinePredicates(LogicalBinaryExpression.Operator operator, Expression... expressions)
    {
        return combinePredicates(operator, Arrays.asList(expressions));
    }

    public static Expression combinePredicates(LogicalBinaryExpression.Operator operator, Collection<Expression> expressions)
    {
        if (operator == LogicalBinaryExpression.Operator.AND) {
            return combineConjuncts(expressions);
        }

        return combineDisjuncts(expressions);
    }

    public static Expression combineConjuncts(Expression... expressions)
    {
        return combineConjuncts(Arrays.asList(expressions));
    }

    public static Expression combineConjuncts(Collection<Expression> expressions)
    {
        requireNonNull(expressions, "expressions is null");

        List<Expression> conjuncts = expressions.stream()
                .flatMap(e -> ExpressionUtils.extractConjuncts(e).stream())
                .filter(e -> !e.equals(TRUE_LITERAL))
                .collect(toList());

        conjuncts = removeDuplicates(conjuncts);

        if (conjuncts.contains(FALSE_LITERAL)) {
            return FALSE_LITERAL;
        }

        return and(conjuncts);
    }

    public static Expression combineDisjuncts(Collection<Expression> expressions)
    {
        return combineDisjunctsWithDefault(expressions, FALSE_LITERAL);
    }

    public static Expression combineDisjunctsWithDefault(Collection<Expression> expressions, Expression emptyDefault)
    {
        requireNonNull(expressions, "expressions is null");

        List<Expression> disjuncts = expressions.stream()
                .flatMap(e -> ExpressionUtils.extractDisjuncts(e).stream())
                .filter(e -> !e.equals(FALSE_LITERAL))
                .collect(toList());

        disjuncts = removeDuplicates(disjuncts);

        if (disjuncts.contains(TRUE_LITERAL)) {
            return TRUE_LITERAL;
        }

        return disjuncts.isEmpty() ? emptyDefault : or(disjuncts);
    }

    public static Expression filterConjuncts(Expression expression, Predicate<Expression> predicate)
    {
        List<Expression> conjuncts = extractConjuncts(expression).stream()
                .filter(predicate)
                .collect(toList());

        return combineConjuncts(conjuncts);
    }

    /**
     * Removes duplicate deterministic expressions. Preserves the relative order
     * of the expressions in the list.
     */
    private static List<Expression> removeDuplicates(List<Expression> expressions)
    {
        Set<Expression> seen = new HashSet<>();

        ImmutableList.Builder<Expression> result = ImmutableList.builder();
        for (Expression expression : expressions) {
            if (!ExpressionDeterminismEvaluator.isDeterministic(expression)) {
                result.add(expression);
            }
            else if (!seen.contains(expression)) {
                result.add(expression);
                seen.add(expression);
            }
        }

        return result.build();
    }

    public static Expression normalize(Expression expression)
    {
        if (expression instanceof NotExpression) {
            NotExpression not = (NotExpression) expression;
            if (not.getValue() instanceof ComparisonExpression && ((ComparisonExpression) not.getValue()).getOperator() != IS_DISTINCT_FROM) {
                ComparisonExpression comparison = (ComparisonExpression) not.getValue();
                return new ComparisonExpression(comparison.getOperator().negate(), comparison.getLeft(), comparison.getRight());
            }
            if (not.getValue() instanceof NotExpression) {
                return normalize(((NotExpression) not.getValue()).getValue());
            }
        }
        return expression;
    }

    public static Expression rewriteIdentifiersToSymbolReferences(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new SymbolReference(node.getValue());
            }

            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new LambdaExpression(node.getArguments(), treeRewriter.rewrite(node.getBody(), context));
            }
        }, expression);
    }

    public static GroupingElement removeGroupingElementPrefix(GroupingElement groupingElement, Optional<Identifier> removablePrefix)
    {
        if (!removablePrefix.isPresent()) {
            return groupingElement;
        }
        if (groupingElement instanceof SimpleGroupBy) {
            boolean prefixRemoved = false;
            ImmutableList.Builder<Expression> rewrittenColumns = ImmutableList.builder();
            for (Expression column : groupingElement.getExpressions()) {
                Expression processedColumn = removeExpressionPrefix(column, removablePrefix);
                if (processedColumn != column) {
                    prefixRemoved = true;
                }
                rewrittenColumns.add(processedColumn);
            }
            if (prefixRemoved) {
                return new SimpleGroupBy(rewrittenColumns.build());
            }
        }
        return groupingElement;
    }

    public static SortItem removeSortItemPrefix(SortItem sortItem, Optional<Identifier> removablePrefix)
    {
        if (!removablePrefix.isPresent()) {
            return sortItem;
        }
        Expression processedExpression = removeExpressionPrefix(sortItem.getSortKey(), removablePrefix);
        if (processedExpression != sortItem.getSortKey()) {
            return new SortItem(processedExpression, sortItem.getOrdering(), sortItem.getNullOrdering());
        }
        return sortItem;
    }

    public static SingleColumn removeSingleColumnPrefix(SingleColumn singleColumn, Optional<Identifier> removablePrefix)
    {
        if (!removablePrefix.isPresent()) {
            return singleColumn;
        }
        Expression processedExpression = removeExpressionPrefix(singleColumn.getExpression(), removablePrefix);
        if (processedExpression != singleColumn.getExpression()) {
            return new SingleColumn(removeExpressionPrefix(singleColumn.getExpression(), removablePrefix), singleColumn.getAlias());
        }
        return singleColumn;
    }

    public static Expression removeExpressionPrefix(Expression expression, Optional<Identifier> removablePrefix)
    {
        if (!removablePrefix.isPresent()) {
            return expression;
        }
        if (expression instanceof DereferenceExpression) {
            return removeDereferenceExpressionElementPrefix((DereferenceExpression) expression, removablePrefix);
        }
        if (expression instanceof FunctionCall) {
            return removeFunctionCallPrefix((FunctionCall) expression, removablePrefix);
        }
        return expression;
    }

    private static Expression removeDereferenceExpressionElementPrefix(DereferenceExpression dereferenceExpression, Optional<Identifier> removablePrefix)
    {
        if (removablePrefix.isPresent() && removablePrefix.get().equals(dereferenceExpression.getBase())) {
            return dereferenceExpression.getField();
        }
        return dereferenceExpression;
    }

    private static FunctionCall removeFunctionCallPrefix(FunctionCall functionCall, Optional<Identifier> removablePrefix)
    {
        if (!removablePrefix.isPresent()) {
            return functionCall;
        }
        boolean prefixRemoved = false;
        ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();
        for (Expression argument : functionCall.getArguments()) {
            Expression processedArgument = removeExpressionPrefix(argument, removablePrefix);
            if (processedArgument != argument) {
                prefixRemoved = true;
            }
            rewrittenArguments.add(removeExpressionPrefix(argument, removablePrefix));
        }
        if (prefixRemoved) {
            return new FunctionCall(
                    functionCall.getName(),
                    functionCall.getWindow(),
                    functionCall.getFilter(),
                    functionCall.getOrderBy(),
                    functionCall.isDistinct(),
                    functionCall.isIgnoreNulls(),
                    rewrittenArguments.build());
        }
        return functionCall;
    }
}
