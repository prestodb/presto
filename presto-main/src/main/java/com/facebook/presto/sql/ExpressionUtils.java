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

import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.IS_DISTINCT_FROM;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class ExpressionUtils
{
    private ExpressionUtils() {}

    public static List<Expression> extractConjuncts(Expression expression)
    {
        return extractPredicates(LogicalBinaryExpression.Type.AND, expression);
    }

    public static List<Expression> extractDisjuncts(Expression expression)
    {
        return extractPredicates(LogicalBinaryExpression.Type.OR, expression);
    }

    public static List<Expression> extractPredicates(LogicalBinaryExpression expression)
    {
        return extractPredicates(expression.getType(), expression);
    }

    public static List<Expression> extractPredicates(LogicalBinaryExpression.Type type, Expression expression)
    {
        if (expression instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) expression).getType() == type) {
            LogicalBinaryExpression logicalBinaryExpression = (LogicalBinaryExpression) expression;
            return ImmutableList.<Expression>builder()
                    .addAll(extractPredicates(type, logicalBinaryExpression.getLeft()))
                    .addAll(extractPredicates(type, logicalBinaryExpression.getRight()))
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
        return binaryExpression(LogicalBinaryExpression.Type.AND, expressions);
    }

    public static Expression or(Expression... expressions)
    {
        return or(Arrays.asList(expressions));
    }

    public static Expression or(Collection<Expression> expressions)
    {
        return binaryExpression(LogicalBinaryExpression.Type.OR, expressions);
    }

    public static Expression binaryExpression(LogicalBinaryExpression.Type type, Collection<Expression> expressions)
    {
        requireNonNull(type, "type is null");
        requireNonNull(expressions, "expressions is null");
        Preconditions.checkArgument(!expressions.isEmpty(), "expressions is empty");

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
                buffer.add(new LogicalBinaryExpression(type, queue.remove(), queue.remove()));
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

    public static Expression combinePredicates(LogicalBinaryExpression.Type type, Expression... expressions)
    {
        return combinePredicates(type, Arrays.asList(expressions));
    }

    public static Expression combinePredicates(LogicalBinaryExpression.Type type, Collection<Expression> expressions)
    {
        if (type == LogicalBinaryExpression.Type.AND) {
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
        return combineConjunctsWithDefault(expressions, TRUE_LITERAL);
    }

    public static Expression combineConjunctsWithDefault(Collection<Expression> expressions, Expression emptyDefault)
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

        return conjuncts.isEmpty() ? emptyDefault : and(conjuncts);
    }

    public static Expression combineDisjuncts(Expression... expressions)
    {
        return combineDisjuncts(Arrays.asList(expressions));
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

    public static Expression stripNonDeterministicConjuncts(Expression expression)
    {
        List<Expression> conjuncts = extractConjuncts(expression).stream()
                .filter(DeterminismEvaluator::isDeterministic)
                .collect(toList());

        return combineConjuncts(conjuncts);
    }

    public static Expression stripDeterministicConjuncts(Expression expression)
    {
        return combineConjuncts(extractConjuncts(expression)
                .stream()
                .filter((conjunct) -> !DeterminismEvaluator.isDeterministic(conjunct))
                .collect(toImmutableList()));
    }

    public static Function<Expression, Expression> expressionOrNullSymbols(final Predicate<Symbol>... nullSymbolScopes)
    {
        return expression -> {
            ImmutableList.Builder<Expression> resultDisjunct = ImmutableList.builder();
            resultDisjunct.add(expression);

            for (Predicate<Symbol> nullSymbolScope : nullSymbolScopes) {
                List<Symbol> symbols = DependencyExtractor.extractUnique(expression).stream()
                        .filter(nullSymbolScope)
                        .collect(toImmutableList());

                if (Iterables.isEmpty(symbols)) {
                    continue;
                }

                ImmutableList.Builder<Expression> nullConjuncts = ImmutableList.builder();
                for (Symbol symbol : symbols) {
                    nullConjuncts.add(new IsNullPredicate(symbol.toSymbolReference()));
                }

                resultDisjunct.add(and(nullConjuncts.build()));
            }

            return or(resultDisjunct.build());
        };
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
            if (!DeterminismEvaluator.isDeterministic(expression)) {
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
            if (not.getValue() instanceof ComparisonExpression && ((ComparisonExpression) not.getValue()).getType() != IS_DISTINCT_FROM) {
                ComparisonExpression comparison = (ComparisonExpression) not.getValue();
                return new ComparisonExpression(comparison.getType().negate(), comparison.getLeft(), comparison.getRight());
            }
            if (not.getValue() instanceof NotExpression) {
                return normalize(((NotExpression) not.getValue()).getValue());
            }
        }
        return expression;
    }

    public static Expression rewriteQualifiedNamesToSymbolReferences(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>() {
            @Override
            public Expression rewriteQualifiedNameReference(QualifiedNameReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new SymbolReference(node.getName().toString());
            }

            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return new LambdaExpression(node.getArguments(), treeRewriter.rewrite(node.getBody(), context));
            }
        }, expression);
    }
}
