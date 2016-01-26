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
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.IS_DISTINCT_FROM;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.contains;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Objects.requireNonNull;

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

    public static Expression and(Iterable<Expression> expressions)
    {
        return binaryExpression(LogicalBinaryExpression.Type.AND, expressions);
    }

    public static Expression or(Expression... expressions)
    {
        return or(Arrays.asList(expressions));
    }

    public static Expression or(Iterable<Expression> expressions)
    {
        return binaryExpression(LogicalBinaryExpression.Type.OR, expressions);
    }

    public static Expression binaryExpression(LogicalBinaryExpression.Type type, Iterable<Expression> expressions)
    {
        requireNonNull(type, "type is null");
        requireNonNull(expressions, "expressions is null");
        Preconditions.checkArgument(!Iterables.isEmpty(expressions), "expressions is empty");

        // build balanced tree for efficient recursive processing
        Queue<Expression> queue = new ArrayDeque<>(newArrayList(expressions));
        while (queue.size() > 1) {
            queue.add(new LogicalBinaryExpression(type, queue.remove(), queue.remove()));
        }
        return queue.remove();
    }

    public static Expression combinePredicates(LogicalBinaryExpression.Type type, Expression... expressions)
    {
        return combinePredicates(type, Arrays.asList(expressions));
    }

    public static Expression combinePredicates(LogicalBinaryExpression.Type type, Iterable<Expression> expressions)
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

    public static Expression combineConjuncts(Iterable<Expression> expressions)
    {
        return combineConjunctsWithDefault(expressions, TRUE_LITERAL);
    }

    public static Expression combineConjunctsWithDefault(Iterable<Expression> expressions, Expression emptyDefault)
    {
        requireNonNull(expressions, "expressions is null");

        // Flatten all the expressions into their component conjuncts
        expressions = Iterables.concat(transform(expressions, ExpressionUtils::extractConjuncts));

        // Strip out all true literal conjuncts
        expressions = filter(expressions, not(Predicates.<Expression>equalTo(TRUE_LITERAL)));
        expressions = removeDuplicates(expressions);

        if (contains(expressions, FALSE_LITERAL)) {
            return FALSE_LITERAL;
        }

        return Iterables.isEmpty(expressions) ? emptyDefault : and(expressions);
    }

    public static Expression combineDisjuncts(Expression... expressions)
    {
        return combineDisjuncts(Arrays.asList(expressions));
    }

    public static Expression combineDisjuncts(Iterable<Expression> expressions)
    {
        return combineDisjunctsWithDefault(expressions, FALSE_LITERAL);
    }

    public static Expression combineDisjunctsWithDefault(Iterable<Expression> expressions, Expression emptyDefault)
    {
        requireNonNull(expressions, "expressions is null");

        // Flatten all the expressions into their component disjuncts
        expressions = Iterables.concat(transform(expressions, ExpressionUtils::extractDisjuncts));

        // Strip out all false literal disjuncts
        expressions = filter(expressions, not(Predicates.<Expression>equalTo(FALSE_LITERAL)));
        expressions = removeDuplicates(expressions);

        if (contains(expressions, TRUE_LITERAL)) {
            return TRUE_LITERAL;
        }

        return Iterables.isEmpty(expressions) ? emptyDefault : or(expressions);
    }

    public static Expression stripNonDeterministicConjuncts(Expression expression)
    {
        return combineConjuncts(filter(extractConjuncts(expression), DeterminismEvaluator::isDeterministic));
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
                Iterable<Symbol> symbols = filter(DependencyExtractor.extractUnique(expression), nullSymbolScope);
                if (Iterables.isEmpty(symbols)) {
                    continue;
                }

                ImmutableList.Builder<Expression> nullConjuncts = ImmutableList.builder();
                for (Symbol symbol : symbols) {
                    nullConjuncts.add(new IsNullPredicate(new QualifiedNameReference(symbol.toQualifiedName())));
                }

                resultDisjunct.add(and(nullConjuncts.build()));
            }

            return or(resultDisjunct.build());
        };
    }

    private static Iterable<Expression> removeDuplicates(Iterable<Expression> expressions)
    {
        // Capture all non-deterministic predicates
        Iterable<Expression> nonDeterministicDisjuncts = filter(expressions, not(DeterminismEvaluator::isDeterministic));

        // Capture and de-dupe all deterministic predicates
        Iterable<Expression> deterministicDisjuncts = ImmutableSet.copyOf(filter(expressions, DeterminismEvaluator::isDeterministic));

        return Iterables.concat(nonDeterministicDisjuncts, deterministicDisjuncts);
    }

    public static Expression normalize(Expression expression)
    {
        if (expression instanceof NotExpression) {
            NotExpression not = (NotExpression) expression;
            if (not.getValue() instanceof ComparisonExpression && ((ComparisonExpression) not.getValue()).getType() != IS_DISTINCT_FROM) {
                ComparisonExpression comparison = (ComparisonExpression) not.getValue();
                return new ComparisonExpression(comparison.getType().negate(), comparison.getLeft(), comparison.getRight());
            }
        }
        return expression;
    }
}
