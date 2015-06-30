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
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Type.NOT_EQUAL;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;

public final class ExpressionUtils
{
    private ExpressionUtils() {}

    public static List<Expression> extractConjuncts(Expression expression)
    {
        if (expression instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) expression).getType() == LogicalBinaryExpression.Type.AND) {
            LogicalBinaryExpression and = (LogicalBinaryExpression) expression;
            return ImmutableList.<Expression>builder()
                    .addAll(extractConjuncts(and.getLeft()))
                    .addAll(extractConjuncts(and.getRight()))
                    .build();
        }

        return ImmutableList.of(expression);
    }

    public static List<Expression> extractDisjuncts(Expression expression)
    {
        if (expression instanceof LogicalBinaryExpression && ((LogicalBinaryExpression) expression).getType() == LogicalBinaryExpression.Type.OR) {
            LogicalBinaryExpression or = (LogicalBinaryExpression) expression;
            return ImmutableList.<Expression>builder()
                    .addAll(extractDisjuncts(or.getLeft()))
                    .addAll(extractDisjuncts(or.getRight()))
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
        Preconditions.checkNotNull(type, "type is null");
        Preconditions.checkNotNull(expressions, "expressions is null");
        Preconditions.checkArgument(!Iterables.isEmpty(expressions), "expressions is empty");

        Iterator<Expression> iterator = expressions.iterator();

        Expression result = iterator.next();
        while (iterator.hasNext()) {
            result = new LogicalBinaryExpression(type, result, iterator.next());
        }

        return result;
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
        Preconditions.checkNotNull(expressions, "expressions is null");

        // Flatten all the expressions into their component conjuncts
        expressions = Iterables.concat(Iterables.transform(expressions, ExpressionUtils::extractConjuncts));

        // Strip out all true literal conjuncts
        expressions = Iterables.filter(expressions, not(Predicates.<Expression>equalTo(TRUE_LITERAL)));
        expressions = removeDuplicates(expressions);
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
        Preconditions.checkNotNull(expressions, "expressions is null");

        // Flatten all the expressions into their component disjuncts
        expressions = Iterables.concat(Iterables.transform(expressions, ExpressionUtils::extractDisjuncts));

        // Strip out all false literal disjuncts
        expressions = Iterables.filter(expressions, not(Predicates.<Expression>equalTo(FALSE_LITERAL)));
        expressions = removeDuplicates(expressions);
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

    public static ComparisonExpression.Type flipComparison(ComparisonExpression.Type type)
    {
        switch (type) {
            case EQUAL:
                return EQUAL;
            case NOT_EQUAL:
                return NOT_EQUAL;
            case LESS_THAN:
                return GREATER_THAN;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN_OR_EQUAL;
            case GREATER_THAN:
                return LESS_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM:
                return IS_DISTINCT_FROM;
            default:
                throw new IllegalArgumentException("Unsupported comparison: " + type);
        }
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
        Iterable<Expression> nonDeterministicDisjuncts = Iterables.filter(expressions, not(DeterminismEvaluator::isDeterministic));

        // Capture and de-dupe all deterministic predicates
        Iterable<Expression> deterministicDisjuncts = ImmutableSet.copyOf(Iterables.filter(expressions, DeterminismEvaluator::isDeterministic));

        return Iterables.concat(nonDeterministicDisjuncts, deterministicDisjuncts);
    }
}
