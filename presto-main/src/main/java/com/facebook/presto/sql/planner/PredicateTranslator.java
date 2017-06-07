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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.predicate.AllExpression;
import com.facebook.presto.spi.predicate.AndExpression;
import com.facebook.presto.spi.predicate.DiscreteValues;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.DomainExpression;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.NoneExpression;
import com.facebook.presto.spi.predicate.NotExpression;
import com.facebook.presto.spi.predicate.OrExpression;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.Ranges;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleExpressionVisitor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.PeekingIterator;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.combineDisjunctsWithDefault;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.planner.LiteralInterpreter.toExpression;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.NOT_EQUAL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.stream.Collectors.toList;

public class PredicateTranslator<C>
        implements TupleExpressionVisitor<Expression, Void, C>
{
    @Override
    public Expression visitDomainExpression(DomainExpression expression, Void context)
    {
        return toPredicate(expression.getDomain(), ((Symbol) expression.getColumn()).toSymbolReference());
    }

    @Override
    public Expression visitAndExpression(AndExpression expression, Void context)
    {
        Expression leftExpression = (Expression) expression.getLeftExpression().accept(this, context);
        Expression rightExpression = (Expression) expression.getRightExpression().accept(this, context);
        return and(leftExpression, rightExpression);
    }

    @Override
    public Expression visitOrExpression(OrExpression expression, Void context)
    {
        Expression leftExpression = (Expression) expression.getLeftExpression().accept(this, context);
        Expression rightExpression = (Expression) expression.getRightExpression().accept(this, context);
        return or(leftExpression, rightExpression);
    }

    @Override
    public Expression visitNotExpression(NotExpression expression, Void context)
    {
        return new com.facebook.presto.sql.tree.NotExpression((Expression) expression.getExpression().accept(this, context));
    }

    @Override
    public Expression visitAllExpression(AllExpression expression, Void context)
    {
        return BooleanLiteral.TRUE_LITERAL;
    }

    @Override
    public Expression visitNoneExpression(NoneExpression expression, Void context)
    {
        return BooleanLiteral.FALSE_LITERAL;
    }

    private static Expression toPredicate(Domain domain, SymbolReference reference)
    {
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? new IsNullPredicate(reference) : FALSE_LITERAL;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? TRUE_LITERAL : new com.facebook.presto.sql.tree.NotExpression(new IsNullPredicate(reference));
        }

        List<Expression> disjuncts = new ArrayList<>();

        disjuncts.addAll(domain.getValues().getValuesProcessor().transform(
                ranges -> extractDisjuncts(domain.getType(), ranges, reference),
                discreteValues -> extractDisjuncts(domain.getType(), discreteValues, reference),
                allOrNone -> {
                    throw new IllegalStateException("Case should not be reachable");
                }));

        // Add nullability disjuncts
        if (domain.isNullAllowed()) {
            disjuncts.add(new IsNullPredicate(reference));
        }

        return combineDisjunctsWithDefault(disjuncts, TRUE_LITERAL);
    }

    private static List<Expression> extractDisjuncts(Type type, Ranges ranges, SymbolReference reference)
    {
        List<Expression> disjuncts = new ArrayList<>();
        List<Expression> singleValues = new ArrayList<>();
        List<Range> orderedRanges = ranges.getOrderedRanges();

        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(type, orderedRanges);
        SortedRangeSet complement = sortedRangeSet.complement();

        List<Range> singleValueExclusionsList = complement.getOrderedRanges().stream().filter(Range::isSingleValue).collect(toList());
        List<Range> originalUnionSingleValues = SortedRangeSet.copyOf(type, singleValueExclusionsList).union(sortedRangeSet).getOrderedRanges();
        PeekingIterator<Range> singleValueExclusions = peekingIterator(singleValueExclusionsList.iterator());

        for (Range range : originalUnionSingleValues) {
            if (range.isSingleValue()) {
                singleValues.add(toExpression(range.getSingleValue(), type));
                continue;
            }

            // attempt to optimize ranges that can be coalesced as long as single value points are excluded
            List<Expression> singleValuesInRange = new ArrayList<>();
            while (singleValueExclusions.hasNext() && range.contains(singleValueExclusions.peek())) {
                singleValuesInRange.add(toExpression(singleValueExclusions.next().getSingleValue(), type));
            }

            if (!singleValuesInRange.isEmpty()) {
                disjuncts.add(combineRangeWithExcludedPoints(type, reference, range, singleValuesInRange));
                continue;
            }

            disjuncts.add(processRange(type, range, reference));
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(new ComparisonExpression(EQUAL, reference, getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(new InPredicate(reference, new InListExpression(singleValues)));
        }
        return disjuncts;
    }

    private static List<Expression> extractDisjuncts(Type type, DiscreteValues discreteValues, SymbolReference reference)
    {
        List<Expression> values = discreteValues.getValues().stream()
                .map(object -> toExpression(object, type))
                .collect(toList());

        // If values is empty, then the equatableValues was either ALL or NONE, both of which should already have been checked for
        checkState(!values.isEmpty());

        Expression predicate;
        if (values.size() == 1) {
            predicate = new ComparisonExpression(EQUAL, reference, getOnlyElement(values));
        }
        else {
            predicate = new InPredicate(reference, new InListExpression(values));
        }

        if (!discreteValues.isWhiteList()) {
            predicate = new com.facebook.presto.sql.tree.NotExpression(predicate);
        }
        return ImmutableList.of(predicate);
    }

    private static boolean isBetween(Range range)
    {
        return !range.getLow().isLowerUnbounded() && range.getLow().getBound() == Marker.Bound.EXACTLY
                && !range.getHigh().isUpperUnbounded() && range.getHigh().getBound() == Marker.Bound.EXACTLY;
    }

    private static Expression processRange(Type type, Range range, SymbolReference reference)
    {
        if (range.isAll()) {
            return TRUE_LITERAL;
        }

        if (isBetween(range)) {
            // specialize the range with BETWEEN expression if possible b/c it is currently more efficient
            return new BetweenPredicate(reference, toExpression(range.getLow().getValue(), type), toExpression(range.getHigh().getValue(), type));
        }

        List<Expression> rangeConjuncts = new ArrayList<>();
        if (!range.getLow().isLowerUnbounded()) {
            switch (range.getLow().getBound()) {
                case ABOVE:
                    rangeConjuncts.add(new ComparisonExpression(GREATER_THAN, reference, toExpression(range.getLow().getValue(), type)));
                    break;
                case EXACTLY:
                    rangeConjuncts.add(new ComparisonExpression(GREATER_THAN_OR_EQUAL, reference, toExpression(range.getLow().getValue(),
                            type)));
                    break;
                case BELOW:
                    throw new IllegalStateException("Low Marker should never use BELOW bound: " + range);
                default:
                    throw new AssertionError("Unhandled bound: " + range.getLow().getBound());
            }
        }
        if (!range.getHigh().isUpperUnbounded()) {
            switch (range.getHigh().getBound()) {
                case ABOVE:
                    throw new IllegalStateException("High Marker should never use ABOVE bound: " + range);
                case EXACTLY:
                    rangeConjuncts.add(new ComparisonExpression(LESS_THAN_OR_EQUAL, reference, toExpression(range.getHigh().getValue(), type)));
                    break;
                case BELOW:
                    rangeConjuncts.add(new ComparisonExpression(LESS_THAN, reference, toExpression(range.getHigh().getValue(), type)));
                    break;
                default:
                    throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
            }
        }
        // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
        checkState(!rangeConjuncts.isEmpty());
        return combineConjuncts(rangeConjuncts);
    }

    private static Expression combineRangeWithExcludedPoints(Type type, SymbolReference reference, Range range, List<Expression> excludedPoints)
    {
        if (excludedPoints.isEmpty()) {
            return processRange(type, range, reference);
        }

        Expression excludedPointsExpression = new com.facebook.presto.sql.tree.NotExpression(new InPredicate(reference, new InListExpression(excludedPoints)));
        if (excludedPoints.size() == 1) {
            excludedPointsExpression = new ComparisonExpression(NOT_EQUAL, reference, getOnlyElement(excludedPoints));
        }

        return combineConjuncts(processRange(type, range, reference), excludedPointsExpression);
    }
}
