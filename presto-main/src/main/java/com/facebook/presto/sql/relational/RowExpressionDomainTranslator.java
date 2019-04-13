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
package com.facebook.presto.sql.relational;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.predicate.DiscreteValues;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker;
import com.facebook.presto.spi.predicate.Range;
import com.facebook.presto.spi.predicate.Ranges;
import com.facebook.presto.spi.predicate.SortedRangeSet;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.TupleDomain.ColumnDomain;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.PeekingIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.FALSE;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.TRUE;
import static com.facebook.presto.sql.relational.LogicalRowExpressions.and;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.builder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

public final class RowExpressionDomainTranslator
{
    private final FunctionManager functionManager;
    private final LogicalRowExpressions logicalRowExpressions;
    private final StandardFunctionResolution functionResolution;

    public RowExpressionDomainTranslator(FunctionManager functionManager)
    {
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
        this.logicalRowExpressions = new LogicalRowExpressions(functionManager);
        this.functionResolution = new StandardFunctionResolution(functionManager);
    }

    // This is only used in test
    public RowExpression toPredicate(TupleDomain<? extends RowExpression> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return FALSE;
        }

        Map<? extends RowExpression, Domain> domains = tupleDomain.getDomains().get();
        return domains.entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getKey().toString()))
                .map(entry -> toPredicate(entry.getValue(), entry.getKey()))
                .collect(collectingAndThen(toImmutableList(), logicalRowExpressions::combineConjuncts));
    }

    private RowExpression toPredicate(Domain domain, RowExpression reference)
    {
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? isNull(reference) : FALSE;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? TRUE : not(functionResolution, isNull(reference));
        }

        List<RowExpression> disjuncts = new ArrayList<>();

        disjuncts.addAll(domain.getValues().getValuesProcessor().transform(
                ranges -> extractDisjuncts(domain.getType(), ranges, reference),
                discreteValues -> extractDisjuncts(domain.getType(), discreteValues, reference),
                allOrNone -> {
                    throw new IllegalStateException("Case should not be reachable");
                }));

        // Add nullability disjuncts
        if (domain.isNullAllowed()) {
            disjuncts.add(isNull(reference));
        }

        return logicalRowExpressions.combineDisjunctsWithDefault(disjuncts, TRUE);
    }

    private RowExpression processRange(Type type, Range range, RowExpression reference)
    {
        if (range.isAll()) {
            return TRUE;
        }

        if (isBetween(range)) {
            // specialize the range with BETWEEN expression if possible b/c it is currently more efficient
            return call(
                    BETWEEN.name(),
                    functionManager.resolveOperator(BETWEEN, fromTypes(reference.getType(), type, type)),
                    BOOLEAN,
                    reference,
                    toRowExpression(range.getLow().getValue(), type),
                    toRowExpression(range.getHigh().getValue(), type));
        }

        List<RowExpression> rangeConjuncts = new ArrayList<>();
        if (!range.getLow().isLowerUnbounded()) {
            switch (range.getLow().getBound()) {
                case ABOVE:
                    rangeConjuncts.add(greaterThan(reference, toRowExpression(range.getLow().getValue(), type)));
                    break;
                case EXACTLY:
                    rangeConjuncts.add(greaterThanOrEqual(reference, toRowExpression(range.getLow().getValue(), type)));
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
                    rangeConjuncts.add(lessThanOrEqual(reference, toRowExpression(range.getHigh().getValue(), type)));
                    break;
                case BELOW:
                    rangeConjuncts.add(lessThan(reference, toRowExpression(range.getHigh().getValue(), type)));
                    break;
                default:
                    throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
            }
        }
        // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
        checkState(!rangeConjuncts.isEmpty());
        return logicalRowExpressions.combineConjuncts(rangeConjuncts);
    }

    private RowExpression combineRangeWithExcludedPoints(Type type, RowExpression reference, Range range, List<RowExpression> excludedPoints)
    {
        if (excludedPoints.isEmpty()) {
            return processRange(type, range, reference);
        }

        RowExpression excludedPointsExpression = not(functionResolution, in(reference, excludedPoints));
        if (excludedPoints.size() == 1) {
            excludedPointsExpression = notEqual(reference, getOnlyElement(excludedPoints));
        }

        return logicalRowExpressions.combineConjuncts(processRange(type, range, reference), excludedPointsExpression);
    }

    private List<RowExpression> extractDisjuncts(Type type, Ranges ranges, RowExpression reference)
    {
        List<RowExpression> disjuncts = new ArrayList<>();
        List<RowExpression> singleValues = new ArrayList<>();
        List<Range> orderedRanges = ranges.getOrderedRanges();

        SortedRangeSet sortedRangeSet = SortedRangeSet.copyOf(type, orderedRanges);
        SortedRangeSet complement = sortedRangeSet.complement();

        List<Range> singleValueExclusionsList = complement.getOrderedRanges().stream().filter(Range::isSingleValue).collect(toList());
        List<Range> originalUnionSingleValues = SortedRangeSet.copyOf(type, singleValueExclusionsList).union(sortedRangeSet).getOrderedRanges();
        PeekingIterator<Range> singleValueExclusions = peekingIterator(singleValueExclusionsList.iterator());

        for (Range range : originalUnionSingleValues) {
            if (range.isSingleValue()) {
                singleValues.add(toRowExpression(range.getSingleValue(), type));
                continue;
            }

            // attempt to optimize ranges that can be coalesced as long as single value points are excluded
            List<RowExpression> singleValuesInRange = new ArrayList<>();
            while (singleValueExclusions.hasNext() && range.contains(singleValueExclusions.peek())) {
                singleValuesInRange.add(toRowExpression(singleValueExclusions.next().getSingleValue(), type));
            }

            if (!singleValuesInRange.isEmpty()) {
                disjuncts.add(combineRangeWithExcludedPoints(type, reference, range, singleValuesInRange));
                continue;
            }

            disjuncts.add(processRange(type, range, reference));
        }

        // Add back all of the possible single values either as an equality or an IN predicate
        if (singleValues.size() == 1) {
            disjuncts.add(equal(reference, getOnlyElement(singleValues)));
        }
        else if (singleValues.size() > 1) {
            disjuncts.add(in(reference, singleValues));
        }
        return disjuncts;
    }

    private List<RowExpression> extractDisjuncts(Type type, DiscreteValues discreteValues, RowExpression reference)
    {
        List<RowExpression> values = discreteValues.getValues().stream()
                .map(object -> toRowExpression(object, type))
                .collect(toList());

        // If values is empty, then the equatableValues was either ALL or NONE, both of which should already have been checked for
        checkState(!values.isEmpty());

        RowExpression predicate;
        if (values.size() == 1) {
            predicate = equal(reference, getOnlyElement(values));
        }
        else {
            predicate = in(reference, values);
        }

        if (!discreteValues.isWhiteList()) {
            predicate = not(functionResolution, predicate);
        }
        return ImmutableList.of(predicate);
    }

    private static boolean isBetween(Range range)
    {
        return !range.getLow().isLowerUnbounded() && range.getLow().getBound() == Marker.Bound.EXACTLY
                && !range.getHigh().isUpperUnbounded() && range.getHigh().getBound() == Marker.Bound.EXACTLY;
    }

    /**
     * Convert an Expression predicate into an ExtractionResult consisting of:
     * 1) A successfully extracted TupleDomain
     * 2) An Expression fragment which represents the part of the original Expression that will need to be re-evaluated
     * after filtering with the TupleDomain.
     */
    public ExtractionResult fromPredicate(Metadata metadata, Session session, RowExpression predicate)
    {
        return fromDomainExtraction(new DomainExtractor(metadata.getFunctionManager(), metadata.getTypeManager(), new InterpretedPredicateOptimizer(metadata, session))
                .extract(predicate));
    }

    private static RowExpression isNull(RowExpression expression)
    {
        return new SpecialFormExpression(IS_NULL, BOOLEAN, expression);
    }

    private static RowExpression not(StandardFunctionResolution resolution, RowExpression expression)
    {
        return call("not", resolution.notFunction(), expression.getType(), expression);
    }

    private RowExpression in(RowExpression value, List<RowExpression> inList)
    {
        return new SpecialFormExpression(IN, BOOLEAN, ImmutableList.<RowExpression>builder().add(value).addAll(inList).build());
    }

    private RowExpression binaryOperator(OperatorType operatorType, RowExpression left, RowExpression right)
    {
        return call(operatorType.name(), functionManager.resolveOperator(operatorType, fromTypes(left.getType(), right.getType())), BOOLEAN, left, right);
    }

    private RowExpression greaterThan(RowExpression left, RowExpression right)
    {
        return binaryOperator(OperatorType.GREATER_THAN, left, right);
    }

    private RowExpression lessThan(RowExpression left, RowExpression right)
    {
        return binaryOperator(OperatorType.LESS_THAN, left, right);
    }

    private RowExpression greaterThanOrEqual(RowExpression left, RowExpression right)
    {
        return binaryOperator(GREATER_THAN_OR_EQUAL, left, right);
    }

    private RowExpression lessThanOrEqual(RowExpression left, RowExpression right)
    {
        return binaryOperator(LESS_THAN_OR_EQUAL, left, right);
    }

    private RowExpression equal(RowExpression left, RowExpression right)
    {
        return binaryOperator(EQUAL, left, right);
    }

    private RowExpression notEqual(RowExpression left, RowExpression right)
    {
        return binaryOperator(NOT_EQUAL, left, right);
    }

    public ExtractionResult fromDomainExtraction(DomainExtractor.ExtractionResult result)
    {
        Optional<List<ColumnDomain<RowExpression>>> domains = result.getTupleDomain().getColumnDomains();
        if (domains.isPresent()) {
            ImmutableMap.Builder<VariableReferenceExpression, Domain> variableDomains = ImmutableMap.builder();
            ImmutableList.Builder<RowExpression> unResolvable = builder();
            if (result.getRemainingExpression() != TRUE) {
                unResolvable.add(result.getRemainingExpression());
            }
            for (ColumnDomain<RowExpression> domain : domains.get()) {
                if (domain.getColumn() instanceof VariableReferenceExpression) {
                    variableDomains.put((VariableReferenceExpression) domain.getColumn(), domain.getDomain());
                }
                else if (!domain.getDomain().isAll()) {
                    unResolvable.add(toPredicate(domain.getDomain(), domain.getColumn()));
                }
            }
            return new ExtractionResult(TupleDomain.withColumnDomains(variableDomains.build()), and(unResolvable.build()));
        }
        return new ExtractionResult(TupleDomain.none(), result.getRemainingExpression());
    }

    public static class ExtractionResult
    {
        private final TupleDomain<VariableReferenceExpression> tupleDomain;
        private final RowExpression remainingExpression;

        private ExtractionResult(TupleDomain<VariableReferenceExpression> tupleDomain, RowExpression remainingExpression)
        {
            this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        }

        public TupleDomain<VariableReferenceExpression> getTupleDomain()
        {
            return tupleDomain;
        }

        public RowExpression getRemainingExpression()
        {
            return remainingExpression;
        }
    }
}
