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

import com.facebook.presto.Session;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.DiscreteValues;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Marker;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.Ranges;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.Utils;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.PeekingIterator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.CastType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.combineDisjunctsWithDefault;
import static com.facebook.presto.sql.ExpressionUtils.or;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.Collections.emptyList;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

// No need to inherit from DomainTranslator; the class will be deprecated in the long term
@Deprecated
public final class ExpressionDomainTranslator
{
    private final LiteralEncoder literalEncoder;

    public ExpressionDomainTranslator(LiteralEncoder literalEncoder)
    {
        this.literalEncoder = requireNonNull(literalEncoder, "literalEncoder is null");
    }

    public Expression toPredicate(TupleDomain<String> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return FALSE_LITERAL;
        }

        Map<String, Domain> domains = tupleDomain.getDomains().get();
        return domains.entrySet().stream()
                .sorted(comparing(entry -> entry.getKey()))
                .map(entry -> toPredicate(entry.getValue(), new SymbolReference(entry.getKey())))
                .collect(collectingAndThen(toImmutableList(), ExpressionUtils::combineConjuncts));
    }

    private Expression toPredicate(Domain domain, SymbolReference reference)
    {
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? new IsNullPredicate(reference) : FALSE_LITERAL;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? TRUE_LITERAL : new NotExpression(new IsNullPredicate(reference));
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

    private Expression processRange(Type type, Range range, SymbolReference reference)
    {
        if (range.isAll()) {
            return TRUE_LITERAL;
        }

        if (isBetween(range)) {
            // specialize the range with BETWEEN expression if possible b/c it is currently more efficient
            return new BetweenPredicate(reference, literalEncoder.toExpression(range.getLow().getValue(), type), literalEncoder.toExpression(range.getHigh().getValue(), type));
        }

        List<Expression> rangeConjuncts = new ArrayList<>();
        if (!range.getLow().isLowerUnbounded()) {
            switch (range.getLow().getBound()) {
                case ABOVE:
                    rangeConjuncts.add(new ComparisonExpression(GREATER_THAN, reference, literalEncoder.toExpression(range.getLow().getValue(), type)));
                    break;
                case EXACTLY:
                    rangeConjuncts.add(new ComparisonExpression(GREATER_THAN_OR_EQUAL, reference, literalEncoder.toExpression(range.getLow().getValue(),
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
                    rangeConjuncts.add(new ComparisonExpression(LESS_THAN_OR_EQUAL, reference, literalEncoder.toExpression(range.getHigh().getValue(), type)));
                    break;
                case BELOW:
                    rangeConjuncts.add(new ComparisonExpression(LESS_THAN, reference, literalEncoder.toExpression(range.getHigh().getValue(), type)));
                    break;
                default:
                    throw new AssertionError("Unhandled bound: " + range.getHigh().getBound());
            }
        }
        // If rangeConjuncts is null, then the range was ALL, which should already have been checked for
        checkState(!rangeConjuncts.isEmpty());
        return combineConjuncts(rangeConjuncts);
    }

    private Expression combineRangeWithExcludedPoints(Type type, SymbolReference reference, Range range, List<Expression> excludedPoints)
    {
        if (excludedPoints.isEmpty()) {
            return processRange(type, range, reference);
        }

        Expression excludedPointsExpression = new NotExpression(new InPredicate(reference, new InListExpression(excludedPoints)));
        if (excludedPoints.size() == 1) {
            excludedPointsExpression = new ComparisonExpression(NOT_EQUAL, reference, getOnlyElement(excludedPoints));
        }

        return combineConjuncts(processRange(type, range, reference), excludedPointsExpression);
    }

    private List<Expression> extractDisjuncts(Type type, Ranges ranges, SymbolReference reference)
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
                singleValues.add(literalEncoder.toExpression(range.getSingleValue(), type));
                continue;
            }

            // attempt to optimize ranges that can be coalesced as long as single value points are excluded
            List<Expression> singleValuesInRange = new ArrayList<>();
            while (singleValueExclusions.hasNext() && range.contains(singleValueExclusions.peek())) {
                singleValuesInRange.add(literalEncoder.toExpression(singleValueExclusions.next().getSingleValue(), type));
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

    private List<Expression> extractDisjuncts(Type type, DiscreteValues discreteValues, SymbolReference reference)
    {
        List<Expression> values = discreteValues.getValues().stream()
                .map(object -> literalEncoder.toExpression(object, type))
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
            predicate = new NotExpression(predicate);
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
    public static ExtractionResult fromPredicate(
            Metadata metadata,
            Session session,
            Expression predicate,
            TypeProvider types)
    {
        return new Visitor(metadata, session, types).process(predicate, false);
    }

    private static class Visitor
            extends AstVisitor<ExtractionResult, Boolean>
    {
        private final Metadata metadata;
        private final LiteralEncoder literalEncoder;
        private final Session session;
        private final TypeProvider types;
        private final InterpretedFunctionInvoker functionInvoker;

        private Visitor(Metadata metadata, Session session, TypeProvider types)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionAndTypeManager());
        }

        private Type checkedTypeLookup(Expression expression)
        {
            Type type = types.get(expression);
            checkArgument(type != null, "Types is missing info for expression: %s", expression);
            return type;
        }

        private static ValueSet complementIfNecessary(ValueSet valueSet, boolean complement)
        {
            return complement ? valueSet.complement() : valueSet;
        }

        private static Domain complementIfNecessary(Domain domain, boolean complement)
        {
            return complement ? domain.complement() : domain;
        }

        private static Expression complementIfNecessary(Expression expression, boolean complement)
        {
            return complement ? new NotExpression(expression) : expression;
        }

        @Override
        protected ExtractionResult visitExpression(Expression node, Boolean complement)
        {
            // If we don't know how to process this node, the default response is to say that the TupleDomain is "all"
            return new ExtractionResult(TupleDomain.all(), complementIfNecessary(node, complement));
        }

        @Override
        protected ExtractionResult visitLogicalBinaryExpression(LogicalBinaryExpression node, Boolean complement)
        {
            ExtractionResult leftResult = process(node.getLeft(), complement);
            ExtractionResult rightResult = process(node.getRight(), complement);

            TupleDomain<String> leftTupleDomain = leftResult.getTupleDomain();
            TupleDomain<String> rightTupleDomain = rightResult.getTupleDomain();

            LogicalBinaryExpression.Operator operator = complement ? node.getOperator().flip() : node.getOperator();
            switch (operator) {
                case AND:
                    return new ExtractionResult(
                            leftTupleDomain.intersect(rightTupleDomain),
                            combineConjuncts(leftResult.getRemainingExpression(), rightResult.getRemainingExpression()));

                case OR:
                    TupleDomain<String> columnUnionedTupleDomain = TupleDomain.columnWiseUnion(leftTupleDomain, rightTupleDomain);

                    // In most cases, the columnUnionedTupleDomain is only a superset of the actual strict union
                    // and so we can return the current node as the remainingExpression so that all bounds will be double checked again at execution time.
                    Expression remainingExpression = complementIfNecessary(node, complement);

                    // However, there are a few cases where the column-wise union is actually equivalent to the strict union, so we if can detect
                    // some of these cases, we won't have to double check the bounds unnecessarily at execution time.

                    // We can only make inferences if the remaining expressions on both side are equal and deterministic
                    if (leftResult.getRemainingExpression().equals(rightResult.getRemainingExpression()) &&
                            ExpressionDeterminismEvaluator.isDeterministic(leftResult.getRemainingExpression())) {
                        // The column-wise union is equivalent to the strict union if
                        // 1) If both TupleDomains consist of the same exact single column (e.g. left TupleDomain => (a > 0), right TupleDomain => (a < 10))
                        // 2) If one TupleDomain is a superset of the other (e.g. left TupleDomain => (a > 0, b > 0 && b < 10), right TupleDomain => (a > 5, b = 5))
                        boolean matchingSingleSymbolDomains = !leftTupleDomain.isNone()
                                && !rightTupleDomain.isNone()
                                && leftTupleDomain.getDomains().get().size() == 1
                                && rightTupleDomain.getDomains().get().size() == 1
                                && leftTupleDomain.getDomains().get().keySet().equals(rightTupleDomain.getDomains().get().keySet());
                        boolean oneSideIsSuperSet = leftTupleDomain.contains(rightTupleDomain) || rightTupleDomain.contains(leftTupleDomain);

                        if (matchingSingleSymbolDomains || oneSideIsSuperSet) {
                            remainingExpression = leftResult.getRemainingExpression();
                        }
                    }

                    return new ExtractionResult(columnUnionedTupleDomain, remainingExpression);

                default:
                    throw new AssertionError("Unknown operator: " + node.getOperator());
            }
        }

        @Override
        protected ExtractionResult visitNotExpression(NotExpression node, Boolean complement)
        {
            return process(node.getValue(), !complement);
        }

        @Override
        protected ExtractionResult visitComparisonExpression(ComparisonExpression node, Boolean complement)
        {
            Optional<NormalizedSimpleComparison> optionalNormalized = toNormalizedSimpleComparison(node);
            if (!optionalNormalized.isPresent()) {
                return super.visitComparisonExpression(node, complement);
            }
            NormalizedSimpleComparison normalized = optionalNormalized.get();

            Expression symbolExpression = normalized.getSymbolExpression();
            if (symbolExpression instanceof SymbolReference) {
                String symbolName = ((SymbolReference) symbolExpression).getName();
                NullableValue value = normalized.getValue();
                Type type = value.getType(); // common type for symbol and value
                return createComparisonExtractionResult(normalized.getComparisonOperator(), symbolName, type, value.getValue(), complement);
            }
            else if (symbolExpression instanceof Cast) {
                Cast castExpression = (Cast) symbolExpression;
                if (!isImplicitCoercion(castExpression)) {
                    //
                    // we cannot use non-coercion cast to literal_type on symbol side to build tuple domain
                    //
                    // example which illustrates the problem:
                    //
                    // let t be of timestamp type:
                    //
                    // and expression be:
                    // cast(t as date) == date_literal
                    //
                    // after dropping cast we end up with:
                    //
                    // t == date_literal
                    //
                    // if we build tuple domain based coercion of date_literal to timestamp type we would
                    // end up with tuple domain with just one time point (cast(date_literal as timestamp).
                    // While we need range which maps to single date pointed by date_literal.
                    //
                    return super.visitComparisonExpression(node, complement);
                }

                Type castSourceType = typeOf(castExpression.getExpression(), session, metadata, types); // type of expression which is then cast to type of value

                // we use saturated floor cast value -> castSourceType to rewrite original expression to new one with one cast peeled off the symbol side
                Optional<Expression> coercedExpression = coerceComparisonWithRounding(
                        castSourceType, castExpression.getExpression(), normalized.getValue(), normalized.getComparisonOperator());

                if (coercedExpression.isPresent()) {
                    return process(coercedExpression.get(), complement);
                }

                return super.visitComparisonExpression(node, complement);
            }
            else {
                return super.visitComparisonExpression(node, complement);
            }
        }

        /**
         * Extract a normalized simple comparison between a QualifiedNameReference and a native value if possible.
         */
        private Optional<NormalizedSimpleComparison> toNormalizedSimpleComparison(ComparisonExpression comparison)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = analyzeExpression(comparison);
            Object left = ExpressionInterpreter.expressionOptimizer(comparison.getLeft(), metadata, session, expressionTypes).optimize(NoOpVariableResolver.INSTANCE);
            Object right = ExpressionInterpreter.expressionOptimizer(comparison.getRight(), metadata, session, expressionTypes).optimize(NoOpVariableResolver.INSTANCE);

            Type leftType = expressionTypes.get(NodeRef.of(comparison.getLeft()));
            Type rightType = expressionTypes.get(NodeRef.of(comparison.getRight()));

            // TODO: re-enable this check once we fix the type coercions in the optimizers
            // checkArgument(leftType.equals(rightType), "left and right type do not match in comparison expression (%s)", comparison);

            if (left instanceof Expression == right instanceof Expression) {
                // we expect one side to be expression and other to be value.
                return Optional.empty();
            }

            Expression symbolExpression;
            ComparisonExpression.Operator comparisonOperator;
            NullableValue value;

            if (left instanceof Expression) {
                symbolExpression = comparison.getLeft();
                comparisonOperator = comparison.getOperator();
                value = new NullableValue(rightType, right);
            }
            else {
                symbolExpression = comparison.getRight();
                comparisonOperator = comparison.getOperator().flip();
                value = new NullableValue(leftType, left);
            }

            return Optional.of(new NormalizedSimpleComparison(symbolExpression, comparisonOperator, value));
        }

        private boolean isImplicitCoercion(Cast cast)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = analyzeExpression(cast);
            Type actualType = expressionTypes.get(NodeRef.of(cast.getExpression()));
            Type expectedType = expressionTypes.get(NodeRef.<Expression>of(cast));
            return metadata.getFunctionAndTypeManager().canCoerce(actualType, expectedType);
        }

        private Map<NodeRef<Expression>, Type> analyzeExpression(Expression expression)
        {
            return ExpressionAnalyzer.getExpressionTypes(session, metadata, new SqlParser(), types, expression, emptyList(), WarningCollector.NOOP);
        }

        private static ExtractionResult createComparisonExtractionResult(ComparisonExpression.Operator comparisonOperator, String column, Type type, @Nullable Object value, boolean complement)
        {
            if (value == null) {
                switch (comparisonOperator) {
                    case EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case NOT_EQUAL:
                        return new ExtractionResult(TupleDomain.none(), TRUE_LITERAL);

                    case IS_DISTINCT_FROM:
                        Domain domain = complementIfNecessary(Domain.notNull(type), complement);
                        return new ExtractionResult(
                                TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)),
                                TRUE_LITERAL);

                    default:
                        throw new AssertionError("Unhandled operator: " + comparisonOperator);
                }
            }

            Domain domain;
            if (type.isOrderable()) {
                domain = extractOrderableDomain(comparisonOperator, type, value, complement);
            }
            else if (type.isComparable()) {
                domain = extractEquatableDomain(comparisonOperator, type, value, complement);
            }
            else {
                throw new AssertionError("Type cannot be used in a comparison expression (should have been caught in analysis): " + type);
            }

            return new ExtractionResult(
                    TupleDomain.withColumnDomains(ImmutableMap.of(column, domain)),
                    TRUE_LITERAL);
        }

        private static Domain extractOrderableDomain(ComparisonExpression.Operator comparisonOperator, Type type, Object value, boolean complement)
        {
            checkArgument(value != null);
            switch (comparisonOperator) {
                case EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.equal(type, value)), complement), false);
                case GREATER_THAN:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.greaterThan(type, value)), complement), false);
                case GREATER_THAN_OR_EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.greaterThanOrEqual(type, value)), complement), false);
                case LESS_THAN:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThan(type, value)), complement), false);
                case LESS_THAN_OR_EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThanOrEqual(type, value)), complement), false);
                case NOT_EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.ofRanges(Range.lessThan(type, value), Range.greaterThan(type, value)), complement), false);
                case IS_DISTINCT_FROM:
                    // Need to potential complement the whole domain for IS_DISTINCT_FROM since it is null-aware
                    return complementIfNecessary(Domain.create(ValueSet.ofRanges(Range.lessThan(type, value), Range.greaterThan(type, value)), true), complement);
                default:
                    throw new AssertionError("Unhandled operator: " + comparisonOperator);
            }
        }

        private static Domain extractEquatableDomain(ComparisonExpression.Operator comparisonOperator, Type type, Object value, boolean complement)
        {
            checkArgument(value != null);
            switch (comparisonOperator) {
                case EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.of(type, value), complement), false);
                case NOT_EQUAL:
                    return Domain.create(complementIfNecessary(ValueSet.of(type, value).complement(), complement), false);
                case IS_DISTINCT_FROM:
                    // Need to potential complement the whole domain for IS_DISTINCT_FROM since it is null-aware
                    return complementIfNecessary(Domain.create(ValueSet.of(type, value).complement(), true), complement);
                default:
                    throw new AssertionError("Unhandled operator: " + comparisonOperator);
            }
        }

        private Optional<Expression> coerceComparisonWithRounding(
                Type symbolExpressionType,
                Expression symbolExpression,
                NullableValue nullableValue,
                ComparisonExpression.Operator comparisonOperator)
        {
            requireNonNull(nullableValue, "nullableValue is null");
            if (nullableValue.isNull()) {
                return Optional.empty();
            }
            Type valueType = nullableValue.getType();
            Object value = nullableValue.getValue();
            return floorValue(valueType, symbolExpressionType, value)
                    .map((floorValue) -> rewriteComparisonExpression(symbolExpressionType, symbolExpression, valueType, value, floorValue, comparisonOperator));
        }

        private Expression rewriteComparisonExpression(
                Type symbolExpressionType,
                Expression symbolExpression,
                Type valueType,
                Object originalValue,
                Object coercedValue,
                ComparisonExpression.Operator comparisonOperator)
        {
            int originalComparedToCoerced = compareOriginalValueToCoerced(valueType, originalValue, symbolExpressionType, coercedValue);
            boolean coercedValueIsEqualToOriginal = originalComparedToCoerced == 0;
            boolean coercedValueIsLessThanOriginal = originalComparedToCoerced > 0;
            boolean coercedValueIsGreaterThanOriginal = originalComparedToCoerced < 0;
            Expression coercedLiteral = literalEncoder.toExpression(coercedValue, symbolExpressionType);

            switch (comparisonOperator) {
                case GREATER_THAN_OR_EQUAL:
                case GREATER_THAN: {
                    if (coercedValueIsGreaterThanOriginal) {
                        return new ComparisonExpression(GREATER_THAN_OR_EQUAL, symbolExpression, coercedLiteral);
                    }
                    if (coercedValueIsEqualToOriginal) {
                        return new ComparisonExpression(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    return new ComparisonExpression(GREATER_THAN, symbolExpression, coercedLiteral);
                }
                case LESS_THAN_OR_EQUAL:
                case LESS_THAN: {
                    if (coercedValueIsLessThanOriginal) {
                        return new ComparisonExpression(LESS_THAN_OR_EQUAL, symbolExpression, coercedLiteral);
                    }
                    if (coercedValueIsEqualToOriginal) {
                        return new ComparisonExpression(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    return new ComparisonExpression(LESS_THAN, symbolExpression, coercedLiteral);
                }
                case EQUAL: {
                    if (coercedValueIsEqualToOriginal) {
                        return new ComparisonExpression(EQUAL, symbolExpression, coercedLiteral);
                    }
                    // Return something that is false for all non-null values
                    return and(new ComparisonExpression(EQUAL, symbolExpression, coercedLiteral),
                            new ComparisonExpression(NOT_EQUAL, symbolExpression, coercedLiteral));
                }
                case NOT_EQUAL: {
                    if (coercedValueIsEqualToOriginal) {
                        return new ComparisonExpression(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    // Return something that is true for all non-null values
                    return or(new ComparisonExpression(EQUAL, symbolExpression, coercedLiteral),
                            new ComparisonExpression(NOT_EQUAL, symbolExpression, coercedLiteral));
                }
                case IS_DISTINCT_FROM: {
                    if (coercedValueIsEqualToOriginal) {
                        return new ComparisonExpression(comparisonOperator, symbolExpression, coercedLiteral);
                    }
                    return TRUE_LITERAL;
                }
            }

            throw new IllegalArgumentException("Unhandled operator: " + comparisonOperator);
        }

        private Optional<Object> floorValue(Type fromType, Type toType, Object value)
        {
            return getSaturatedFloorCastOperator(fromType, toType)
                    .map((operator) -> functionInvoker.invoke(operator, session.getSqlFunctionProperties(), value));
        }

        private Optional<FunctionHandle> getSaturatedFloorCastOperator(Type fromType, Type toType)
        {
            try {
                return Optional.of(metadata.getFunctionAndTypeManager().lookupCast(SATURATED_FLOOR_CAST, fromType.getTypeSignature(), toType.getTypeSignature()));
            }
            catch (OperatorNotFoundException e) {
                return Optional.empty();
            }
        }

        private int compareOriginalValueToCoerced(Type originalValueType, Object originalValue, Type coercedValueType, Object coercedValue)
        {
            FunctionHandle castToOriginalTypeOperator = metadata.getFunctionAndTypeManager().lookupCast(CAST, coercedValueType.getTypeSignature(), originalValueType.getTypeSignature());
            Object coercedValueInOriginalType = functionInvoker.invoke(castToOriginalTypeOperator, session.getSqlFunctionProperties(), coercedValue);
            Block originalValueBlock = Utils.nativeValueToBlock(originalValueType, originalValue);
            Block coercedValueBlock = Utils.nativeValueToBlock(originalValueType, coercedValueInOriginalType);
            return originalValueType.compareTo(originalValueBlock, 0, coercedValueBlock, 0);
        }

        @Override
        protected ExtractionResult visitInPredicate(InPredicate node, Boolean complement)
        {
            if (!(node.getValueList() instanceof InListExpression)) {
                return super.visitInPredicate(node, complement);
            }

            InListExpression valueList = (InListExpression) node.getValueList();
            checkState(!valueList.getValues().isEmpty(), "InListExpression should never be empty");

            ImmutableList.Builder<Expression> disjuncts = ImmutableList.builder();
            for (Expression expression : valueList.getValues()) {
                disjuncts.add(new ComparisonExpression(EQUAL, node.getValue(), expression));
            }
            ExtractionResult extractionResult = process(or(disjuncts.build()), complement);

            // preserve original IN predicate as remaining predicate
            if (extractionResult.tupleDomain.isAll()) {
                Expression originalPredicate = node;
                if (complement) {
                    originalPredicate = new NotExpression(originalPredicate);
                }
                return new ExtractionResult(extractionResult.tupleDomain, originalPredicate);
            }
            return extractionResult;
        }

        @Override
        protected ExtractionResult visitBetweenPredicate(BetweenPredicate node, Boolean complement)
        {
            // Re-write as two comparison expressions
            return process(and(
                    new ComparisonExpression(GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin()),
                    new ComparisonExpression(LESS_THAN_OR_EQUAL, node.getValue(), node.getMax())), complement);
        }

        @Override
        protected ExtractionResult visitIsNullPredicate(IsNullPredicate node, Boolean complement)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                return super.visitIsNullPredicate(node, complement);
            }

            Type columnType = checkedTypeLookup(node.getValue());
            Domain domain = complementIfNecessary(Domain.onlyNull(columnType), complement);
            return new ExtractionResult(
                    TupleDomain.withColumnDomains(ImmutableMap.of(((SymbolReference) node.getValue()).getName(), domain)),
                    TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitIsNotNullPredicate(IsNotNullPredicate node, Boolean complement)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                return super.visitIsNotNullPredicate(node, complement);
            }

            Type columnType = checkedTypeLookup(node.getValue());

            Domain domain = complementIfNecessary(Domain.notNull(columnType), complement);
            return new ExtractionResult(
                    TupleDomain.withColumnDomains(ImmutableMap.of(((SymbolReference) node.getValue()).getName(), domain)),
                    TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitBooleanLiteral(BooleanLiteral node, Boolean complement)
        {
            boolean value = complement ? !node.getValue() : node.getValue();
            return new ExtractionResult(value ? TupleDomain.all() : TupleDomain.none(), TRUE_LITERAL);
        }

        @Override
        protected ExtractionResult visitNullLiteral(NullLiteral node, Boolean complement)
        {
            return new ExtractionResult(TupleDomain.none(), TRUE_LITERAL);
        }
    }

    private static Type typeOf(Expression expression, Session session, Metadata metadata, TypeProvider types)
    {
        Map<NodeRef<Expression>, Type> expressionTypes = ExpressionAnalyzer.getExpressionTypes(session, metadata, new SqlParser(), types, expression, emptyList(), WarningCollector.NOOP);
        return expressionTypes.get(NodeRef.of(expression));
    }

    private static class NormalizedSimpleComparison
    {
        private final Expression symbolExpression;
        private final ComparisonExpression.Operator comparisonOperator;
        private final NullableValue value;

        public NormalizedSimpleComparison(Expression symbolExpression, ComparisonExpression.Operator comparisonOperator, NullableValue value)
        {
            this.symbolExpression = requireNonNull(symbolExpression, "nameReference is null");
            this.comparisonOperator = requireNonNull(comparisonOperator, "comparisonOperator is null");
            this.value = requireNonNull(value, "value is null");
        }

        public Expression getSymbolExpression()
        {
            return symbolExpression;
        }

        public ComparisonExpression.Operator getComparisonOperator()
        {
            return comparisonOperator;
        }

        public NullableValue getValue()
        {
            return value;
        }
    }

    public static class ExtractionResult
    {
        private final TupleDomain<String> tupleDomain;
        private final Expression remainingExpression;

        public ExtractionResult(TupleDomain<String> tupleDomain, Expression remainingExpression)
        {
            this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
            this.remainingExpression = requireNonNull(remainingExpression, "remainingExpression is null");
        }

        public TupleDomain<String> getTupleDomain()
        {
            return tupleDomain;
        }

        public Expression getRemainingExpression()
        {
            return remainingExpression;
        }
    }
}
