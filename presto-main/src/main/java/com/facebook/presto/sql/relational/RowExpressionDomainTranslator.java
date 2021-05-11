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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.OperatorType;
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
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression.Form;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.InterpretedFunctionInvoker;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.PeekingIterator;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.or;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.CastType.SATURATED_FLOOR_CAST;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IN;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.LiteralEncoder.toRowExpression;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterators.peekingIterator;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

public final class RowExpressionDomainTranslator
        implements DomainTranslator
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private final LogicalRowExpressions logicalRowExpressions;
    private final StandardFunctionResolution functionResolution;
    private final Metadata metadata;

    @Inject
    public RowExpressionDomainTranslator(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
        this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(functionAndTypeManager), new FunctionResolution(functionAndTypeManager), functionAndTypeManager);
        this.functionResolution = new FunctionResolution(functionAndTypeManager);
    }

    @Override
    public <T extends RowExpression> RowExpression toPredicate(TupleDomain<T> tupleDomain)
    {
        if (tupleDomain.isNone()) {
            return FALSE_CONSTANT;
        }

        Map<T, Domain> domains = tupleDomain.getDomains().get();
        return domains.entrySet().stream()
                .map(entry -> toPredicate(entry.getValue(), entry.getKey()))
                .collect(collectingAndThen(toImmutableList(), logicalRowExpressions::combineConjuncts));
    }

    public ExtractionResult<VariableReferenceExpression> fromPredicate(ConnectorSession session, RowExpression predicate)
    {
        return fromPredicate(session, predicate, BASIC_COLUMN_EXTRACTOR);
    }

    @Override
    public <T> ExtractionResult<T> fromPredicate(ConnectorSession session, RowExpression predicate, ColumnExtractor<T> columnExtractor)
    {
        return predicate.accept(new Visitor<>(metadata, session, columnExtractor), false);
    }

    private RowExpression toPredicate(Domain domain, RowExpression reference)
    {
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? isNull(reference) : FALSE_CONSTANT;
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? TRUE_CONSTANT : not(functionResolution, isNull(reference));
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

        return logicalRowExpressions.combineDisjunctsWithDefault(disjuncts, TRUE_CONSTANT);
    }

    private RowExpression processRange(Type type, Range range, RowExpression reference)
    {
        if (range.isAll()) {
            return TRUE_CONSTANT;
        }

        if (isBetween(range)) {
            // specialize the range with BETWEEN expression if possible b/c it is currently more efficient
            return call(
                    BETWEEN.name(),
                    functionAndTypeManager.resolveOperator(BETWEEN, fromTypes(reference.getType(), type, type)),
                    BOOLEAN,
                    reference,
                    toRowExpression(range.getLowBoundedValue(), type),
                    toRowExpression(range.getHighBoundedValue(), type));
        }

        List<RowExpression> rangeConjuncts = new ArrayList<>();
        if (!range.isLowUnbounded()) {
            if (range.isLowInclusive()) {
                rangeConjuncts.add(greaterThanOrEqual(reference, toRowExpression(range.getLowBoundedValue(), type)));
            }
            else {
                rangeConjuncts.add(greaterThan(reference, toRowExpression(range.getLowBoundedValue(), type)));
            }
        }
        if (!range.isHighUnbounded()) {
            if (range.isHighInclusive()) {
                rangeConjuncts.add(lessThanOrEqual(reference, toRowExpression(range.getHighBoundedValue(), type)));
            }
            else {
                rangeConjuncts.add(lessThan(reference, toRowExpression(range.getHighBoundedValue(), type)));
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

    private static class Visitor<T>
            implements RowExpressionVisitor<ExtractionResult<T>, Boolean>
    {
        private final InterpretedFunctionInvoker functionInvoker;
        private final Metadata metadata;
        private final ConnectorSession session;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final LogicalRowExpressions logicalRowExpressions;
        private final DeterminismEvaluator determinismEvaluator;
        private final StandardFunctionResolution resolution;
        private final ColumnExtractor<T> columnExtractor;

        private Visitor(Metadata metadata, ConnectorSession session, ColumnExtractor<T> columnExtractor)
        {
            this.functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionAndTypeManager());
            this.metadata = metadata;
            this.session = session;
            this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
            this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(functionAndTypeManager), new FunctionResolution(functionAndTypeManager), functionAndTypeManager);
            this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
            this.resolution = new FunctionResolution(functionAndTypeManager);
            this.columnExtractor = requireNonNull(columnExtractor, "columnExtractor is null");
        }

        @Override
        public ExtractionResult<T> visitSpecialForm(SpecialFormExpression node, Boolean complement)
        {
            switch (node.getForm()) {
                case AND:
                case OR: {
                    return visitBinaryLogic(node, complement);
                }
                case IN: {
                    RowExpression target = node.getArguments().get(0);
                    List<RowExpression> values = node.getArguments().subList(1, node.getArguments().size());
                    checkState(!values.isEmpty(), "values should never be empty");

                    ImmutableList.Builder<RowExpression> disjuncts = ImmutableList.builder();
                    for (RowExpression expression : values) {
                        disjuncts.add(call(EQUAL.name(), functionAndTypeManager.resolveOperator(EQUAL, fromTypes(target.getType(), expression.getType())), BOOLEAN, target, expression));
                    }
                    ExtractionResult extractionResult = or(disjuncts.build()).accept(this, complement);

                    // preserve original IN predicate as remaining predicate
                    if (extractionResult.getTupleDomain().isAll()) {
                        RowExpression originalPredicate = node;
                        if (complement) {
                            originalPredicate = not(resolution, originalPredicate);
                        }
                        return new ExtractionResult<>(extractionResult.getTupleDomain(), originalPredicate);
                    }
                    return extractionResult;
                }
                case IS_NULL: {
                    RowExpression value = node.getArguments().get(0);
                    Domain domain = complementIfNecessary(Domain.onlyNull(value.getType()), complement);
                    Optional<T> column = columnExtractor.extract(value, domain);
                    if (!column.isPresent()) {
                        return visitRowExpression(node, complement);
                    }

                    return new ExtractionResult<>(TupleDomain.withColumnDomains(ImmutableMap.of(column.get(), domain)), TRUE_CONSTANT);
                }
                default:
                    return visitRowExpression(node, complement);
            }
        }

        @Override
        public ExtractionResult<T> visitConstant(ConstantExpression node, Boolean complement)
        {
            if (node.getValue() == null) {
                return new ExtractionResult<>(TupleDomain.none(), TRUE_CONSTANT);
            }
            if (node.getType() == BOOLEAN) {
                boolean value = complement != (boolean) node.getValue();
                return new ExtractionResult<>(value ? TupleDomain.all() : TupleDomain.none(), TRUE_CONSTANT);
            }
            throw new IllegalStateException("Can not extract predicate from constant type: " + node.getType());
        }

        @Override
        public ExtractionResult<T> visitLambda(LambdaDefinitionExpression node, Boolean complement)
        {
            return visitRowExpression(node, complement);
        }

        @Override
        public ExtractionResult<T> visitVariableReference(VariableReferenceExpression node, Boolean complement)
        {
            if (node.getType() == BOOLEAN) {
                Domain domain = createComparisonDomain(EQUAL, BOOLEAN, !complement, Boolean.FALSE);
                Optional<T> column = columnExtractor.extract(node, domain);
                return new ExtractionResult<>(TupleDomain.withColumnDomains(ImmutableMap.of(column.get(), domain)), TRUE_CONSTANT);
            }
            return visitRowExpression(node, complement);
        }

        @Override
        public ExtractionResult<T> visitCall(CallExpression node, Boolean complement)
        {
            if (node.getFunctionHandle().equals(resolution.notFunction())) {
                return node.getArguments().get(0).accept(this, !complement);
            }

            if (resolution.isBetweenFunction(node.getFunctionHandle())) {
                // Re-write as two comparison expressions
                return and(
                        binaryOperator(GREATER_THAN_OR_EQUAL, node.getArguments().get(0), node.getArguments().get(1)),
                        binaryOperator(LESS_THAN_OR_EQUAL, node.getArguments().get(0), node.getArguments().get(2))).accept(this, complement);
            }

            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(node.getFunctionHandle());
            if (functionMetadata.getOperatorType().map(OperatorType::isComparisonOperator).orElse(false)) {
                Optional<NormalizedSimpleComparison> optionalNormalized = toNormalizedSimpleComparison(functionMetadata.getOperatorType().get(), node.getArguments().get(0), node.getArguments().get(1));
                if (!optionalNormalized.isPresent()) {
                    return visitRowExpression(node, complement);
                }
                NormalizedSimpleComparison normalized = optionalNormalized.get();

                RowExpression expression = normalized.getExpression();
                NullableValue value = normalized.getValue();
                Domain domain = createComparisonDomain(normalized.getComparisonOperator(), value.getType(), value.getValue(), complement);
                Optional<T> column = columnExtractor.extract(expression, domain);
                if (column.isPresent()) {
                    if (domain.isNone()) {
                        return new ExtractionResult<>(TupleDomain.none(), TRUE_CONSTANT);
                    }
                    return new ExtractionResult<>(TupleDomain.withColumnDomains(ImmutableMap.of(column.get(), domain)), TRUE_CONSTANT);
                }

                if (expression instanceof CallExpression && resolution.isCastFunction(((CallExpression) expression).getFunctionHandle())) {
                    CallExpression castExpression = (CallExpression) expression;
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
                        return visitRowExpression(node, complement);
                    }

                    CallExpression cast = (CallExpression) expression;
                    Type sourceType = cast.getArguments().get(0).getType();

                    // we use saturated floor cast value -> castSourceType to rewrite original expression to new one with one cast peeled off the symbol side
                    Optional<RowExpression> coercedExpression = coerceComparisonWithRounding(
                            sourceType, cast.getArguments().get(0), normalized.getValue(), normalized.getComparisonOperator());

                    if (coercedExpression.isPresent()) {
                        return coercedExpression.get().accept(this, complement);
                    }

                    return visitRowExpression(node, complement);
                }
                else {
                    return visitRowExpression(node, complement);
                }
            }

            return visitRowExpression(node, complement);
        }

        @Override
        public ExtractionResult<T> visitInputReference(InputReferenceExpression node, Boolean complement)
        {
            return visitRowExpression(node, complement);
        }

        private Optional<RowExpression> coerceComparisonWithRounding(
                Type expressionType,
                RowExpression expression,
                NullableValue nullableValue,
                OperatorType comparisonOperator)
        {
            requireNonNull(nullableValue, "nullableValue is null");
            if (nullableValue.isNull()) {
                return Optional.empty();
            }
            Type valueType = nullableValue.getType();
            Object value = nullableValue.getValue();
            return floorValue(valueType, expressionType, value)
                    .map((floorValue) -> rewriteComparisonExpression(expressionType, expression, valueType, value, floorValue, comparisonOperator));
        }

        private RowExpression rewriteComparisonExpression(
                Type expressionType,
                RowExpression expression,
                Type valueType,
                Object originalValue,
                Object coercedValue,
                OperatorType comparisonOperator)
        {
            int originalComparedToCoerced = compareOriginalValueToCoerced(valueType, originalValue, expressionType, coercedValue);
            boolean coercedValueIsEqualToOriginal = originalComparedToCoerced == 0;
            boolean coercedValueIsLessThanOriginal = originalComparedToCoerced > 0;
            boolean coercedValueIsGreaterThanOriginal = originalComparedToCoerced < 0;
            RowExpression coercedLiteral = toRowExpression(coercedValue, expressionType);

            switch (comparisonOperator) {
                case GREATER_THAN_OR_EQUAL:
                case GREATER_THAN: {
                    if (coercedValueIsGreaterThanOriginal) {
                        return binaryOperator(GREATER_THAN_OR_EQUAL, expression, coercedLiteral);
                    }
                    if (coercedValueIsEqualToOriginal) {
                        return binaryOperator(comparisonOperator, expression, coercedLiteral);
                    }
                    return binaryOperator(GREATER_THAN, expression, coercedLiteral);
                }
                case LESS_THAN_OR_EQUAL:
                case LESS_THAN: {
                    if (coercedValueIsLessThanOriginal) {
                        return binaryOperator(LESS_THAN_OR_EQUAL, expression, coercedLiteral);
                    }
                    if (coercedValueIsEqualToOriginal) {
                        return binaryOperator(comparisonOperator, expression, coercedLiteral);
                    }
                    return binaryOperator(LESS_THAN, expression, coercedLiteral);
                }
                case EQUAL: {
                    if (coercedValueIsEqualToOriginal) {
                        return binaryOperator(EQUAL, expression, coercedLiteral);
                    }
                    // Return something that is false for all non-null values
                    return and(binaryOperator(EQUAL, expression, coercedLiteral),
                            binaryOperator(NOT_EQUAL, expression, coercedLiteral));
                }
                case NOT_EQUAL: {
                    if (coercedValueIsEqualToOriginal) {
                        return binaryOperator(comparisonOperator, expression, coercedLiteral);
                    }
                    // Return something that is true for all non-null values
                    return or(binaryOperator(EQUAL, expression, coercedLiteral),
                            binaryOperator(NOT_EQUAL, expression, coercedLiteral));
                }
                case IS_DISTINCT_FROM: {
                    if (coercedValueIsEqualToOriginal) {
                        return binaryOperator(comparisonOperator, expression, coercedLiteral);
                    }
                    return TRUE_CONSTANT;
                }
            }

            throw new IllegalArgumentException("Unhandled operator: " + comparisonOperator);
        }

        private RowExpression binaryOperator(OperatorType operatorType, RowExpression left, RowExpression right)
        {
            return call(
                    operatorType.name(),
                    metadata.getFunctionAndTypeManager().resolveOperator(operatorType, fromTypes(left.getType(), right.getType())),
                    BOOLEAN,
                    left,
                    right);
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

        private boolean isImplicitCoercion(CallExpression cast)
        {
            Type sourceType = cast.getArguments().get(0).getType();
            Type targetType = cast.getType();
            return metadata.getFunctionAndTypeManager().canCoerce(sourceType, targetType);
        }

        private static Domain extractOrderableDomain(OperatorType comparisonOperator, Type type, Object value, boolean complement)
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

        private static Domain extractEquatableDomain(OperatorType comparisonOperator, Type type, Object value, boolean complement)
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

        /**
         * Extract a normalized simple comparison between a QualifiedNameReference and a native value if possible.
         */
        private Optional<NormalizedSimpleComparison> toNormalizedSimpleComparison(OperatorType operatorType, RowExpression leftExpression, RowExpression rightExpression)
        {
            Object left;
            Object right;
            if (leftExpression instanceof VariableReferenceExpression) {
                left = leftExpression;
            }
            else {
                left = new RowExpressionInterpreter(leftExpression, metadata, session, OPTIMIZED).optimize();
            }
            if (rightExpression instanceof VariableReferenceExpression) {
                right = rightExpression;
            }
            else {
                right = new RowExpressionInterpreter(rightExpression, metadata, session, OPTIMIZED).optimize();
            }

            if (left instanceof RowExpression == right instanceof RowExpression) {
                // we expect one side to be row expression and other to be value.
                return Optional.empty();
            }

            RowExpression expression;
            OperatorType comparisonOperator;
            NullableValue value;

            if (left instanceof RowExpression) {
                expression = leftExpression;
                comparisonOperator = operatorType;
                value = new NullableValue(rightExpression.getType(), right);
            }
            else {
                expression = rightExpression;
                comparisonOperator = flip(operatorType);
                value = new NullableValue(leftExpression.getType(), left);
            }

            return Optional.of(new NormalizedSimpleComparison(expression, comparisonOperator, value));
        }

        private static Domain createComparisonDomain(OperatorType comparisonOperator, Type type, @Nullable Object value, boolean complement)
        {
            if (value == null) {
                switch (comparisonOperator) {
                    case EQUAL:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                    case NOT_EQUAL:
                        return Domain.none(type);

                    case IS_DISTINCT_FROM:
                        return complementIfNecessary(Domain.notNull(type), complement);

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

            return domain;
        }

        private static OperatorType flip(OperatorType operatorType)
        {
            switch (operatorType) {
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
                    throw new IllegalArgumentException("Unsupported comparison: " + operatorType);
            }
        }

        private static ValueSet complementIfNecessary(ValueSet valueSet, boolean complement)
        {
            return complement ? valueSet.complement() : valueSet;
        }

        private static Domain complementIfNecessary(Domain domain, boolean complement)
        {
            return complement ? domain.complement() : domain;
        }

        private RowExpression complementIfNecessary(RowExpression expression, boolean complement)
        {
            return complement ? not(resolution, expression) : expression;
        }

        private ExtractionResult<T> visitRowExpression(RowExpression node, Boolean complement)
        {
            // If we don't know how to process this node, the default response is to say that the TupleDomain is "all"
            return new ExtractionResult<>(TupleDomain.all(), complementIfNecessary(node, complement));
        }

        private ExtractionResult<T> visitBinaryLogic(SpecialFormExpression node, Boolean complement)
        {
            ExtractionResult<T> leftResult = node.getArguments().get(0).accept(this, complement);
            ExtractionResult<T> rightResult = node.getArguments().get(1).accept(this, complement);

            TupleDomain<T> leftTupleDomain = leftResult.getTupleDomain();
            TupleDomain<T> rightTupleDomain = rightResult.getTupleDomain();

            Form operator = node.getForm();
            if (complement) {
                if (operator == AND) {
                    operator = OR;
                }
                else if (operator == OR) {
                    operator = AND;
                }
                else {
                    throw new IllegalStateException("Can not extract predicate from special form: " + node.getForm());
                }
            }

            switch (operator) {
                case AND: {
                    return new ExtractionResult<>(
                            leftTupleDomain.intersect(rightTupleDomain),
                            logicalRowExpressions.combineConjuncts(leftResult.getRemainingExpression(), rightResult.getRemainingExpression()));
                }
                case OR: {
                    TupleDomain<T> columnUnionedTupleDomain = TupleDomain.columnWiseUnion(leftTupleDomain, rightTupleDomain);

                    // In most cases, the columnUnionedTupleDomain is only a superset of the actual strict union
                    // and so we can return the current node as the remainingExpression so that all bounds will be double checked again at execution time.
                    RowExpression remainingExpression = complementIfNecessary(node, complement);

                    // However, there are a few cases where the column-wise union is actually equivalent to the strict union, so we if can detect
                    // some of these cases, we won't have to double check the bounds unnecessarily at execution time.

                    // We can only make inferences if the remaining expressions on both side are equal and deterministic
                    if (leftResult.getRemainingExpression().equals(rightResult.getRemainingExpression()) &&
                            determinismEvaluator.isDeterministic(leftResult.getRemainingExpression())) {
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

                    return new ExtractionResult<>(columnUnionedTupleDomain, remainingExpression);
                }
                default:
                    throw new IllegalStateException("Can not extract predicate from special form: " + node.getForm());
            }
        }
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
        return call(operatorType.name(), functionAndTypeManager.resolveOperator(operatorType, fromTypes(left.getType(), right.getType())), BOOLEAN, left, right);
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

    private static class NormalizedSimpleComparison
    {
        private final RowExpression expression;
        private final OperatorType comparisonOperator;
        private final NullableValue value;

        public NormalizedSimpleComparison(RowExpression expression, OperatorType comparisonOperator, NullableValue value)
        {
            this.expression = requireNonNull(expression, "expression is null");
            this.comparisonOperator = requireNonNull(comparisonOperator, "comparisonOperator is null");
            this.value = requireNonNull(value, "value is null");
        }

        public RowExpression getExpression()
        {
            return expression;
        }

        public OperatorType getComparisonOperator()
        {
            return comparisonOperator;
        }

        public NullableValue getValue()
        {
            return value;
        }
    }
}
