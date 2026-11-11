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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.NoOpVariableResolver;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
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
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.facebook.presto.SystemSessionProperties.shouldOptimizerUseHistograms;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.cost.StatsUtil.toStatsRepresentation;
import static com.facebook.presto.expressions.DynamicFilters.isDynamicFilter;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Double.NaN;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Double.max;
import static java.lang.Double.min;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;

public class FilterStatsCalculator
{
    /**
     *
     * This value applies a filter factor to upper-bound the size of the variable range selected for an IN predicate
     * Since the estimator sums up the individual estimates, we don't want to go beyond 1.0
     * This also impacts NOT IN similarly, we never apply a filter factor of 0.0 for a NOT IN clause
     */
    static final double CEIL_IN_PREDICATE_UPPER_BOUND_COEFFICIENT = 0.8;
    static final double UNKNOWN_FILTER_COEFFICIENT = 0.9;
    private static final int MAX_PINNED_BRANCHES = 10_000;
    private static final Pattern BOUNDED_VARCHAR = Pattern.compile("varchar\\((\\d+)\\)");

    private final Metadata metadata;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final StatsNormalizer normalizer;
    private final LiteralEncoder literalEncoder;
    private final FunctionResolution functionResolution;

    @Inject
    public FilterStatsCalculator(Metadata metadata, ScalarStatsCalculator scalarStatsCalculator, StatsNormalizer normalizer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
        this.normalizer = requireNonNull(normalizer, "normalizer is null");
        this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
    }

    @Deprecated
    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            Expression predicate,
            Session session,
            TypeProvider types)
    {
        Expression simplifiedExpression = simplifyExpression(session, predicate, types);
        return new FilterExpressionStatsCalculatingVisitor(statsEstimate, session, types)
                .process(simplifiedExpression);
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            RowExpression predicate,
            ConnectorSession session,
            /* TODO: this session parameter is optional because this method can be called from the
            ConnectorFilterStatsCalculatorService which only has access to a ConnectorSession
            object. When the ConnectorSession API allows access to the underlying session, this
            method should be updated to just accept a ConnectorSession.
            */
            Session systemSession)
    {
        RowExpression simplifiedExpression = simplifyExpression(session, predicate);
        return new FilterRowExpressionStatsCalculatingVisitor(statsEstimate, session, metadata.getFunctionAndTypeManager(), systemSession).process(simplifiedExpression);
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            RowExpression predicate,
            Session session)
    {
        return filterStats(statsEstimate, predicate, session.toConnectorSession(), session);
    }

    private Expression simplifyExpression(Session session, Expression predicate, TypeProvider types)
    {
        // TODO reuse com.facebook.presto.sql.planner.iterative.rule.SimplifyExpressions.rewrite

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, predicate, types);
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(predicate, metadata, session, expressionTypes);
        Object value = interpreter.optimize(NoOpVariableResolver.INSTANCE);

        if (value == null) {
            // Expression evaluates to SQL null, which in Filter is equivalent to false. This assumes the expression is a top-level expression (eg. not in NOT).
            value = false;
        }
        return literalEncoder.toExpression(value, BOOLEAN);
    }

    /**
     * Whether two OR branches can never both be true because every branch of one pins a string column to
     * literal values the other branch rules out, as {@code name = 'a'} and {@code name = 'b'} do. A string
     * column has no value range, so applying one branch to the other's output cannot see the contradiction:
     * {@code name = 'a' AND name = 'b'} looks as large as {@code name = 'a'}, and an OR of many such branches
     * collapses to the size of one. Each argument lists a predicate's OR branches, each as the literal values
     * it requires of the columns it pins.
     */
    private static boolean areDisjoint(List<Map<VariableReferenceExpression, Set<Object>>> left, List<Map<VariableReferenceExpression, Set<Object>>> right)
    {
        for (Map<VariableReferenceExpression, Set<Object>> leftBranch : left) {
            for (Map<VariableReferenceExpression, Set<Object>> rightBranch : right) {
                boolean contradicts = leftBranch.entrySet().stream()
                        .anyMatch(pin -> rightBranch.containsKey(pin.getKey()) && Collections.disjoint(pin.getValue(), rightBranch.get(pin.getKey())));
                if (!contradicts) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * A predicate's OR branches as pins. A predicate that pins nothing, or has too many branches to track, is
     * one branch that pins nothing.
     */
    private static List<Map<VariableReferenceExpression, Set<Object>>> branches(Optional<List<Map<VariableReferenceExpression, Set<Object>>>> pins)
    {
        return pins.orElse(ImmutableList.of(ImmutableMap.of()));
    }

    private static Optional<List<Map<VariableReferenceExpression, Set<Object>>>> andPins(
            Optional<List<Map<VariableReferenceExpression, Set<Object>>>> left,
            Optional<List<Map<VariableReferenceExpression, Set<Object>>>> right)
    {
        if (!left.isPresent()) {
            return right;
        }
        if (!right.isPresent()) {
            return left;
        }
        if ((long) left.get().size() * right.get().size() > MAX_PINNED_BRANCHES) {
            return Optional.empty();
        }
        ImmutableList.Builder<Map<VariableReferenceExpression, Set<Object>>> result = ImmutableList.builder();
        for (Map<VariableReferenceExpression, Set<Object>> leftBranch : left.get()) {
            for (Map<VariableReferenceExpression, Set<Object>> rightBranch : right.get()) {
                Map<VariableReferenceExpression, Set<Object>> combined = new HashMap<>(leftBranch);
                rightBranch.forEach((variable, values) -> combined.merge(variable, values, (a, b) -> a.stream().filter(b::contains).collect(toImmutableSet())));
                result.add(combined);
            }
        }
        return Optional.of(result.build());
    }

    private static Optional<List<Map<VariableReferenceExpression, Set<Object>>>> orPins(
            Optional<List<Map<VariableReferenceExpression, Set<Object>>>> left,
            Optional<List<Map<VariableReferenceExpression, Set<Object>>>> right)
    {
        if (!left.isPresent() || !right.isPresent() || left.get().size() + right.get().size() > MAX_PINNED_BRANCHES) {
            return Optional.empty();
        }
        return Optional.of(ImmutableList.<Map<VariableReferenceExpression, Set<Object>>>builder().addAll(left.get()).addAll(right.get()).build());
    }

    /**
     * Only string columns are pinned: a column with a known value range is handled by the range math.
     */
    private static Optional<List<Map<VariableReferenceExpression, Set<Object>>>> pin(VariableReferenceExpression variable, Set<Object> values)
    {
        if (!(variable.getType() instanceof VarcharType) || values.contains(null)) {
            return Optional.empty();
        }
        return Optional.of(ImmutableList.of(ImmutableMap.of(variable, values)));
    }

    /**
     * The value of a string literal, as {@link LiteralEncoder} writes one into an expression: a plain literal,
     * or one cast to a bounded varchar at least as long as the literal.
     */
    private static Optional<Object> stringValue(Expression expression)
    {
        if (expression instanceof StringLiteral) {
            return Optional.of(utf8Slice(((StringLiteral) expression).getValue()));
        }
        if (expression instanceof Cast && ((Cast) expression).getExpression() instanceof StringLiteral) {
            String literal = ((StringLiteral) ((Cast) expression).getExpression()).getValue();
            String type = ((Cast) expression).getType().toLowerCase(ENGLISH);
            Matcher bounded = BOUNDED_VARCHAR.matcher(type);
            if (type.equals("varchar") || (bounded.matches() && Integer.parseInt(bounded.group(1)) >= literal.codePointCount(0, literal.length()))) {
                return Optional.of(utf8Slice(literal));
            }
        }
        return Optional.empty();
    }

    private RowExpression simplifyExpression(ConnectorSession session, RowExpression predicate)
    {
        RowExpressionInterpreter interpreter = new RowExpressionInterpreter(predicate, metadata.getFunctionAndTypeManager(), session, OPTIMIZED);
        Object value = interpreter.optimize();

        if (value == null) {
            // Expression evaluates to SQL null, which in Filter is equivalent to false. This assumes the expression is a top-level expression (eg. not in NOT).
            value = false;
        }
        return LiteralEncoder.toRowExpression(predicate.getSourceLocation(), value, BOOLEAN);
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Session session, Expression expression, TypeProvider types)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(),
                session,
                types,
                emptyMap(),
                node -> new IllegalStateException("Unexpected node: " + node),
                WarningCollector.NOOP,
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }

    private class FilterExpressionStatsCalculatingVisitor
            extends AstVisitor<PlanNodeStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final TypeProvider types;
        private final PlanNodeStatsEstimateMath calculator;
        private final ComparisonStatsCalculator comparisonCalculator;

        FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, Session session, TypeProvider types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
            this.calculator = new PlanNodeStatsEstimateMath(shouldOptimizerUseHistograms(session));
            this.comparisonCalculator = new ComparisonStatsCalculator(session);
        }

        @Override
        public PlanNodeStatsEstimate process(Node node, @Nullable Void context)
        {
            return normalizer.normalize(super.process(node, context));
        }

        @Override
        protected PlanNodeStatsEstimate visitExpression(Expression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitNotExpression(NotExpression node, Void context)
        {
            if (node.getValue() instanceof IsNullPredicate) {
                return process(new IsNotNullPredicate(((IsNullPredicate) node.getValue()).getValue()));
            }
            return calculator.subtractSubsetStats(input, process(node.getValue()));
        }

        @Override
        protected PlanNodeStatsEstimate visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            switch (node.getOperator()) {
                case AND:
                    return estimateLogicalAnd(node.getLeft(), node.getRight());
                case OR:
                    return estimateLogicalOr(node.getLeft(), node.getRight());
                default:
                    throw new IllegalArgumentException("Unexpected binary operator: " + node.getOperator());
            }
        }

        private PlanNodeStatsEstimate estimateLogicalAnd(Expression left, Expression right)
        {
            // first try to estimate in the fair way
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (!leftEstimate.isOutputRowCountUnknown()) {
                PlanNodeStatsEstimate logicalAndEstimate = new FilterExpressionStatsCalculatingVisitor(leftEstimate, session, types).process(right);
                if (!logicalAndEstimate.isOutputRowCountUnknown()) {
                    return logicalAndEstimate;
                }
            }

            // If some of the filters cannot be estimated, take the smallest estimate.
            // Apply 0.9 filter factor as "unknown filter" factor.
            PlanNodeStatsEstimate rightEstimate = process(right);
            PlanNodeStatsEstimate smallestKnownEstimate;
            if (leftEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = rightEstimate;
            }
            else if (rightEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = leftEstimate;
            }
            else {
                smallestKnownEstimate = leftEstimate.getOutputRowCount() <= rightEstimate.getOutputRowCount() ? leftEstimate : rightEstimate;
            }
            if (smallestKnownEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }
            return smallestKnownEstimate.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT);
        }

        private PlanNodeStatsEstimate estimateLogicalOr(Expression left, Expression right)
        {
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (leftEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate rightEstimate = process(right);
            if (rightEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            if (areDisjoint(branches(pins(left)), branches(pins(right)))) {
                return calculator.capStats(calculator.addStatsAndSumDistinctValues(leftEstimate, rightEstimate), input);
            }

            PlanNodeStatsEstimate andEstimate = new FilterExpressionStatsCalculatingVisitor(leftEstimate, session, types).process(right);
            if (andEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            return calculator.capStats(
                    calculator.subtractSubsetStats(
                            calculator.addStatsAndSumDistinctValues(leftEstimate, rightEstimate),
                            andEstimate),
                    input);
        }

        private Optional<List<Map<VariableReferenceExpression, Set<Object>>>> pins(Expression expression)
        {
            if (expression instanceof LogicalBinaryExpression) {
                LogicalBinaryExpression logical = (LogicalBinaryExpression) expression;
                Optional<List<Map<VariableReferenceExpression, Set<Object>>>> left = pins(logical.getLeft());
                Optional<List<Map<VariableReferenceExpression, Set<Object>>>> right = pins(logical.getRight());
                return logical.getOperator() == LogicalBinaryExpression.Operator.AND ? andPins(left, right) : orPins(left, right);
            }
            if (expression instanceof ComparisonExpression && ((ComparisonExpression) expression).getOperator() == EQUAL) {
                ComparisonExpression comparison = (ComparisonExpression) expression;
                Expression column = comparison.getLeft() instanceof SymbolReference ? comparison.getLeft() : comparison.getRight();
                Expression literal = column == comparison.getLeft() ? comparison.getRight() : comparison.getLeft();
                Optional<Object> value = stringValue(literal);
                if (column instanceof SymbolReference && value.isPresent()) {
                    return pin(toVariable(column), ImmutableSet.of(value.get()));
                }
            }
            if (expression instanceof InPredicate && ((InPredicate) expression).getValue() instanceof SymbolReference
                    && ((InPredicate) expression).getValueList() instanceof InListExpression) {
                InPredicate in = (InPredicate) expression;
                List<Optional<Object>> values = ((InListExpression) in.getValueList()).getValues().stream()
                        .map(FilterStatsCalculator::stringValue)
                        .collect(toImmutableList());
                if (values.stream().allMatch(Optional::isPresent)) {
                    return pin(toVariable(in.getValue()), values.stream().map(Optional::get).collect(toImmutableSet()));
                }
            }
            return Optional.empty();
        }

        @Override
        protected PlanNodeStatsEstimate visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            if (node.getValue()) {
                return input;
            }

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
            result.setOutputRowCount(0.0);
            input.getVariablesWithKnownStatistics().forEach(variable -> result.addVariableStatistics(variable, VariableStatsEstimate.zero()));
            return result.build();
        }

        @Override
        protected PlanNodeStatsEstimate visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                VariableReferenceExpression variable = toVariable(node.getValue());
                VariableStatsEstimate variableStats = input.getVariableStatistics(variable);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * (1 - variableStats.getNullsFraction()));
                result.addVariableStatistics(variable, variableStats.mapNullsFraction(x -> 0.0));
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                VariableReferenceExpression variable = toVariable(node.getValue());
                VariableStatsEstimate variableStats = input.getVariableStatistics(variable);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * variableStats.getNullsFraction());
                result.addVariableStatistics(variable, VariableStatsEstimate.builder()
                        .setNullsFraction(1.0)
                        .setLowValue(NaN)
                        .setHighValue(NaN)
                        .setDistinctValuesCount(0.0)
                        .build());
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                return PlanNodeStatsEstimate.unknown();
            }
            if (!getExpressionStats(node.getMin()).isSingleValue()) {
                return PlanNodeStatsEstimate.unknown();
            }
            if (!getExpressionStats(node.getMax()).isSingleValue()) {
                return PlanNodeStatsEstimate.unknown();
            }

            VariableStatsEstimate valueStats = input.getVariableStatistics(toVariable(node.getValue()));
            Expression lowerBound = new ComparisonExpression(GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin());
            Expression upperBound = new ComparisonExpression(LESS_THAN_OR_EQUAL, node.getValue(), node.getMax());

            Expression transformed;
            if (isInfinite(valueStats.getLowValue())) {
                // We want to do heuristic cut (infinite range to finite range) ASAP and then do filtering on finite range.
                // We rely on 'and()' being processed left to right
                transformed = and(lowerBound, upperBound);
            }
            else {
                transformed = and(upperBound, lowerBound);
            }
            return process(transformed);
        }

        @Override
        protected PlanNodeStatsEstimate visitInPredicate(InPredicate node, Void context)
        {
            if (!(node.getValueList() instanceof InListExpression)) {
                return PlanNodeStatsEstimate.unknown();
            }

            InListExpression inList = (InListExpression) node.getValueList();
            ImmutableList<PlanNodeStatsEstimate> equalityEstimates = inList.getValues().stream()
                    .map(inValue -> process(new ComparisonExpression(EQUAL, node.getValue(), inValue)))
                    .collect(toImmutableList());

            if (equalityEstimates.stream().anyMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown)) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate inEstimate = equalityEstimates.stream()
                    .reduce(calculator::addStatsAndSumDistinctValues)
                    .orElse(PlanNodeStatsEstimate.unknown());

            if (inEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            VariableStatsEstimate valueStats = getExpressionStats(node.getValue());
            if (valueStats.isUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - valueStats.getNullsFraction());
            double ceiledInEstimated = max(notNullValuesBeforeIn * CEIL_IN_PREDICATE_UPPER_BOUND_COEFFICIENT, 1.0);
            double inEstimateRowCount = min(inEstimate.getOutputRowCount(), ceiledInEstimated);

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
            result.setOutputRowCount(inEstimateRowCount);

            if (node.getValue() instanceof SymbolReference) {
                VariableReferenceExpression valueVariable = toVariable(node.getValue());
                VariableStatsEstimate newvariableStats = inEstimate.getVariableStatistics(valueVariable)
                        .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, valueStats.getDistinctValuesCount()));
                result.addVariableStatistics(valueVariable, newvariableStats);
            }
            return result.build();
        }

        @Override
        protected PlanNodeStatsEstimate visitComparisonExpression(ComparisonExpression node, Void context)
        {
            ComparisonExpression.Operator operator = node.getOperator();
            Expression left = node.getLeft();
            Expression right = node.getRight();

            checkArgument(!(left instanceof Literal && right instanceof Literal), "Literal-to-literal not supported here, should be eliminated earlier");

            if (!(left instanceof SymbolReference) && right instanceof SymbolReference) {
                // normalize so that symbol is on the left
                return process(new ComparisonExpression(operator.flip(), right, left));
            }

            if (left instanceof Literal && !(right instanceof Literal)) {
                // normalize so that literal is on the right
                return process(new ComparisonExpression(operator.flip(), right, left));
            }

            if (left instanceof SymbolReference && left.equals(right)) {
                return process(new IsNotNullPredicate(left));
            }

            VariableStatsEstimate leftStats = getExpressionStats(left);
            Optional<VariableReferenceExpression> leftVariable = left instanceof SymbolReference ? Optional.of(toVariable(left)) : Optional.empty();
            if (right instanceof Literal) {
                Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), right);
                if (literalValue == null) {
                    return visitBooleanLiteral(FALSE_LITERAL, null);
                }
                OptionalDouble literal = toStatsRepresentation(metadata, session, getType(left), literalValue);
                return comparisonCalculator.estimateExpressionToLiteralComparison(input, leftStats, leftVariable, literal, operator);
            }

            VariableStatsEstimate rightStats = getExpressionStats(right);
            if (rightStats.isSingleValue()) {
                OptionalDouble value = isNaN(rightStats.getLowValue()) ? OptionalDouble.empty() : OptionalDouble.of(rightStats.getLowValue());
                return comparisonCalculator.estimateExpressionToLiteralComparison(input, leftStats, leftVariable, value, operator);
            }

            Optional<VariableReferenceExpression> rightVariable = right instanceof SymbolReference ? Optional.of(toVariable(right)) : Optional.empty();
            return comparisonCalculator.estimateExpressionToExpressionComparison(input, leftStats, leftVariable, rightStats, rightVariable, operator);
        }

        private Type getType(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                return requireNonNull(types.get(expression), () -> format("No type for expression %s", expression));
            }

            ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                    metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(),
                    session,
                    types,
                    ImmutableMap.of(),
                    // At this stage, there should be no subqueries in the plan.
                    node -> new VerifyException("Unexpected subquery"),
                    WarningCollector.NOOP,
                    false);
            return expressionAnalyzer.analyze(expression, Scope.create());
        }

        private VariableStatsEstimate getExpressionStats(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                VariableReferenceExpression variable = toVariable(expression);
                return requireNonNull(input.getVariableStatistics(variable), () -> format("No statistics for variable %s", variable));
            }
            return scalarStatsCalculator.calculate(expression, input, session, types);
        }

        private VariableReferenceExpression toVariable(Expression expression)
        {
            checkArgument(expression instanceof SymbolReference, "Unexpected expression: %s", expression);
            return new VariableReferenceExpression(getSourceLocation(expression), ((SymbolReference) expression).getName(), types.get(expression));
        }
    }

    private class FilterRowExpressionStatsCalculatingVisitor
            implements RowExpressionVisitor<PlanNodeStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final ConnectorSession session;
        private final Session systemSession;
        private final FunctionAndTypeManager functionAndTypeManager;
        private final PlanNodeStatsEstimateMath calculator;
        private final ComparisonStatsCalculator comparisonCalculator;

        FilterRowExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, ConnectorSession session, FunctionAndTypeManager functionAndTypeManager, Session systemSession)
        {
            this.input = requireNonNull(input, "input is null");
            this.session = requireNonNull(session, "session is null");
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
            this.systemSession = requireNonNull(systemSession, "systemSession is null");
            this.calculator = new PlanNodeStatsEstimateMath(SystemSessionProperties.shouldOptimizerUseHistograms(systemSession));
            this.comparisonCalculator = new ComparisonStatsCalculator(systemSession);
        }

        @Override
        public PlanNodeStatsEstimate visitSpecialForm(SpecialFormExpression node, Void context)
        {
            switch (node.getForm()) {
                case AND: {
                    return estimateLogicalAnd(node.getArguments().get(0), node.getArguments().get(1));
                }
                case OR: {
                    return estimateLogicalOr(node.getArguments().get(0), node.getArguments().get(1));
                }
                case IN: {
                    return estimateIn(node.getArguments().get(0), node.getArguments().subList(1, node.getArguments().size()));
                }
                case IS_NULL: {
                    return estimateIsNull(node.getArguments().get(0));
                }
                default:
                    return PlanNodeStatsEstimate.unknown();
            }
        }

        @Override
        public PlanNodeStatsEstimate visitConstant(ConstantExpression node, Void context)
        {
            if (node.getType().equals(BOOLEAN)) {
                if (node.getValue() != null && (boolean) node.getValue()) {
                    return input;
                }
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
                result.setOutputRowCount(0.0);
                input.getVariablesWithKnownStatistics().forEach(variable -> result.addVariableStatistics(variable, VariableStatsEstimate.zero()));
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitLambda(LambdaDefinitionExpression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitVariableReference(VariableReferenceExpression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitCall(CallExpression node, Void context)
        {
            // comparison case
            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(node.getFunctionHandle());
            if (functionMetadata.getOperatorType().map(OperatorType::isComparisonOperator).orElse(false)) {
                OperatorType operatorType = functionMetadata.getOperatorType().get();
                RowExpression left = node.getArguments().get(0);
                RowExpression right = node.getArguments().get(1);

                checkArgument(!(left instanceof ConstantExpression && right instanceof ConstantExpression), "Literal-to-literal not supported here, should be eliminated earlier");

                if (!(left instanceof VariableReferenceExpression) && right instanceof VariableReferenceExpression) {
                    // normalize so that variable is on the left
                    OperatorType flippedOperator = flip(operatorType);
                    return process(call(node.getSourceLocation(), flippedOperator.name(), metadata.getFunctionAndTypeManager().resolveOperator(flippedOperator, fromTypes(right.getType(), left.getType())), BOOLEAN, right, left));
                }

                if (left instanceof ConstantExpression) {
                    // normalize so that literal is on the right
                    OperatorType flippedOperator = flip(operatorType);
                    return process(call(node.getSourceLocation(), flippedOperator.name(), metadata.getFunctionAndTypeManager().resolveOperator(flippedOperator, fromTypes(right.getType(), left.getType())), BOOLEAN, right, left));
                }

                if (left instanceof VariableReferenceExpression && left.equals(right)) {
                    return process(not(isNull(left)));
                }

                VariableStatsEstimate leftStats = getRowExpressionStats(left);
                Optional<VariableReferenceExpression> leftVariable = left instanceof VariableReferenceExpression ? Optional.of((VariableReferenceExpression) left) : Optional.empty();
                if (right instanceof ConstantExpression) {
                    Object rightValue = ((ConstantExpression) right).getValue();
                    if (rightValue == null) {
                        return visitConstant(constantNull(right.getSourceLocation(), BOOLEAN), null);
                    }
                    OptionalDouble literal = toStatsRepresentation(metadata.getFunctionAndTypeManager(), session, right.getType(), rightValue);
                    return comparisonCalculator.estimateExpressionToLiteralComparison(input, leftStats, leftVariable, literal, getComparisonOperator(operatorType));
                }

                VariableStatsEstimate rightStats = getRowExpressionStats(right);
                if (rightStats.isSingleValue()) {
                    OptionalDouble value = isNaN(rightStats.getLowValue()) ? OptionalDouble.empty() : OptionalDouble.of(rightStats.getLowValue());
                    return comparisonCalculator.estimateExpressionToLiteralComparison(input, leftStats, leftVariable, value, getComparisonOperator(operatorType));
                }

                Optional<VariableReferenceExpression> rightVariable = right instanceof VariableReferenceExpression ? Optional.of((VariableReferenceExpression) right) : Optional.empty();
                return comparisonCalculator.estimateExpressionToExpressionComparison(input, leftStats, leftVariable, rightStats, rightVariable, getComparisonOperator(operatorType));
            }

            // NOT case
            if (node.getFunctionHandle().equals(functionResolution.notFunction())) {
                RowExpression argument = node.getArguments().get(0);
                if (argument instanceof SpecialFormExpression && ((SpecialFormExpression) argument).getForm().equals(IS_NULL)) {
                    // IS NOT NULL case
                    RowExpression innerArgument = ((SpecialFormExpression) argument).getArguments().get(0);
                    if (innerArgument instanceof VariableReferenceExpression) {
                        VariableReferenceExpression variable = (VariableReferenceExpression) innerArgument;
                        VariableStatsEstimate variableStats = input.getVariableStatistics(variable);
                        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                        result.setOutputRowCount(input.getOutputRowCount() * (1 - variableStats.getNullsFraction()));
                        result.addVariableStatistics(variable, variableStats.mapNullsFraction(x -> 0.0));
                        return result.build();
                    }
                    return PlanNodeStatsEstimate.unknown();
                }
                return calculator.subtractSubsetStats(input, process(argument));
            }

            // BETWEEN case
            if (functionResolution.isBetweenFunction(node.getFunctionHandle())) {
                RowExpression value = node.getArguments().get(0);
                RowExpression min = node.getArguments().get(1);
                RowExpression max = node.getArguments().get(2);
                if (!(value instanceof VariableReferenceExpression)) {
                    return PlanNodeStatsEstimate.unknown();
                }
                if (!getRowExpressionStats(min).isSingleValue()) {
                    return PlanNodeStatsEstimate.unknown();
                }
                if (!getRowExpressionStats(max).isSingleValue()) {
                    return PlanNodeStatsEstimate.unknown();
                }

                VariableStatsEstimate valueStats = input.getVariableStatistics((VariableReferenceExpression) value);
                RowExpression lowerBound = call(
                        value.getSourceLocation(),
                        OperatorType.GREATER_THAN_OR_EQUAL.name(),
                        metadata.getFunctionAndTypeManager().resolveOperator(OperatorType.GREATER_THAN_OR_EQUAL, fromTypes(value.getType(), min.getType())),
                        BOOLEAN,
                        value,
                        min);
                RowExpression upperBound = call(
                        value.getSourceLocation(),
                        OperatorType.LESS_THAN_OR_EQUAL.name(),
                        metadata.getFunctionAndTypeManager().resolveOperator(OperatorType.LESS_THAN_OR_EQUAL, fromTypes(value.getType(), max.getType())),
                        BOOLEAN,
                        value,
                        max);

                RowExpression transformed;
                if (isInfinite(valueStats.getLowValue())) {
                    // We want to do heuristic cut (infinite range to finite range) ASAP and then do filtering on finite range.
                    // We rely on 'and()' being processed left to right
                    transformed = and(lowerBound, upperBound);
                }
                else {
                    transformed = and(upperBound, lowerBound);
                }
                return process(transformed);
            }

            if (isDynamicFilter(node)) {
                return process(TRUE_CONSTANT);
            }

            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitInputReference(InputReferenceExpression node, Void context)
        {
            throw new UnsupportedOperationException("plan node stats estimation should not reach channel mapping");
        }

        private FilterRowExpressionStatsCalculatingVisitor newEstimate(PlanNodeStatsEstimate input)
        {
            return new FilterRowExpressionStatsCalculatingVisitor(input, session, functionAndTypeManager, systemSession);
        }

        private PlanNodeStatsEstimate process(RowExpression rowExpression)
        {
            return normalizer.normalize(rowExpression.accept(this, null));
        }

        private PlanNodeStatsEstimate estimateLogicalAnd(RowExpression left, RowExpression right)
        {
            // first try to estimate in the fair way
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (!leftEstimate.isOutputRowCountUnknown()) {
                PlanNodeStatsEstimate logicalAndEstimate = newEstimate(leftEstimate).process(right);
                if (!logicalAndEstimate.isOutputRowCountUnknown()) {
                    return logicalAndEstimate;
                }
            }

            // If some of the filters cannot be estimated, take the smallest estimate.
            // Apply 0.9 filter factor as "unknown filter" factor.
            PlanNodeStatsEstimate rightEstimate = process(right);
            PlanNodeStatsEstimate smallestKnownEstimate;
            if (leftEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = rightEstimate;
            }
            else if (rightEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = leftEstimate;
            }
            else {
                smallestKnownEstimate = leftEstimate.getOutputRowCount() <= rightEstimate.getOutputRowCount() ? leftEstimate : rightEstimate;
            }
            if (smallestKnownEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }
            return smallestKnownEstimate.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT);
        }

        private PlanNodeStatsEstimate estimateLogicalOr(RowExpression left, RowExpression right)
        {
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (leftEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate rightEstimate = process(right);
            if (rightEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            if (areDisjoint(branches(pins(left)), branches(pins(right)))) {
                return calculator.capStats(calculator.addStatsAndSumDistinctValues(leftEstimate, rightEstimate), input);
            }

            PlanNodeStatsEstimate andEstimate = newEstimate(leftEstimate).process(right);
            if (andEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            return calculator.capStats(
                    calculator.subtractSubsetStats(
                            calculator.addStatsAndSumDistinctValues(leftEstimate, rightEstimate),
                            andEstimate),
                    input);
        }

        private Optional<List<Map<VariableReferenceExpression, Set<Object>>>> pins(RowExpression expression)
        {
            if (expression instanceof SpecialFormExpression) {
                SpecialFormExpression special = (SpecialFormExpression) expression;
                List<RowExpression> arguments = special.getArguments();
                switch (special.getForm()) {
                    case AND:
                        return arguments.stream().map(this::pins).reduce(FilterStatsCalculator::andPins).orElse(Optional.empty());
                    case OR:
                        return arguments.stream().map(this::pins).reduce(FilterStatsCalculator::orPins).orElse(Optional.empty());
                    case IN:
                        if (arguments.get(0) instanceof VariableReferenceExpression
                                && arguments.stream().skip(1).allMatch(ConstantExpression.class::isInstance)) {
                            return pin((VariableReferenceExpression) arguments.get(0), arguments.stream()
                                    .skip(1)
                                    .map(argument -> ((ConstantExpression) argument).getValue())
                                    .collect(toSet()));
                        }
                        return Optional.empty();
                    default:
                        return Optional.empty();
                }
            }
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                boolean isEquality = metadata.getFunctionAndTypeManager().getFunctionMetadata(call.getFunctionHandle()).getOperatorType()
                        .map(OperatorType.EQUAL::equals)
                        .orElse(false);
                if (isEquality) {
                    RowExpression left = call.getArguments().get(0);
                    RowExpression right = call.getArguments().get(1);
                    if (left instanceof VariableReferenceExpression && right instanceof ConstantExpression) {
                        return pin((VariableReferenceExpression) left, singleton(((ConstantExpression) right).getValue()));
                    }
                    if (right instanceof VariableReferenceExpression && left instanceof ConstantExpression) {
                        return pin((VariableReferenceExpression) right, singleton(((ConstantExpression) left).getValue()));
                    }
                }
            }
            return Optional.empty();
        }

        private PlanNodeStatsEstimate estimateIn(RowExpression value, List<RowExpression> candidates)
        {
            ImmutableList<PlanNodeStatsEstimate> equalityEstimates = candidates.stream()
                    .map(inValue -> process(call(value.getSourceLocation(), OperatorType.EQUAL.name(), metadata.getFunctionAndTypeManager().resolveOperator(OperatorType.EQUAL, fromTypes(value.getType(), inValue.getType())), BOOLEAN, value, inValue)))
                    .collect(toImmutableList());

            if (equalityEstimates.stream().anyMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown)) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate inEstimate = equalityEstimates.stream()
                    .reduce(calculator::addStatsAndSumDistinctValues)
                    .orElse(PlanNodeStatsEstimate.unknown());

            if (inEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            VariableStatsEstimate valueStats = getRowExpressionStats(value);
            if (valueStats.isUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - valueStats.getNullsFraction());
            double ceiledInEstimated = max(notNullValuesBeforeIn * CEIL_IN_PREDICATE_UPPER_BOUND_COEFFICIENT, 1.0);
            double inEstimateRowCount = min(inEstimate.getOutputRowCount(), ceiledInEstimated);

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
            result.setOutputRowCount(inEstimateRowCount);

            if (value instanceof VariableReferenceExpression) {
                VariableReferenceExpression valueVariable = (VariableReferenceExpression) value;
                VariableStatsEstimate newVariableStats = inEstimate.getVariableStatistics(valueVariable)
                        .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, valueStats.getDistinctValuesCount()));
                result.addVariableStatistics(valueVariable, newVariableStats);
            }
            return result.build();
        }

        private PlanNodeStatsEstimate estimateIsNull(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                VariableReferenceExpression variable = (VariableReferenceExpression) expression;
                VariableStatsEstimate variableStats = input.getVariableStatistics(variable);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * variableStats.getNullsFraction());
                result.addVariableStatistics(variable, VariableStatsEstimate.builder()
                        .setNullsFraction(1.0)
                        .setLowValue(NaN)
                        .setHighValue(NaN)
                        .setDistinctValuesCount(0.0)
                        .build());
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        private RowExpression isNull(RowExpression expression)
        {
            return new SpecialFormExpression(expression.getSourceLocation(), IS_NULL, BOOLEAN, expression);
        }

        private RowExpression not(RowExpression expression)
        {
            return call(expression.getSourceLocation(), "not", functionResolution.notFunction(), expression.getType(), expression);
        }

        private ComparisonExpression.Operator getComparisonOperator(OperatorType operator)
        {
            switch (operator) {
                case EQUAL:
                    return EQUAL;
                case NOT_EQUAL:
                    return NOT_EQUAL;
                case LESS_THAN:
                    return LESS_THAN;
                case LESS_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return GREATER_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return GREATER_THAN_OR_EQUAL;
                case IS_DISTINCT_FROM:
                    return IS_DISTINCT_FROM;
                default:
                    throw new IllegalStateException("Unsupported comparison operator type: " + operator);
            }
        }

        private OperatorType flip(OperatorType operatorType)
        {
            switch (operatorType) {
                case EQUAL:
                    return OperatorType.EQUAL;
                case NOT_EQUAL:
                    return OperatorType.NOT_EQUAL;
                case LESS_THAN:
                    return OperatorType.GREATER_THAN;
                case LESS_THAN_OR_EQUAL:
                    return OperatorType.GREATER_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return OperatorType.LESS_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return OperatorType.LESS_THAN_OR_EQUAL;
                case IS_DISTINCT_FROM:
                    return OperatorType.IS_DISTINCT_FROM;
                default:
                    throw new IllegalArgumentException("Unsupported comparison: " + operatorType);
            }
        }

        private VariableStatsEstimate getRowExpressionStats(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                VariableReferenceExpression variable = (VariableReferenceExpression) expression;
                return requireNonNull(input.getVariableStatistics(variable), () -> format("No statistics for variable %s", variable));
            }
            return scalarStatsCalculator.calculate(expression, input, session);
        }
    }
}
