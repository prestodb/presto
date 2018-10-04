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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
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
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.cost.ComparisonStatsCalculator.estimateExpressionToExpressionComparison;
import static com.facebook.presto.cost.ComparisonStatsCalculator.estimateExpressionToLiteralComparison;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStatsAndSumDistinctValues;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.differenceInNonRangeStats;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.differenceInStats;
import static com.facebook.presto.cost.StatsUtil.toStatsRepresentation;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Double.min;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class FilterStatsCalculator
{
    static final double UNKNOWN_FILTER_COEFFICIENT = 0.9;

    private final Metadata metadata;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final StatsNormalizer normalizer;
    private final LiteralEncoder literalEncoder;

    public FilterStatsCalculator(Metadata metadata, ScalarStatsCalculator scalarStatsCalculator, StatsNormalizer normalizer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
        this.normalizer = requireNonNull(normalizer, "normalizer is null");
        this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            Expression predicate,
            Session session,
            TypeProvider types)
    {
        Expression simplifiedExpression = simplifyExpression(session, predicate, types);
        return new FilterExpressionStatsCalculatingVisitor(statsEstimate, session, types)
                .process(simplifiedExpression)
                .orElseGet(() -> normalizer.normalize(filterStatsForUnknownExpression(statsEstimate), types));
    }

    private Expression simplifyExpression(Session session, Expression predicate, TypeProvider types)
    {
        // TODO reuse com.facebook.presto.sql.planner.iterative.rule.SimplifyExpressions.rewrite

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, predicate, types);
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(predicate, metadata, session, expressionTypes);
        Object value = interpreter.optimize(NoOpSymbolResolver.INSTANCE);

        if (value == null) {
            // Expression evaluates to SQL null, which in Filter is equivalent to false. This assumes the expression is a top-level expression (eg. not in NOT).
            value = false;
        }
        return literalEncoder.toExpression(value, BOOLEAN);
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Session session, Expression expression, TypeProvider types)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionRegistry(),
                metadata.getTypeManager(),
                session,
                types,
                emptyList(),
                node -> new IllegalStateException("Expected node: %s" + node),
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }

    private static PlanNodeStatsEstimate filterStatsForUnknownExpression(PlanNodeStatsEstimate inputStatistics)
    {
        return inputStatistics.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT);
    }

    private class FilterExpressionStatsCalculatingVisitor
            extends AstVisitor<Optional<PlanNodeStatsEstimate>, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final TypeProvider types;

        FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, Session session, TypeProvider types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        public Optional<PlanNodeStatsEstimate> process(Node node, @Nullable Void context)
        {
            return super.process(node, context)
                    .map(estimate -> normalizer.normalize(estimate, types));
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitExpression(Expression node, Void context)
        {
            return Optional.empty();
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitNotExpression(NotExpression node, Void context)
        {
            if (node.getValue() instanceof IsNullPredicate) {
                return process(new IsNotNullPredicate(((IsNullPredicate) node.getValue()).getValue()));
            }
            return process(node.getValue()).map(estimate -> differenceInStats(input, estimate));
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
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

        private Optional<PlanNodeStatsEstimate> estimateLogicalAnd(Expression left, Expression right)
        {
            Optional<PlanNodeStatsEstimate> leftEstimate = process(left);
            if (leftEstimate.isPresent()) {
                Optional<PlanNodeStatsEstimate> logicalAndEstimate = new FilterExpressionStatsCalculatingVisitor(leftEstimate.get(), session, types).process(right);
                if (logicalAndEstimate.isPresent()) {
                    return logicalAndEstimate;
                }
                return leftEstimate.map(FilterStatsCalculator::filterStatsForUnknownExpression);
            }

            Optional<PlanNodeStatsEstimate> rightEstimate = process(right);
            return rightEstimate.map(FilterStatsCalculator::filterStatsForUnknownExpression);
        }

        private Optional<PlanNodeStatsEstimate> estimateLogicalOr(Expression left, Expression right)
        {
            Optional<PlanNodeStatsEstimate> leftEstimate = process(left);
            if (!leftEstimate.isPresent()) {
                return Optional.empty();
            }

            Optional<PlanNodeStatsEstimate> rightEstimate = process(right);
            if (!rightEstimate.isPresent()) {
                return Optional.empty();
            }

            Optional<PlanNodeStatsEstimate> logicalAndEstimate = new FilterExpressionStatsCalculatingVisitor(leftEstimate.get(), session, types).process(right);
            if (!logicalAndEstimate.isPresent()) {
                return Optional.empty();
            }
            PlanNodeStatsEstimate sumEstimate = addStatsAndSumDistinctValues(leftEstimate.get(), rightEstimate.get());
            return Optional.of(differenceInNonRangeStats(sumEstimate, logicalAndEstimate.get()));
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            if (node.getValue()) {
                return Optional.of(input);
            }

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
            result.setOutputRowCount(0.0);
            input.getSymbolsWithKnownStatistics().forEach(symbol -> result.addSymbolStatistics(symbol, SymbolStatsEstimate.zero()));
            return Optional.of(result.build());
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                Symbol symbol = Symbol.from(node.getValue());
                SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * (1 - symbolStats.getNullsFraction()));
                result.addSymbolStatistics(symbol, symbolStats.mapNullsFraction(x -> 0.0));
                return Optional.of(result.build());
            }
            return Optional.empty();
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                Symbol symbol = Symbol.from(node.getValue());
                SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * symbolStats.getNullsFraction());
                result.addSymbolStatistics(symbol, SymbolStatsEstimate.builder()
                        .setNullsFraction(1.0)
                        .setLowValue(NaN)
                        .setHighValue(NaN)
                        .setDistinctValuesCount(0.0)
                        .build());
                return Optional.of(result.build());
            }
            return Optional.empty();
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                return Optional.empty();
            }
            if (!getExpressionStats(node.getMin()).isSingleValue()) {
                return Optional.empty();
            }
            if (!getExpressionStats(node.getMax()).isSingleValue()) {
                return Optional.empty();
            }

            SymbolStatsEstimate valueStats = input.getSymbolStatistics(Symbol.from(node.getValue()));
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
        protected Optional<PlanNodeStatsEstimate> visitInPredicate(InPredicate node, Void context)
        {
            if (!(node.getValueList() instanceof InListExpression)) {
                return Optional.empty();
            }

            InListExpression inList = (InListExpression) node.getValueList();
            ImmutableList<Optional<PlanNodeStatsEstimate>> equalityEstimates = inList.getValues().stream()
                    .map(inValue -> process(new ComparisonExpression(EQUAL, node.getValue(), inValue)))
                    .collect(toImmutableList());

            if (!equalityEstimates.stream().allMatch(Optional::isPresent)) {
                return Optional.empty();
            }

            PlanNodeStatsEstimate inEstimate = equalityEstimates.stream()
                    .map(Optional::get)
                    .reduce(PlanNodeStatsEstimateMath::addStatsAndSumDistinctValues)
                    .orElse(PlanNodeStatsEstimate.unknown());

            if (inEstimate.isOutputRowCountUnknown()) {
                return Optional.empty();
            }

            SymbolStatsEstimate valueStats = getExpressionStats(node.getValue());
            if (valueStats.isUnknown()) {
                return Optional.empty();
            }

            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - valueStats.getNullsFraction());

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
            result.setOutputRowCount(min(inEstimate.getOutputRowCount(), notNullValuesBeforeIn));

            if (node.getValue() instanceof SymbolReference) {
                Symbol valueSymbol = Symbol.from(node.getValue());
                SymbolStatsEstimate newSymbolStats = inEstimate.getSymbolStatistics(valueSymbol)
                        .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, valueStats.getDistinctValuesCount()));
                result.addSymbolStatistics(valueSymbol, newSymbolStats);
            }
            return Optional.of(result.build());
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitComparisonExpression(ComparisonExpression node, Void context)
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

            SymbolStatsEstimate leftStats = getExpressionStats(left);
            if (leftStats.isUnknown()) {
                return Optional.empty();
            }

            Optional<Symbol> leftSymbol = left instanceof SymbolReference ? Optional.of(Symbol.from(left)) : Optional.empty();
            if (right instanceof Literal) {
                OptionalDouble literal = doubleValueFromLiteral(getType(left), (Literal) right);
                PlanNodeStatsEstimate estimate = estimateExpressionToLiteralComparison(input, leftStats, leftSymbol, literal, operator);
                if (estimate.isOutputRowCountUnknown()) {
                    return Optional.empty();
                }
                return Optional.of(estimate);
            }

            SymbolStatsEstimate rightStats = getExpressionStats(right);
            if (rightStats.isUnknown()) {
                return Optional.empty();
            }

            if (left instanceof SymbolReference && left.equals(right)) {
                return process(new IsNotNullPredicate(left));
            }

            if (rightStats.isSingleValue()) {
                OptionalDouble value = isNaN(rightStats.getLowValue()) ? OptionalDouble.empty() : OptionalDouble.of(rightStats.getLowValue());
                PlanNodeStatsEstimate estimate = estimateExpressionToLiteralComparison(input, leftStats, leftSymbol, value, operator);
                if (estimate.isOutputRowCountUnknown()) {
                    return Optional.empty();
                }
                return Optional.of(estimate);
            }

            Optional<Symbol> rightSymbol = right instanceof SymbolReference ? Optional.of(Symbol.from(right)) : Optional.empty();
            PlanNodeStatsEstimate estimate = estimateExpressionToExpressionComparison(input, leftStats, leftSymbol, rightStats, rightSymbol, operator);
            if (estimate.isOutputRowCountUnknown()) {
                return Optional.empty();
            }
            return Optional.of(estimate);
        }

        private Type getType(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                Symbol symbol = Symbol.from(expression);
                return requireNonNull(types.get(symbol), () -> format("No type for symbol %s", symbol));
            }

            ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                    metadata.getFunctionRegistry(),
                    metadata.getTypeManager(),
                    session,
                    types,
                    ImmutableList.of(),
                    // At this stage, there should be no subqueries in the plan.
                    node -> new VerifyException("Unexpected subquery"),
                    false);
            return expressionAnalyzer.analyze(expression, Scope.create());
        }

        private SymbolStatsEstimate getExpressionStats(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                Symbol symbol = Symbol.from(expression);
                return requireNonNull(input.getSymbolStatistics(symbol), () -> format("No statistics for symbol %s", symbol));
            }
            return scalarStatsCalculator.calculate(expression, input, session);
        }

        private OptionalDouble doubleValueFromLiteral(Type type, Literal literal)
        {
            Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), literal);
            return toStatsRepresentation(metadata, session, type, literalValue);
        }
    }
}
