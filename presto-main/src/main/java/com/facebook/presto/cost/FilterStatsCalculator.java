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
import com.facebook.presto.sql.planner.LiteralInterpreter;
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
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.cost.ComparisonStatsCalculator.comparisonExpressionToExpressionStats;
import static com.facebook.presto.cost.ComparisonStatsCalculator.comparisonExpressionToLiteralStats;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStatsAndSumDistinctValues;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.differenceInNonRangeStats;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.differenceInStats;
import static com.facebook.presto.cost.StatsUtil.toStatsRepresentation;
import static com.facebook.presto.cost.SymbolStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.cost.SymbolStatsEstimate.ZERO_STATS;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Double.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FilterStatsCalculator
{
    static final double UNKNOWN_FILTER_COEFFICIENT = 0.9;

    private final Metadata metadata;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final StatsNormalizer normalizer;

    public FilterStatsCalculator(Metadata metadata, ScalarStatsCalculator scalarStatsCalculator, StatsNormalizer normalizer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
        this.normalizer = requireNonNull(normalizer, "normalizer is null");
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            Expression predicate,
            Session session,
            TypeProvider types)
    {
        return new FilterExpressionStatsCalculatingVisitor(statsEstimate, session, types).process(predicate)
                .orElseGet(() -> normalizer.normalize(filterStatsForUnknownExpression(statsEstimate), types));
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

        private Optional<PlanNodeStatsEstimate> filterForFalseExpression()
        {
            PlanNodeStatsEstimate.Builder falseStatsBuilder = PlanNodeStatsEstimate.builder();
            input.getSymbolsWithKnownStatistics().forEach(symbol -> falseStatsBuilder.addSymbolStatistics(symbol, ZERO_STATS));
            return Optional.of(falseStatsBuilder
                    .setOutputRowCount(0.0)
                    .build());
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitNotExpression(NotExpression node, Void context)
        {
            if (node.getValue() instanceof IsNullPredicate) {
                return process(new IsNotNullPredicate(((IsNullPredicate) node.getValue()).getValue()));
            }
            return process(node.getValue()).map(childStats -> differenceInStats(input, childStats));
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            switch (node.getOperator()) {
                case AND:
                    return visitLogicalBinaryAnd(node.getLeft(), node.getRight());
                case OR:
                    return visitLogicalBinaryOr(node.getLeft(), node.getRight());
                default:
                    throw new IllegalStateException("Unimplemented logical binary operator expression " + node.getOperator());
            }
        }

        private Optional<PlanNodeStatsEstimate> visitLogicalBinaryAnd(Expression left, Expression right)
        {
            Optional<PlanNodeStatsEstimate> leftStats = process(left);
            if (leftStats.isPresent()) {
                Optional<PlanNodeStatsEstimate> andStats = new FilterExpressionStatsCalculatingVisitor(leftStats.get(), session, types).process(right);
                if (andStats.isPresent()) {
                    return andStats;
                }
                return leftStats.map(FilterStatsCalculator::filterStatsForUnknownExpression);
            }

            Optional<PlanNodeStatsEstimate> rightStats = process(right);
            return rightStats.map(FilterStatsCalculator::filterStatsForUnknownExpression);
        }

        private Optional<PlanNodeStatsEstimate> visitLogicalBinaryOr(Expression left, Expression right)
        {
            Optional<PlanNodeStatsEstimate> leftStats = process(left);
            if (!leftStats.isPresent()) {
                return Optional.empty();
            }

            Optional<PlanNodeStatsEstimate> rightStats = process(right);
            if (!rightStats.isPresent()) {
                return Optional.empty();
            }

            Optional<PlanNodeStatsEstimate> andStats = new FilterExpressionStatsCalculatingVisitor(leftStats.get(), session, types).process(right);
            if (!andStats.isPresent()) {
                return Optional.empty();
            }
            PlanNodeStatsEstimate sumStats = addStatsAndSumDistinctValues(leftStats.get(), rightStats.get());
            return Optional.of(differenceInNonRangeStats(sumStats, andStats.get()));
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            if (node.equals(BooleanLiteral.TRUE_LITERAL)) {
                return Optional.of(input);
            }
            return filterForFalseExpression();
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                Symbol symbol = Symbol.from(node.getValue());
                SymbolStatsEstimate symbolStatsEstimate = input.getSymbolStatistics(symbol);
                return Optional.of(input.mapOutputRowCount(rowCount -> rowCount * (1 - symbolStatsEstimate.getNullsFraction()))
                        .mapSymbolColumnStatistics(symbol, statsEstimate -> statsEstimate.mapNullsFraction(x -> 0.0)));
            }
            return visitExpression(node, context);
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                Symbol symbol = Symbol.from(node.getValue());
                SymbolStatsEstimate symbolStatsEstimate = input.getSymbolStatistics(symbol);
                return Optional.of(input.mapOutputRowCount(rowCount -> rowCount * symbolStatsEstimate.getNullsFraction())
                        .mapSymbolColumnStatistics(symbol, statsEstimate ->
                                SymbolStatsEstimate.builder().setNullsFraction(1.0)
                                        .setLowValue(NaN)
                                        .setHighValue(NaN)
                                        .setDistinctValuesCount(0.0).build()));
            }
            return visitExpression(node, context);
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                return visitExpression(node, context);
            }
            if (!(node.getMin() instanceof Literal || isSingleValue(getExpressionStats(node.getMin())))) {
                return visitExpression(node, context);
            }
            if (!(node.getMax() instanceof Literal || isSingleValue(getExpressionStats(node.getMax())))) {
                return visitExpression(node, context);
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
            ImmutableList<Optional<PlanNodeStatsEstimate>> valuesEqualityStats = inList.getValues().stream()
                    .map(inValue -> process(new ComparisonExpression(EQUAL, node.getValue(), inValue)))
                    .collect(ImmutableList.toImmutableList());

            if (!valuesEqualityStats.stream().allMatch(Optional::isPresent)) {
                return Optional.empty();
            }

            PlanNodeStatsEstimate statsSum = valuesEqualityStats.stream()
                    .map(Optional::get)
                    .reduce(filterForFalseExpression().get(), PlanNodeStatsEstimateMath::addStatsAndSumDistinctValues);

            if (isNaN(statsSum.getOutputRowCount())) {
                return Optional.empty();
            }

            Optional<Symbol> inValueSymbol = asSymbol(node.getValue());
            SymbolStatsEstimate inValueStats = getExpressionStats(node.getValue());
            if (Objects.equals(inValueStats, UNKNOWN_STATS)) {
                return Optional.empty();
            }

            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - inValueStats.getNullsFraction());

            PlanNodeStatsEstimate estimate = input.mapOutputRowCount(rowCount -> min(statsSum.getOutputRowCount(), notNullValuesBeforeIn));

            if (inValueSymbol.isPresent()) {
                SymbolStatsEstimate newSymbolStats = statsSum.getSymbolStatistics(inValueSymbol.get())
                        .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, inValueStats.getDistinctValuesCount()));
                estimate = estimate.mapSymbolColumnStatistics(inValueSymbol.get(), oldSymbolStats -> newSymbolStats);
            }
            return Optional.of(estimate);
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

            Optional<Symbol> leftSymbol = asSymbol(left);
            SymbolStatsEstimate leftStats = getExpressionStats(left);
            if (Objects.equals(leftStats, UNKNOWN_STATS)) {
                return visitExpression(node, context);
            }

            if (right instanceof Literal) {
                OptionalDouble literal = doubleValueFromLiteral(getType(left), (Literal) right);
                return comparisonExpressionToLiteralStats(input, leftSymbol, leftStats, literal, operator);
            }

            Optional<Symbol> rightSymbol = asSymbol(right);

            SymbolStatsEstimate rightStats = getExpressionStats(right);
            if (Objects.equals(rightStats, UNKNOWN_STATS)) {
                return visitExpression(node, context);
            }

            if (left instanceof SymbolReference && Objects.equals(left, right)) {
                return process(new IsNotNullPredicate(left));
            }

            if (isSingleValue(rightStats)) {
                OptionalDouble value = isNaN(rightStats.getLowValue()) ? OptionalDouble.empty() : OptionalDouble.of(rightStats.getLowValue());
                return comparisonExpressionToLiteralStats(input, leftSymbol, leftStats, value, operator);
            }

            return comparisonExpressionToExpressionStats(input, leftSymbol, leftStats, rightSymbol, rightStats, operator);
        }

        private Optional<Symbol> asSymbol(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                return Optional.of(Symbol.from(expression));
            }
            return Optional.empty();
        }

        private boolean isSingleValue(SymbolStatsEstimate stats)
        {
            return stats.getDistinctValuesCount() == 1.0
                    && Double.compare(stats.getLowValue(), stats.getHighValue()) == 0
                    && !isInfinite(stats.getLowValue());
        }

        private Type getType(Expression expression)
        {
            return asSymbol(expression)
                    .map(symbol -> requireNonNull(types.get(symbol), () -> format("No type for symbol %s", symbol)))
                    .orElseGet(() -> {
                        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                                metadata.getFunctionRegistry(),
                                metadata.getTypeManager(),
                                session,
                                types,
                                ImmutableList.of(),
                                // At this stage, there should be no subqueries in the plan.
                                node -> new IllegalStateException("Unexpected Subquery"),
                                false);
                        Type type = expressionAnalyzer.analyze(expression, Scope.create());
                        return type;
                    });
        }

        private SymbolStatsEstimate getExpressionStats(Expression expression)
        {
            return asSymbol(expression)
                    .map(symbol -> requireNonNull(input.getSymbolStatistics(symbol), () -> format("No statistics for symbol %s", symbol)))
                    .orElseGet(() -> scalarStatsCalculator.calculate(expression, input, session));
        }

        private OptionalDouble doubleValueFromLiteral(Type type, Literal literal)
        {
            Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), literal);
            return toStatsRepresentation(metadata, session, type, literalValue);
        }
    }
}
