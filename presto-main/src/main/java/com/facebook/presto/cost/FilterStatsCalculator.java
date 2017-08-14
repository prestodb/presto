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
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.cost.ComparisonStatsCalculator.comparisonExpressionToExpressionStats;
import static com.facebook.presto.cost.ComparisonStatsCalculator.comparisonExpressionToLiteralStats;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.addStatsAndSumDistinctValues;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.differenceInNonRangeStats;
import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.differenceInStats;
import static com.facebook.presto.cost.SymbolStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.cost.SymbolStatsEstimate.buildFrom;
import static com.facebook.presto.sql.ExpressionUtils.and;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.LESS_THAN_OR_EQUAL;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.NaN;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Double.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FilterStatsCalculator
{
    public static final double UNKNOWN_FILTER_COEFFICIENT = 0.9;

    private final Metadata metadata;

    @Inject
    public FilterStatsCalculator(Metadata metadata)
    {
        this.metadata = metadata;
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            Expression predicate,
            Session session,
            Map<Symbol, Type> types)
    {
        return new FilterExpressionStatsCalculatingVisitor(statsEstimate, session, types).process(predicate)
                .orElse(filterStatsForUnknownExpression(statsEstimate));
    }

    public static PlanNodeStatsEstimate filterStatsForUnknownExpression(PlanNodeStatsEstimate inputStatistics)
    {
        return inputStatistics.mapOutputRowCount(size -> size * UNKNOWN_FILTER_COEFFICIENT);
    }

    private class FilterExpressionStatsCalculatingVisitor
            extends AstVisitor<Optional<PlanNodeStatsEstimate>, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final Map<Symbol, Type> types;

        FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, Session session, Map<Symbol, Type> types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitExpression(Expression node, Void context)
        {
            return Optional.empty();
        }

        private Optional<PlanNodeStatsEstimate> filterForFalseExpression()
        {
            PlanNodeStatsEstimate.Builder falseStatsBuilder = PlanNodeStatsEstimate.builder();

            input.getSymbolsWithKnownStatistics().forEach(
                    symbol ->
                            falseStatsBuilder.addSymbolStatistics(symbol,
                                    buildFrom(input.getSymbolStatistics(symbol))
                                            .setLowValue(NaN)
                                            .setHighValue(NaN)
                                            .setDistinctValuesCount(0.0)
                                            .setNullsFraction(NaN).build()));

            return Optional.of(falseStatsBuilder.setOutputRowCount(0.0).build());
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
            Optional<PlanNodeStatsEstimate> leftStats = process(node.getLeft());
            Optional<PlanNodeStatsEstimate> rightStats = process(node.getRight());

            Optional<PlanNodeStatsEstimate> andStats;
            if (leftStats.isPresent() && rightStats.isPresent()) {
                andStats = new FilterExpressionStatsCalculatingVisitor(leftStats.get(), session, types).process(node.getRight());
            }
            else if (leftStats.isPresent() && !rightStats.isPresent()) {
                andStats = leftStats.map(FilterStatsCalculator::filterStatsForUnknownExpression);
            }
            else if (!leftStats.isPresent() && rightStats.isPresent()) {
                andStats = rightStats.map(FilterStatsCalculator::filterStatsForUnknownExpression);
            }
            else {
                andStats = Optional.empty();
            }
            if (leftStats.isPresent() && !andStats.isPresent()) {
                // if we know stats for at least one arg let's use this.
                andStats = leftStats.map(FilterStatsCalculator::filterStatsForUnknownExpression);
            }

            switch (node.getType()) {
                case AND:
                    return andStats;
                case OR:
                    if (!leftStats.isPresent() || !rightStats.isPresent()) {
                        return visitExpression(node, context);
                    }
                    checkState(andStats.isPresent(), "Expected andStats to be present");
                    PlanNodeStatsEstimate sumStats = addStatsAndSumDistinctValues(leftStats.get(), rightStats.get());
                    return Optional.of(differenceInNonRangeStats(sumStats, andStats.get()));
                default:
                    throw new IllegalStateException(format("Unimplemented logical binary operator expression %s", node.getType()));
            }
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            if (node.equals(BooleanLiteral.TRUE_LITERAL)) {
                return Optional.of(input);
            }
            else {
                return filterForFalseExpression();
            }
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
            if (!(node.getValue() instanceof SymbolReference) || !(node.getMin() instanceof Literal) || !(node.getMax() instanceof Literal)) {
                return visitExpression(node, context);
            }

            SymbolStatsEstimate valueStats = input.getSymbolStatistics(Symbol.from(node.getValue()));
            Expression leftComparison;
            Expression rightComparison;

            // We want to do heuristic cut (infinite range to finite range) ASAP and than do filtering on finite range.
            if (isInfinite(valueStats.getLowValue())) {
                leftComparison = new ComparisonExpression(GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin());
                rightComparison = new ComparisonExpression(LESS_THAN_OR_EQUAL, node.getValue(), node.getMax());
            }
            else {
                rightComparison = new ComparisonExpression(GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin());
                leftComparison = new ComparisonExpression(LESS_THAN_OR_EQUAL, node.getValue(), node.getMax());
            }

            // we relay on and processing left to right
            return process(and(leftComparison, rightComparison));
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitInPredicate(InPredicate node, Void context)
        {
            if (!(node.getValueList() instanceof InListExpression) || !(node.getValue() instanceof SymbolReference)) {
                return visitExpression(node, context);
            }

            InListExpression inList = (InListExpression) node.getValueList();
            ImmutableList<Optional<PlanNodeStatsEstimate>> valuesEqualityStats = inList.getValues().stream()
                    .map(inValue -> process(new ComparisonExpression(EQUAL, node.getValue(), inValue)))
                    .collect(ImmutableList.toImmutableList());

            if (!valuesEqualityStats.stream().allMatch(Optional::isPresent)) {
                return visitExpression(node, context);
            }

            PlanNodeStatsEstimate statsSum = valuesEqualityStats.stream()
                    .map(Optional::get)
                    .reduce(filterForFalseExpression().get(), PlanNodeStatsEstimateMath::addStatsAndSumDistinctValues);

            if (isNaN(statsSum.getOutputRowCount())) {
                return visitExpression(node, context);
            }

            Symbol inValueSymbol = Symbol.from(node.getValue());
            SymbolStatsEstimate symbolStats = input.getSymbolStatistics(inValueSymbol);
            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - symbolStats.getNullsFraction());

            SymbolStatsEstimate newSymbolStats = statsSum.getSymbolStatistics(inValueSymbol)
                    .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, symbolStats.getDistinctValuesCount()));

            return Optional.of(input.mapOutputRowCount(rowCount -> min(statsSum.getOutputRowCount(), notNullValuesBeforeIn))
                    .mapSymbolColumnStatistics(inValueSymbol, oldSymbolStats -> newSymbolStats));
        }

        @Override
        protected Optional<PlanNodeStatsEstimate> visitComparisonExpression(ComparisonExpression node, Void context)
        {
            // TODO: verify we eliminate Literal-Literal earlier or support them here

            ComparisonExpressionType type = node.getType();
            Expression left = node.getLeft();
            Expression right = node.getRight();

            if (!(left instanceof SymbolReference) && right instanceof SymbolReference) {
                // normalize so that symbol is on the left
                return process(new ComparisonExpression(type.flip(), right, left));
            }

            if (left instanceof Literal && !(right instanceof Literal)) {
                // normalize so that literal is on the right
                return process(new ComparisonExpression(type.flip(), right, left));
            }

            Optional<Symbol> leftSymbol = asSymbol(left);
            Optional<SymbolStatsEstimate> leftStats = getSymbolStatsEstimate(left);
            if (!leftStats.isPresent()) {
                return visitExpression(node, context);
            }

            if (right instanceof Literal) {
                // TODO support Cast(Literal) same way as Literal (nested Casts too)
                OptionalDouble literal = doubleValueFromLiteral(getType(left), (Literal) right);
                return comparisonExpressionToLiteralStats(input, leftSymbol, leftStats.get(), literal, type);
            }

            Optional<Symbol> rightSymbol = asSymbol(right);
            Optional<SymbolStatsEstimate> rightStats = getSymbolStatsEstimate(right);
            if (!rightStats.isPresent()) {
                return visitExpression(node, context);
            }

            if (leftStats.equals(UNKNOWN_STATS) || rightStats.equals(UNKNOWN_STATS)) {
                return Optional.empty();
            }
            return comparisonExpressionToExpressionStats(input, leftSymbol, leftStats.get(), rightSymbol, rightStats.get(), type);
        }

        private Optional<Symbol> asSymbol(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                return Optional.of(Symbol.from(expression));
            }
            return Optional.empty();
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

        private Optional<SymbolStatsEstimate> getSymbolStatsEstimate(Expression expression)
        {
            SymbolStatsEstimate symbolStatsEstimate = asSymbol(expression)
                    .map(symbol -> requireNonNull(input.getSymbolStatistics(symbol), () -> format("No statistics for symbol %s", symbol)))
                    .orElseGet(() -> new ScalarStatsCalculator(metadata).calculate(expression, input, session, types));

            if (UNKNOWN_STATS.equals(symbolStatsEstimate)) {
                return Optional.empty();
            }
            else {
                return Optional.of(symbolStatsEstimate);
            }
        }

        private OptionalDouble doubleValueFromLiteral(Type type, Literal literal)
        {
            Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), literal);
            DomainConverter domainConverter = new DomainConverter(type, metadata.getFunctionRegistry(), session.toConnectorSession());
            return domainConverter.translateToDouble(literalValue);
        }
    }
}
