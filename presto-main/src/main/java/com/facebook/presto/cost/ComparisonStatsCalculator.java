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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.sql.tree.ComparisonExpression;

import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static com.facebook.presto.cost.VariableStatsEstimate.buildFrom;
import static com.facebook.presto.util.MoreMath.firstNonNaN;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;

public final class ComparisonStatsCalculator
{
    private static final Logger log = Logger.get(ComparisonStatsCalculator.class);

    private ComparisonStatsCalculator() {}

    public static PlanNodeStatsEstimate estimateExpressionToLiteralComparison(
            PlanNodeStatsEstimate inputStatistics,
            VariableStatsEstimate expressionStatistics,
            Optional<VariableReferenceExpression> expressionVariable,
            OptionalDouble literalValue,
            ComparisonExpression.Operator operator,
            Optional<Session> session)
    {
        switch (operator) {
            case EQUAL:
                return estimateExpressionEqualToLiteral(inputStatistics, expressionStatistics, expressionVariable, literalValue, session);
            case NOT_EQUAL:
                return estimateExpressionNotEqualToLiteral(inputStatistics, expressionStatistics, expressionVariable, literalValue, session);
            case LESS_THAN:
                return estimateExpressionLessThanLiteral(inputStatistics, expressionStatistics, expressionVariable, literalValue, false, session);
            case LESS_THAN_OR_EQUAL:
                return estimateExpressionLessThanLiteral(inputStatistics, expressionStatistics, expressionVariable, literalValue, true, session);
            case GREATER_THAN:
                return estimateExpressionGreaterThanLiteral(inputStatistics, expressionStatistics, expressionVariable, literalValue, false, session);
            case GREATER_THAN_OR_EQUAL:
                return estimateExpressionGreaterThanLiteral(inputStatistics, expressionStatistics, expressionVariable, literalValue, true, session);
            case IS_DISTINCT_FROM:
                return PlanNodeStatsEstimate.unknown();
            default:
                throw new IllegalArgumentException("Unexpected comparison operator: " + operator);
        }
    }

    private static PlanNodeStatsEstimate estimateExpressionEqualToLiteral(
            PlanNodeStatsEstimate inputStatistics,
            VariableStatsEstimate expressionStatistics,
            Optional<VariableReferenceExpression> expressionVariable,
            OptionalDouble literalValue,
            Optional<Session> session)
    {
        StatisticRange filterRange;
        if (literalValue.isPresent()) {
            filterRange = new StatisticRange(literalValue.getAsDouble(), false, literalValue.getAsDouble(), false, 1);
        }
        else {
            filterRange = new StatisticRange(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
        }
        return estimateFilterRange(inputStatistics, expressionStatistics, expressionVariable, filterRange, session);
    }

    private static PlanNodeStatsEstimate estimateExpressionNotEqualToLiteral(
            PlanNodeStatsEstimate inputStatistics,
            VariableStatsEstimate expressionStatistics,
            Optional<VariableReferenceExpression> expressionVariable,
            OptionalDouble literalValue,
            Optional<Session> session)
    {
        StatisticRange filterRange;
        if (literalValue.isPresent()) {
            filterRange = new StatisticRange(literalValue.getAsDouble(), false, literalValue.getAsDouble(), false, 1);
        }
        else {
            filterRange = new StatisticRange(NEGATIVE_INFINITY, true, POSITIVE_INFINITY, true, 1);
        }
        double filterFactor = 1 - calculateFilterFactor(expressionStatistics, filterRange, session);

        PlanNodeStatsEstimate.Builder estimate = PlanNodeStatsEstimate.buildFrom(inputStatistics);
        estimate.setOutputRowCount(filterFactor * (1 - expressionStatistics.getNullsFraction()) * inputStatistics.getOutputRowCount());
        if (expressionVariable.isPresent()) {
            VariableStatsEstimate symbolNewEstimate = buildFrom(expressionStatistics)
                    .setNullsFraction(0.0)
                    .setDistinctValuesCount(max(expressionStatistics.getDistinctValuesCount() - 1, 0))
                    .build();
            estimate = estimate.addVariableStatistics(expressionVariable.get(), symbolNewEstimate);
        }
        return estimate.build();
    }

    private static PlanNodeStatsEstimate estimateExpressionLessThanLiteral(
            PlanNodeStatsEstimate inputStatistics,
            VariableStatsEstimate expressionStatistics,
            Optional<VariableReferenceExpression> expressionVariable,
            OptionalDouble literalValue,
            boolean equals,
            Optional<Session> session)
    {
        StatisticRange filterRange = new StatisticRange(NEGATIVE_INFINITY, true, literalValue.orElse(POSITIVE_INFINITY), !equals, NaN);
        return estimateFilterRange(inputStatistics, expressionStatistics, expressionVariable, filterRange, session);
    }

    private static PlanNodeStatsEstimate estimateExpressionGreaterThanLiteral(
            PlanNodeStatsEstimate inputStatistics,
            VariableStatsEstimate expressionStatistics,
            Optional<VariableReferenceExpression> expressionVariable,
            OptionalDouble literalValue,
            boolean equals,
            Optional<Session> session)
    {
        StatisticRange filterRange = new StatisticRange(literalValue.orElse(NEGATIVE_INFINITY), !equals, POSITIVE_INFINITY, true, NaN);
        return estimateFilterRange(inputStatistics, expressionStatistics, expressionVariable, filterRange, session);
    }

    private static PlanNodeStatsEstimate estimateFilterRange(
            PlanNodeStatsEstimate inputStatistics,
            VariableStatsEstimate expressionStatistics,
            Optional<VariableReferenceExpression> expressionVariable,
            StatisticRange filterRange,
            Optional<Session> session)
    {
        double filterFactor = calculateFilterFactor(expressionStatistics, filterRange, session);

        StatisticRange expressionRange = StatisticRange.from(expressionStatistics);
        StatisticRange intersectRange = expressionRange.intersect(filterRange);
        PlanNodeStatsEstimate estimate = inputStatistics.mapOutputRowCount(rowCount -> filterFactor * (1 - expressionStatistics.getNullsFraction()) * rowCount);
        if (expressionVariable.isPresent()) {
            VariableStatsEstimate symbolNewEstimate =
                    VariableStatsEstimate.builder()
                            .setAverageRowSize(expressionStatistics.getAverageRowSize())
                            .setStatisticsRange(intersectRange)
                            .setNullsFraction(0.0)
                            .setHistogram(DisjointRangeDomainHistogram.addConjunction(expressionStatistics.getHistogram(), intersectRange))
                            .build();
            estimate = estimate.mapVariableColumnStatistics(expressionVariable.get(), oldStats -> symbolNewEstimate);
        }
        return estimate;
    }

    private static double calculateFilterFactor(VariableStatsEstimate variableStatistics, StatisticRange filterRange, Optional<Session> session)
    {
        StatisticRange variableRange = StatisticRange.from(variableStatistics);
        StatisticRange intersectRange = variableRange.intersect(filterRange);
        Estimate filterEstimate;
        if (session.map(SystemSessionProperties::shouldOptimizerUseHistograms).orElse(false)) {
            Estimate distinctEstimate = isNaN(variableStatistics.getDistinctValuesCount()) ? Estimate.unknown() : Estimate.of(variableRange.getDistinctValuesCount());
            filterEstimate = HistogramCalculator.calculateFilterFactor(intersectRange, variableStatistics.getHistogram(), distinctEstimate, true);
            if (log.isDebugEnabled()) {
                double expressionFilter = variableRange.overlapPercentWith(intersectRange);
                if (!Double.isNaN(expressionFilter) &&
                        !filterEstimate.fuzzyEquals(Estimate.of(expressionFilter), .0001)) {
                    log.debug(String.format("histogram-calculated filter factor differs from the uniformity assumption:" +
                            "expression range: %s%n" +
                            "intersect range: %s%n" +
                            "overlapPercent: %s%n" +
                            "histogram: %s%n" +
                            "histogramFilterIntersect: %s%n", variableRange, intersectRange, expressionFilter, variableStatistics.getHistogram(), filterEstimate));
                }
            }
        }
        else {
            filterEstimate = Estimate.estimateFromDouble(variableRange.overlapPercentWith(intersectRange));
        }

        return filterEstimate.orElse(() -> UNKNOWN_FILTER_COEFFICIENT);
    }

    public static PlanNodeStatsEstimate estimateExpressionToExpressionComparison(
            PlanNodeStatsEstimate inputStatistics,
            VariableStatsEstimate leftExpressionStatistics,
            Optional<VariableReferenceExpression> leftExpressionVariable,
            VariableStatsEstimate rightExpressionStatistics,
            Optional<VariableReferenceExpression> rightExpressionVariable,
            ComparisonExpression.Operator operator)
    {
        switch (operator) {
            case EQUAL:
                return estimateExpressionEqualToExpression(inputStatistics, leftExpressionStatistics, leftExpressionVariable, rightExpressionStatistics, rightExpressionVariable);
            case NOT_EQUAL:
                return estimateExpressionNotEqualToExpression(inputStatistics, leftExpressionStatistics, leftExpressionVariable, rightExpressionStatistics, rightExpressionVariable);
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case IS_DISTINCT_FROM:
                return PlanNodeStatsEstimate.unknown();
            default:
                throw new IllegalArgumentException("Unexpected comparison operator: " + operator);
        }
    }

    private static PlanNodeStatsEstimate estimateExpressionEqualToExpression(
            PlanNodeStatsEstimate inputStatistics,
            VariableStatsEstimate leftExpressionStatistics,
            Optional<VariableReferenceExpression> leftExpressionVariable,
            VariableStatsEstimate rightExpressionStatistics,
            Optional<VariableReferenceExpression> rightExpressionVariable)
    {
        if (isNaN(leftExpressionStatistics.getDistinctValuesCount()) || isNaN(rightExpressionStatistics.getDistinctValuesCount())) {
            return PlanNodeStatsEstimate.unknown();
        }

        StatisticRange leftExpressionRange = StatisticRange.from(leftExpressionStatistics);
        StatisticRange rightExpressionRange = StatisticRange.from(rightExpressionStatistics);

        StatisticRange intersect = leftExpressionRange.intersect(rightExpressionRange);

        double nullsFilterFactor = (1 - leftExpressionStatistics.getNullsFraction()) * (1 - rightExpressionStatistics.getNullsFraction());
        double leftNdv = leftExpressionRange.getDistinctValuesCount();
        double rightNdv = rightExpressionRange.getDistinctValuesCount();
        double filterFactor = 1.0 / max(leftNdv, rightNdv, 1);
        double retainedNdv = min(leftNdv, rightNdv);

        PlanNodeStatsEstimate.Builder estimate = PlanNodeStatsEstimate.buildFrom(inputStatistics)
                .setOutputRowCount(inputStatistics.getOutputRowCount() * nullsFilterFactor * filterFactor);

        VariableStatsEstimate equalityStats = VariableStatsEstimate.builder()
                .setAverageRowSize(averageExcludingNaNs(leftExpressionStatistics.getAverageRowSize(), rightExpressionStatistics.getAverageRowSize()))
                .setNullsFraction(0)
                .setStatisticsRange(intersect)
                .setDistinctValuesCount(retainedNdv)
                .build();

        leftExpressionVariable.ifPresent(variable -> estimate.addVariableStatistics(variable, equalityStats));
        rightExpressionVariable.ifPresent(variable -> estimate.addVariableStatistics(variable, equalityStats));

        return estimate.build();
    }

    private static PlanNodeStatsEstimate estimateExpressionNotEqualToExpression(
            PlanNodeStatsEstimate inputStatistics,
            VariableStatsEstimate leftExpressionStatistics,
            Optional<VariableReferenceExpression> leftExpressionVariable,
            VariableStatsEstimate rightExpressionStatistics,
            Optional<VariableReferenceExpression> rightExpressionVariable)
    {
        double nullsFilterFactor = (1 - leftExpressionStatistics.getNullsFraction()) * (1 - rightExpressionStatistics.getNullsFraction());
        PlanNodeStatsEstimate inputNullsFiltered = inputStatistics.mapOutputRowCount(size -> size * nullsFilterFactor);
        VariableStatsEstimate leftNullsFiltered = leftExpressionStatistics.mapNullsFraction(nullsFraction -> 0.0);
        VariableStatsEstimate rightNullsFiltered = rightExpressionStatistics.mapNullsFraction(nullsFraction -> 0.0);
        PlanNodeStatsEstimate equalityStats = estimateExpressionEqualToExpression(
                inputNullsFiltered,
                leftNullsFiltered,
                leftExpressionVariable,
                rightNullsFiltered,
                rightExpressionVariable);
        if (equalityStats.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(inputNullsFiltered);
        double equalityFilterFactor = equalityStats.getOutputRowCount() / inputNullsFiltered.getOutputRowCount();
        if (!isFinite(equalityFilterFactor)) {
            equalityFilterFactor = 0.0;
        }
        result.setOutputRowCount(inputNullsFiltered.getOutputRowCount() * (1 - equalityFilterFactor));
        leftExpressionVariable.ifPresent(symbol -> result.addVariableStatistics(symbol, leftNullsFiltered));
        rightExpressionVariable.ifPresent(symbol -> result.addVariableStatistics(symbol, rightNullsFiltered));
        return result.build();
    }

    private static double averageExcludingNaNs(double first, double second)
    {
        if (isNaN(first) && isNaN(second)) {
            return NaN;
        }
        if (!isNaN(first) && !isNaN(second)) {
            return (first + second) / 2;
        }
        return firstNonNaN(first, second);
    }
}
