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
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.util.MoreMath;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.cost.SymbolStatsEstimate.buildFrom;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.util.MoreMath.rangeMax;
import static com.facebook.presto.util.MoreMath.rangeMin;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Math.min;
import static java.util.Comparator.comparingDouble;
import static java.util.Objects.requireNonNull;

public class JoinStatsRule
        extends SimpleStatsRule
{
    private static final double DEFAULT_UNMATCHED_JOIN_COMPLEMENT_NDVS_COEFFICIENT = 0.5;

    private final FilterStatsCalculator filterStatsCalculator;
    private final double unmatchedJoinComplementNdvsCoefficient;

    public JoinStatsRule(FilterStatsCalculator filterStatsCalculator, StatsNormalizer normalizer)
    {
        this(filterStatsCalculator, normalizer, DEFAULT_UNMATCHED_JOIN_COMPLEMENT_NDVS_COEFFICIENT);
    }

    @VisibleForTesting
    JoinStatsRule(FilterStatsCalculator filterStatsCalculator, StatsNormalizer normalizer, double unmatchedJoinComplementNdvsCoefficient)
    {
        super(normalizer);
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator is null");
        this.unmatchedJoinComplementNdvsCoefficient = unmatchedJoinComplementNdvsCoefficient;
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return Pattern.typeOf(JoinNode.class);
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(PlanNode node, StatsProvider sourceStats, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        JoinNode joinNode = (JoinNode) node;

        PlanNodeStatsEstimate leftStats = sourceStats.getStats(joinNode.getLeft());
        PlanNodeStatsEstimate rightStats = sourceStats.getStats(joinNode.getRight());
        PlanNodeStatsEstimate crossJoinStats = crossJoinStats(joinNode, leftStats, rightStats);

        switch (joinNode.getType()) {
            case INNER:
                return Optional.of(computeInnerJoinStats(joinNode, crossJoinStats, session, types));
            case LEFT:
                return Optional.of(computeLeftJoinStats(joinNode, leftStats, rightStats, crossJoinStats, session, types));
            case RIGHT:
                return Optional.of(computeRightJoinStats(joinNode, leftStats, rightStats, crossJoinStats, session, types));
            case FULL:
                return Optional.of(computeFullJoinStats(joinNode, leftStats, rightStats, crossJoinStats, session, types));
            default:
                throw new IllegalStateException("Unknown join type: " + joinNode.getType());
        }
    }

    private PlanNodeStatsEstimate computeFullJoinStats(
            JoinNode node,
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            PlanNodeStatsEstimate crossJoinStats,
            Session session,
            Map<Symbol, Type> types)
    {
        JoinComplementStats rightJoinComplementStats = calculateJoinComplementStats(node.getFilter(), flippedCriteria(node), rightStats, leftStats);
        return addJoinComplementStats(
                crossJoinStats,
                computeLeftJoinStats(node, leftStats, rightStats, crossJoinStats, session, types),
                rightJoinComplementStats,
                getDrivingClauseSymbolMaxNdvs(rightJoinComplementStats, rightStats));
    }

    private PlanNodeStatsEstimate computeLeftJoinStats(
            JoinNode node,
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            PlanNodeStatsEstimate crossJoinStats,
            Session session,
            Map<Symbol, Type> types)
    {
        PlanNodeStatsEstimate innerJoinStats = computeInnerJoinStats(node, crossJoinStats, session, types);
        JoinComplementStats leftJoinComplementStats = calculateJoinComplementStats(node.getFilter(), node.getCriteria(), leftStats, rightStats);
        return addJoinComplementStats(
                crossJoinStats,
                innerJoinStats,
                leftJoinComplementStats,
                getDrivingClauseSymbolMaxNdvs(leftJoinComplementStats, leftStats));
    }

    private PlanNodeStatsEstimate computeRightJoinStats(
            JoinNode node,
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            PlanNodeStatsEstimate crossJoinStats,
            Session session,
            Map<Symbol, Type> types)
    {
        PlanNodeStatsEstimate innerJoinStats = computeInnerJoinStats(node, crossJoinStats, session, types);
        JoinComplementStats rightJoinComplementStats = calculateJoinComplementStats(node.getFilter(), flippedCriteria(node), rightStats, leftStats);
        return addJoinComplementStats(
                crossJoinStats,
                innerJoinStats,
                rightJoinComplementStats,
                getDrivingClauseSymbolMaxNdvs(rightJoinComplementStats, rightStats));
    }

    private double getDrivingClauseSymbolMaxNdvs(JoinComplementStats joinComplementStats, PlanNodeStatsEstimate stats)
    {
        return joinComplementStats.getDrivingClauseSymbol().map(symbol -> stats.getSymbolStatistics(symbol).getDistinctValuesCount()).orElse(NaN);
    }

    private PlanNodeStatsEstimate computeInnerJoinStats(JoinNode node, PlanNodeStatsEstimate crossJoinStats, Session session, Map<Symbol, Type> types)
    {
        List<EquiJoinClause> equiJoinClauses = node.getCriteria();

        PlanNodeStatsEstimate equiJoinClausesFilteredStats = IntStream.range(0, equiJoinClauses.size())
                .mapToObj(drivingClauseId -> {
                    EquiJoinClause drivingClause = equiJoinClauses.get(drivingClauseId);
                    List<EquiJoinClause> remainingClauses = copyWithout(equiJoinClauses, drivingClauseId);
                    return filterByEquiJoinClauses(crossJoinStats, drivingClause, remainingClauses, session, types);
                })
                .min(comparingDouble(PlanNodeStatsEstimate::getOutputRowCount))
                .orElse(crossJoinStats);

        return node.getFilter()
                .map(filter -> filterStatsCalculator.filterStats(equiJoinClausesFilteredStats, filter, session, types))
                .orElse(equiJoinClausesFilteredStats);
    }

    private static <T> List<T> copyWithout(List<? extends T> list, int filteredOutIndex)
    {
        List<T> copy = new ArrayList<>(list);
        copy.remove(filteredOutIndex);
        return copy;
    }

    private PlanNodeStatsEstimate filterByEquiJoinClauses(
            PlanNodeStatsEstimate stats,
            EquiJoinClause drivingClause,
            List<EquiJoinClause> auxiliaryClauses,
            Session session,
            Map<Symbol, Type> types)
    {
        ComparisonExpression drivingPredicate = new ComparisonExpression(EQUAL, drivingClause.getLeft().toSymbolReference(), drivingClause.getRight().toSymbolReference());
        PlanNodeStatsEstimate filteredStats = filterStatsCalculator.filterStats(stats, drivingPredicate, session, types);
        for (EquiJoinClause clause : auxiliaryClauses) {
            filteredStats = filterByAuxiliaryClause(filteredStats, clause);
        }
        return filteredStats;
    }

    private PlanNodeStatsEstimate filterByAuxiliaryClause(PlanNodeStatsEstimate stats, EquiJoinClause clause)
    {
        // we just clear null fraction and adjust ranges here
        // selectivity is mostly handled by driving clause. We just scale heuristically by UNKNOWN_FILTER_COEFFICIENT here.

        SymbolStatsEstimate leftStats = stats.getSymbolStatistics(clause.getLeft());
        SymbolStatsEstimate rightStats = stats.getSymbolStatistics(clause.getRight());
        StatisticRange leftRange = StatisticRange.from(leftStats);
        StatisticRange rightRange = StatisticRange.from(rightStats);

        StatisticRange intersect = leftRange.intersect(rightRange);
        double leftFilterValue = firstNonNaN(leftRange.overlapPercentWith(intersect), 1);
        double rightFilterValue = firstNonNaN(rightRange.overlapPercentWith(intersect), 1);
        double leftNdvInRange = leftFilterValue * leftRange.getDistinctValuesCount();
        double rightNdvInRange = rightFilterValue * rightRange.getDistinctValuesCount();
        double retainedNdv = MoreMath.min(leftNdvInRange, rightNdvInRange);

        SymbolStatsEstimate newLeftStats = buildFrom(leftStats)
                .setNullsFraction(0)
                .setStatisticsRange(intersect)
                .setDistinctValuesCount(retainedNdv)
                .build();

        SymbolStatsEstimate newRightStats = buildFrom(rightStats)
                .setNullsFraction(0)
                .setStatisticsRange(intersect)
                .setDistinctValuesCount(retainedNdv)
                .build();

        return stats
                .mapSymbolColumnStatistics(clause.getLeft(), oldLeftStats -> newLeftStats)
                .mapSymbolColumnStatistics(clause.getRight(), oldRightStats -> newRightStats)
                .mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT);
    }

    private static double firstNonNaN(double... values)
    {
        for (double value : values) {
            if (!isNaN(value)) {
                return value;
            }
        }
        throw new IllegalArgumentException("All values are NaN");
    }

    @VisibleForTesting
    JoinComplementStats calculateJoinComplementStats(
            Optional<Expression> filter,
            List<JoinNode.EquiJoinClause> criteria,
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats)
    {
        // TODO: add support for non-equality conditions (e.g: <=, !=, >)
        if (filter.isPresent()) {
            // non-equi filters are not supported
            return new JoinComplementStats(UNKNOWN_STATS, Optional.empty());
        }

        if (criteria.isEmpty()) {
            if (rightStats.getOutputRowCount() > 0) {
                // all left side rows are matched
                return new JoinComplementStats(leftStats.mapOutputRowCount(rowCount -> 0.0), Optional.empty());
            }
            if (rightStats.getOutputRowCount() == 0) {
                // none left side rows are matched
                return new JoinComplementStats(leftStats, Optional.empty());
            }
            // right stats row count is NaN
            return new JoinComplementStats(UNKNOWN_STATS, Optional.empty());
        }

        // heuristics: select the most selective criteria for join complement clause
        return IntStream.range(0, criteria.size())
                .mapToObj(drivingClauseId -> {
                    EquiJoinClause drivingClause = criteria.get(drivingClauseId);
                    List<EquiJoinClause> remainingClauses = copyWithout(criteria, drivingClauseId);
                    return calculateJoinComplementStats(leftStats, rightStats, drivingClause, remainingClauses);
                })
                .max(comparingDouble(joinComplementStats -> joinComplementStats.getStats().getOutputRowCount()))
                .get();
    }

    private JoinComplementStats calculateJoinComplementStats(
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            EquiJoinClause drivingClause,
            List<EquiJoinClause> remainingClauses)
    {
        PlanNodeStatsEstimate result = leftStats;

        SymbolStatsEstimate leftColumnStats = leftStats.getSymbolStatistics(drivingClause.getLeft());
        SymbolStatsEstimate rightColumnStats = rightStats.getSymbolStatistics(drivingClause.getRight());

        // TODO: use range methods when they have defined (and consistent) semantics
        double leftNDV = leftColumnStats.getDistinctValuesCount();
        double matchingRightNDV = rightColumnStats.getDistinctValuesCount() * unmatchedJoinComplementNdvsCoefficient;

        if (leftNDV > matchingRightNDV) {
            double drivingClauseRangeFraction = leftColumnStats.getValuesFraction() * (leftNDV - matchingRightNDV) / leftNDV;
            double scaleFactor = drivingClauseRangeFraction + leftColumnStats.getNullsFraction();
            double newLeftNullsFraction = leftColumnStats.getNullsFraction() / scaleFactor;
            result = result.mapSymbolColumnStatistics(drivingClause.getLeft(), columnStats ->
                    SymbolStatsEstimate.buildFrom(columnStats)
                            .setLowValue(leftColumnStats.getLowValue())
                            .setHighValue(leftColumnStats.getHighValue())
                            .setNullsFraction(newLeftNullsFraction)
                            .setDistinctValuesCount(leftNDV - matchingRightNDV)
                            .build());
            result = result.mapOutputRowCount(rowCount -> rowCount * scaleFactor);
        }
        else if (leftNDV <= matchingRightNDV) {
            // only null values are left
            result = result.mapSymbolColumnStatistics(drivingClause.getLeft(), columnStats ->
                    SymbolStatsEstimate.buildFrom(columnStats)
                            .setLowValue(NaN)
                            .setHighValue(NaN)
                            .setNullsFraction(1.0)
                            .setDistinctValuesCount(0.0)
                            .build());
            result = result.mapOutputRowCount(rowCount -> rowCount * leftColumnStats.getNullsFraction());
        }
        else {
            // either leftNDV or rightNDV is NaN
            return new JoinComplementStats(UNKNOWN_STATS, Optional.empty());
        }

        // account for remaining clauses
        for (int i = 0; i < remainingClauses.size(); ++i) {
            result = result.mapOutputRowCount(rowCount -> min(leftStats.getOutputRowCount(), rowCount / UNKNOWN_FILTER_COEFFICIENT));
        }

        return new JoinComplementStats(result, Optional.of(drivingClause.getLeft()));
    }

    @VisibleForTesting
    PlanNodeStatsEstimate addJoinComplementStats(
            PlanNodeStatsEstimate crossJoinStats,
            PlanNodeStatsEstimate innerJoinStats,
            JoinComplementStats joinComplementStats,
            double drivingClauseSymbolMaxNdv)
    {
        checkState(innerJoinStats.getSymbolsWithKnownStatistics().containsAll(joinComplementStats.getStats().getSymbolsWithKnownStatistics()));

        double joinOutputRowCount = innerJoinStats.getOutputRowCount();
        double joinComplementOutputRowCount = joinComplementStats.getStats().getOutputRowCount();
        double totalRowCount = joinOutputRowCount + joinComplementOutputRowCount;

        PlanNodeStatsEstimate.Builder outputStats = PlanNodeStatsEstimate.buildFrom(innerJoinStats);
        outputStats.setOutputRowCount(joinOutputRowCount + joinComplementOutputRowCount);

        for (Symbol symbol : joinComplementStats.getStats().getSymbolsWithKnownStatistics()) {
            SymbolStatsEstimate innerSymbolStats = innerJoinStats.getSymbolStatistics(symbol);
            SymbolStatsEstimate joinComplementColumnStats = joinComplementStats.getStats().getSymbolStatistics(symbol);
            SymbolStatsEstimate crossSymbolStats = crossJoinStats.getSymbolStatistics(symbol);

            // weighted average
            double newNullsFraction = (innerSymbolStats.getNullsFraction() * joinOutputRowCount + joinComplementColumnStats.getNullsFraction() * joinComplementOutputRowCount) / totalRowCount;

            double distinctValues;
            if (joinComplementStats.getDrivingClauseSymbol().isPresent() && joinComplementStats.getDrivingClauseSymbol().get().equals(symbol)) {
                distinctValues = min(innerSymbolStats.getDistinctValuesCount() + joinComplementColumnStats.getDistinctValuesCount(), drivingClauseSymbolMaxNdv);
            }
            else {
                distinctValues = firstNonNaN(
                        min(innerSymbolStats.getDistinctValuesCount() + joinComplementColumnStats.getDistinctValuesCount(), crossSymbolStats.getDistinctValuesCount()),
                        innerSymbolStats.getDistinctValuesCount(),
                        joinComplementColumnStats.getDistinctValuesCount(),
                        crossSymbolStats.getDistinctValuesCount());
            }

            outputStats.addSymbolStatistics(symbol, SymbolStatsEstimate.buildFrom(innerSymbolStats)
                    .setLowValue(rangeMin(innerSymbolStats.getLowValue(), joinComplementColumnStats.getLowValue()))
                    .setHighValue(rangeMax(innerSymbolStats.getHighValue(), joinComplementColumnStats.getHighValue()))
                    .setDistinctValuesCount(distinctValues)
                    .setNullsFraction(newNullsFraction)
                    .build());
        }

        // add nulls to columns that don't exist in right stats
        for (Symbol symbol : difference(innerJoinStats.getSymbolsWithKnownStatistics(), joinComplementStats.getStats().getSymbolsWithKnownStatistics())) {
            SymbolStatsEstimate innerSymbolStats = innerJoinStats.getSymbolStatistics(symbol);
            double newNullsFraction = (innerSymbolStats.getNullsFraction() * joinOutputRowCount + joinComplementOutputRowCount) / totalRowCount;
            outputStats.addSymbolStatistics(symbol, innerSymbolStats.mapNullsFraction(nullsFraction -> newNullsFraction));
        }

        return outputStats.build();
    }

    private PlanNodeStatsEstimate crossJoinStats(JoinNode node, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(leftStats.getOutputRowCount() * rightStats.getOutputRowCount());

        node.getLeft().getOutputSymbols().forEach(symbol -> builder.addSymbolStatistics(symbol, leftStats.getSymbolStatistics(symbol)));
        node.getRight().getOutputSymbols().forEach(symbol -> builder.addSymbolStatistics(symbol, rightStats.getSymbolStatistics(symbol)));

        return builder.build();
    }

    private List<JoinNode.EquiJoinClause> flippedCriteria(JoinNode node)
    {
        return node.getCriteria().stream()
                .map(criteria -> new JoinNode.EquiJoinClause(criteria.getRight(), criteria.getLeft()))
                .collect(toImmutableList());
    }

    @VisibleForTesting
    static class JoinComplementStats
    {
        private final PlanNodeStatsEstimate stats;
        private final Optional<Symbol> selectedClauseSymbol;

        JoinComplementStats(PlanNodeStatsEstimate stats, Optional<Symbol> selectedClauseSymbol)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.selectedClauseSymbol = requireNonNull(selectedClauseSymbol, "selectedClauseSymbol is null");
        }

        PlanNodeStatsEstimate getStats()
        {
            return stats;
        }

        Optional<Symbol> getDrivingClauseSymbol()
        {
            return selectedClauseSymbol;
        }
    }
}
