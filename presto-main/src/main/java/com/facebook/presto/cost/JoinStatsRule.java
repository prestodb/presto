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
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.util.MoreMath;
import com.google.common.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.facebook.presto.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.cost.SymbolStatsEstimate.buildFrom;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.lang.Math.min;
import static java.util.Comparator.comparingDouble;
import static java.util.Objects.requireNonNull;

public class JoinStatsRule
        extends SimpleStatsRule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();
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
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(JoinNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate leftStats = sourceStats.getStats(node.getLeft());
        PlanNodeStatsEstimate rightStats = sourceStats.getStats(node.getRight());
        PlanNodeStatsEstimate crossJoinStats = crossJoinStats(node, leftStats, rightStats);

        switch (node.getType()) {
            case INNER:
                return Optional.of(computeInnerJoinStats(node, crossJoinStats, session, types));
            case LEFT:
                return Optional.of(computeLeftJoinStats(node, leftStats, rightStats, crossJoinStats, session, types));
            case RIGHT:
                return Optional.of(computeRightJoinStats(node, leftStats, rightStats, crossJoinStats, session, types));
            case FULL:
                return Optional.of(computeFullJoinStats(node, leftStats, rightStats, crossJoinStats, session, types));
            default:
                throw new IllegalStateException("Unknown join type: " + node.getType());
        }
    }

    private PlanNodeStatsEstimate computeFullJoinStats(
            JoinNode node,
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            PlanNodeStatsEstimate crossJoinStats,
            Session session,
            TypeProvider types)
    {
        PlanNodeStatsEstimate rightJoinComplementStats = calculateJoinComplementStats(node.getFilter(), flippedCriteria(node), rightStats, leftStats);
        return addJoinComplementStats(
                rightStats,
                computeLeftJoinStats(node, leftStats, rightStats, crossJoinStats, session, types),
                rightJoinComplementStats);
    }

    private PlanNodeStatsEstimate computeLeftJoinStats(
            JoinNode node,
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            PlanNodeStatsEstimate crossJoinStats,
            Session session,
            TypeProvider types)
    {
        PlanNodeStatsEstimate innerJoinStats = computeInnerJoinStats(node, crossJoinStats, session, types);
        PlanNodeStatsEstimate leftJoinComplementStats = calculateJoinComplementStats(node.getFilter(), node.getCriteria(), leftStats, rightStats);
        return addJoinComplementStats(
                leftStats,
                innerJoinStats,
                leftJoinComplementStats);
    }

    private PlanNodeStatsEstimate computeRightJoinStats(
            JoinNode node,
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            PlanNodeStatsEstimate crossJoinStats,
            Session session,
            TypeProvider types)
    {
        PlanNodeStatsEstimate innerJoinStats = computeInnerJoinStats(node, crossJoinStats, session, types);
        PlanNodeStatsEstimate rightJoinComplementStats = calculateJoinComplementStats(node.getFilter(), flippedCriteria(node), rightStats, leftStats);
        return addJoinComplementStats(
                rightStats,
                innerJoinStats,
                rightJoinComplementStats);
    }

    private PlanNodeStatsEstimate computeInnerJoinStats(JoinNode node, PlanNodeStatsEstimate crossJoinStats, Session session, TypeProvider types)
    {
        List<EquiJoinClause> equiJoinClauses = node.getCriteria();

        // Join equality clauses are usually correlated. Therefore we shouldn't treat each join equality
        // clause separately because stats estimates would be way off. Instead we choose so called
        // "driving clause" which mostly reduces join output rows cardinality and apply UNKNOWN_FILTER_COEFFICIENT
        // for other (auxiliary) clauses.
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
            TypeProvider types)
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

    /**
     * Calculates statistics for unmatched left rows.
     */
    @VisibleForTesting
    PlanNodeStatsEstimate calculateJoinComplementStats(
            Optional<Expression> filter,
            List<JoinNode.EquiJoinClause> criteria,
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats)
    {
        if (rightStats.getOutputRowCount() == 0) {
            // no left side rows are matched
            return leftStats;
        }

        if (criteria.isEmpty()) {
            // TODO: account for non-equi conditions
            if (filter.isPresent()) {
                return UNKNOWN_STATS;
            }

            return leftStats.mapOutputRowCount(rowCount -> 0.0);
        }

        // TODO: add support for non-equality conditions (e.g: <=, !=, >)
        int numberOfFilterClauses = filter.map(
                exression -> ExpressionUtils.extractConjuncts(exression).size())
                .orElse(0);

        // Heuristics: select the most selective criteria for join complement clause.
        // Principals behind this heuristics is the same as in computeInnerJoinStats:
        // select "driving join clause" that reduces matched rows the most.
        return IntStream.range(0, criteria.size())
                .mapToObj(drivingClauseId -> {
                    EquiJoinClause drivingClause = criteria.get(drivingClauseId);
                    return calculateJoinComplementStats(leftStats, rightStats, drivingClause, criteria.size() - 1 + numberOfFilterClauses);
                })
                .max(comparingDouble(PlanNodeStatsEstimate::getOutputRowCount))
                .get();
    }

    private PlanNodeStatsEstimate calculateJoinComplementStats(
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            EquiJoinClause drivingClause,
            int numberOfRemainingClauses)
    {
        PlanNodeStatsEstimate result = leftStats;

        SymbolStatsEstimate leftColumnStats = leftStats.getSymbolStatistics(drivingClause.getLeft());
        SymbolStatsEstimate rightColumnStats = rightStats.getSymbolStatistics(drivingClause.getRight());

        // TODO: use range methods when they have defined (and consistent) semantics
        double leftNDV = leftColumnStats.getDistinctValuesCount();
        double matchingRightNDV = rightColumnStats.getDistinctValuesCount() * unmatchedJoinComplementNdvsCoefficient;

        if (leftNDV > matchingRightNDV) {
            // Assume "excessive" left NDVs and left null rows are unmatched.
            double nonMatchingLeftValuesFraction = leftColumnStats.getValuesFraction() * (leftNDV - matchingRightNDV) / leftNDV;
            double scaleFactor = nonMatchingLeftValuesFraction + leftColumnStats.getNullsFraction();
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
            // Assume all non-null left rows are matched. Therefore only null left rows are unmatched.
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
            return UNKNOWN_STATS;
        }

        // limit the number of complement rows (to left row count) and account for remaining clauses
        result = result.mapOutputRowCount(rowCount -> min(leftStats.getOutputRowCount(), rowCount / Math.pow(UNKNOWN_FILTER_COEFFICIENT, numberOfRemainingClauses)));

        return result;
    }

    @VisibleForTesting
    PlanNodeStatsEstimate addJoinComplementStats(
            PlanNodeStatsEstimate sourceStats,
            PlanNodeStatsEstimate baseJoinStats,
            PlanNodeStatsEstimate joinComplementStats)
    {
        checkState(baseJoinStats.getSymbolsWithKnownStatistics().containsAll(joinComplementStats.getSymbolsWithKnownStatistics()));

        double joinOutputRowCount = baseJoinStats.getOutputRowCount();
        double joinComplementOutputRowCount = joinComplementStats.getOutputRowCount();
        double totalRowCount = joinOutputRowCount + joinComplementOutputRowCount;

        PlanNodeStatsEstimate.Builder outputStats = PlanNodeStatsEstimate.buildFrom(baseJoinStats);
        outputStats.setOutputRowCount(joinOutputRowCount + joinComplementOutputRowCount);

        for (Symbol symbol : joinComplementStats.getSymbolsWithKnownStatistics()) {
            SymbolStatsEstimate sourceSymbolStats = sourceStats.getSymbolStatistics(symbol);
            SymbolStatsEstimate innerSymbolStats = baseJoinStats.getSymbolStatistics(symbol);
            SymbolStatsEstimate joinComplementSymbolStats = joinComplementStats.getSymbolStatistics(symbol);

            // weighted average
            double newNullsFraction = (innerSymbolStats.getNullsFraction() * joinOutputRowCount + joinComplementSymbolStats.getNullsFraction() * joinComplementOutputRowCount) / totalRowCount;
            outputStats.addSymbolStatistics(symbol, SymbolStatsEstimate.buildFrom(innerSymbolStats)
                    // in outer join low value, high value and NDVs of outer side columns are preserved
                    .setLowValue(sourceSymbolStats.getLowValue())
                    .setHighValue(sourceSymbolStats.getHighValue())
                    .setDistinctValuesCount(sourceSymbolStats.getDistinctValuesCount())
                    .setNullsFraction(newNullsFraction)
                    .build());
        }

        // add nulls to columns that don't exist in right stats
        for (Symbol symbol : difference(baseJoinStats.getSymbolsWithKnownStatistics(), joinComplementStats.getSymbolsWithKnownStatistics())) {
            SymbolStatsEstimate innerSymbolStats = baseJoinStats.getSymbolStatistics(symbol);
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
                .map(EquiJoinClause::flip)
                .collect(toImmutableList());
    }
}
