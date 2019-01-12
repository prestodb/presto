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
package io.prestosql.cost;

import com.google.common.annotations.VisibleForTesting;
import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.JoinNode.EquiJoinClause;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.util.MoreMath;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Sets.difference;
import static io.prestosql.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.prestosql.cost.SymbolStatsEstimate.buildFrom;
import static io.prestosql.sql.ExpressionUtils.extractConjuncts;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
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
    private final StatsNormalizer normalizer;
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
        this.normalizer = normalizer;
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
        PlanNodeStatsEstimate crossJoinStats = crossJoinStats(node, leftStats, rightStats, types);

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
        PlanNodeStatsEstimate rightJoinComplementStats = calculateJoinComplementStats(node.getFilter(), flippedCriteria(node), rightStats, leftStats, types);
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
        PlanNodeStatsEstimate leftJoinComplementStats = calculateJoinComplementStats(node.getFilter(), node.getCriteria(), leftStats, rightStats, types);
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
        PlanNodeStatsEstimate rightJoinComplementStats = calculateJoinComplementStats(node.getFilter(), flippedCriteria(node), rightStats, leftStats, types);
        return addJoinComplementStats(
                rightStats,
                innerJoinStats,
                rightJoinComplementStats);
    }

    private PlanNodeStatsEstimate computeInnerJoinStats(JoinNode node, PlanNodeStatsEstimate crossJoinStats, Session session, TypeProvider types)
    {
        List<EquiJoinClause> equiJoinCriteria = node.getCriteria();

        if (equiJoinCriteria.isEmpty()) {
            if (!node.getFilter().isPresent()) {
                return crossJoinStats;
            }
            // TODO: this might explode stats
            return filterStatsCalculator.filterStats(crossJoinStats, node.getFilter().get(), session, types);
        }

        PlanNodeStatsEstimate equiJoinEstimate = filterByEquiJoinClauses(crossJoinStats, node.getCriteria(), session, types);

        if (equiJoinEstimate.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        if (!node.getFilter().isPresent()) {
            return equiJoinEstimate;
        }

        PlanNodeStatsEstimate filteredEquiJoinEstimate = filterStatsCalculator.filterStats(equiJoinEstimate, node.getFilter().get(), session, types);

        if (filteredEquiJoinEstimate.isOutputRowCountUnknown()) {
            return normalizer.normalize(equiJoinEstimate.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT), types);
        }

        return filteredEquiJoinEstimate;
    }

    private PlanNodeStatsEstimate filterByEquiJoinClauses(
            PlanNodeStatsEstimate stats,
            Collection<EquiJoinClause> clauses,
            Session session,
            TypeProvider types)
    {
        checkArgument(!clauses.isEmpty(), "clauses is empty");
        PlanNodeStatsEstimate result = PlanNodeStatsEstimate.unknown();
        // Join equality clauses are usually correlated. Therefore we shouldn't treat each join equality
        // clause separately because stats estimates would be way off. Instead we choose so called
        // "driving clause" which mostly reduces join output rows cardinality and apply UNKNOWN_FILTER_COEFFICIENT
        // for other (auxiliary) clauses.
        Queue<EquiJoinClause> remainingClauses = new LinkedList<>(clauses);
        EquiJoinClause drivingClause = remainingClauses.poll();
        for (int i = 0; i < clauses.size(); i++) {
            PlanNodeStatsEstimate estimate = filterByEquiJoinClauses(stats, drivingClause, remainingClauses, session, types);
            if (result.isOutputRowCountUnknown() || (!estimate.isOutputRowCountUnknown() && estimate.getOutputRowCount() < result.getOutputRowCount())) {
                result = estimate;
            }
            remainingClauses.add(drivingClause);
            drivingClause = remainingClauses.poll();
        }

        return result;
    }

    private PlanNodeStatsEstimate filterByEquiJoinClauses(
            PlanNodeStatsEstimate stats,
            EquiJoinClause drivingClause,
            Collection<EquiJoinClause> remainingClauses,
            Session session,
            TypeProvider types)
    {
        ComparisonExpression drivingPredicate = new ComparisonExpression(EQUAL, drivingClause.getLeft().toSymbolReference(), drivingClause.getRight().toSymbolReference());
        PlanNodeStatsEstimate filteredStats = filterStatsCalculator.filterStats(stats, drivingPredicate, session, types);
        for (EquiJoinClause clause : remainingClauses) {
            filteredStats = filterByAuxiliaryClause(filteredStats, clause, types);
        }
        return filteredStats;
    }

    private PlanNodeStatsEstimate filterByAuxiliaryClause(PlanNodeStatsEstimate stats, EquiJoinClause clause, TypeProvider types)
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

        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(stats)
                .setOutputRowCount(stats.getOutputRowCount() * UNKNOWN_FILTER_COEFFICIENT)
                .addSymbolStatistics(clause.getLeft(), newLeftStats)
                .addSymbolStatistics(clause.getRight(), newRightStats);
        return normalizer.normalize(result.build(), types);
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
            PlanNodeStatsEstimate rightStats,
            TypeProvider types)
    {
        if (rightStats.getOutputRowCount() == 0) {
            // no left side rows are matched
            return leftStats;
        }

        if (criteria.isEmpty()) {
            // TODO: account for non-equi conditions
            if (filter.isPresent()) {
                return PlanNodeStatsEstimate.unknown();
            }

            return normalizer.normalize(leftStats.mapOutputRowCount(rowCount -> 0.0), types);
        }

        // TODO: add support for non-equality conditions (e.g: <=, !=, >)
        int numberOfFilterClauses = filter.map(expression -> extractConjuncts(expression).size()).orElse(0);

        // Heuristics: select the most selective criteria for join complement clause.
        // Principals behind this heuristics is the same as in computeInnerJoinStats:
        // select "driving join clause" that reduces matched rows the most.
        return criteria.stream()
                .map(drivingClause -> calculateJoinComplementStats(leftStats, rightStats, drivingClause, criteria.size() - 1 + numberOfFilterClauses))
                .filter(estimate -> !estimate.isOutputRowCountUnknown())
                .max(comparingDouble(PlanNodeStatsEstimate::getOutputRowCount))
                .map(estimate -> normalizer.normalize(estimate, types))
                .orElse(PlanNodeStatsEstimate.unknown());
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
            return PlanNodeStatsEstimate.unknown();
        }

        // limit the number of complement rows (to left row count) and account for remaining clauses
        result = result.mapOutputRowCount(rowCount -> min(leftStats.getOutputRowCount(), rowCount / Math.pow(UNKNOWN_FILTER_COEFFICIENT, numberOfRemainingClauses)));

        return result;
    }

    @VisibleForTesting
    PlanNodeStatsEstimate addJoinComplementStats(
            PlanNodeStatsEstimate sourceStats,
            PlanNodeStatsEstimate innerJoinStats,
            PlanNodeStatsEstimate joinComplementStats)
    {
        double innerJoinRowCount = innerJoinStats.getOutputRowCount();
        double joinComplementRowCount = joinComplementStats.getOutputRowCount();
        if (joinComplementRowCount == 0) {
            return innerJoinStats;
        }

        double outputRowCount = innerJoinRowCount + joinComplementRowCount;

        PlanNodeStatsEstimate.Builder outputStats = PlanNodeStatsEstimate.buildFrom(innerJoinStats);
        outputStats.setOutputRowCount(outputRowCount);

        for (Symbol symbol : joinComplementStats.getSymbolsWithKnownStatistics()) {
            SymbolStatsEstimate leftSymbolStats = sourceStats.getSymbolStatistics(symbol);
            SymbolStatsEstimate innerJoinSymbolStats = innerJoinStats.getSymbolStatistics(symbol);
            SymbolStatsEstimate joinComplementSymbolStats = joinComplementStats.getSymbolStatistics(symbol);

            // weighted average
            double newNullsFraction = (innerJoinSymbolStats.getNullsFraction() * innerJoinRowCount + joinComplementSymbolStats.getNullsFraction() * joinComplementRowCount) / outputRowCount;
            outputStats.addSymbolStatistics(symbol, SymbolStatsEstimate.buildFrom(innerJoinSymbolStats)
                    // in outer join low value, high value and NDVs of outer side columns are preserved
                    .setLowValue(leftSymbolStats.getLowValue())
                    .setHighValue(leftSymbolStats.getHighValue())
                    .setDistinctValuesCount(leftSymbolStats.getDistinctValuesCount())
                    .setNullsFraction(newNullsFraction)
                    .build());
        }

        // add nulls to columns that don't exist in right stats
        for (Symbol symbol : difference(innerJoinStats.getSymbolsWithKnownStatistics(), joinComplementStats.getSymbolsWithKnownStatistics())) {
            SymbolStatsEstimate innerJoinSymbolStats = innerJoinStats.getSymbolStatistics(symbol);
            double newNullsFraction = (innerJoinSymbolStats.getNullsFraction() * innerJoinRowCount + joinComplementRowCount) / outputRowCount;
            outputStats.addSymbolStatistics(symbol, innerJoinSymbolStats.mapNullsFraction(nullsFraction -> newNullsFraction));
        }

        return outputStats.build();
    }

    private PlanNodeStatsEstimate crossJoinStats(JoinNode node, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats, TypeProvider types)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(leftStats.getOutputRowCount() * rightStats.getOutputRowCount());

        node.getLeft().getOutputSymbols().forEach(symbol -> builder.addSymbolStatistics(symbol, leftStats.getSymbolStatistics(symbol)));
        node.getRight().getOutputSymbols().forEach(symbol -> builder.addSymbolStatistics(symbol, rightStats.getSymbolStatistics(symbol)));

        return normalizer.normalize(builder.build(), types);
    }

    private List<JoinNode.EquiJoinClause> flippedCriteria(JoinNode node)
    {
        return node.getCriteria().stream()
                .map(EquiJoinClause::flip)
                .collect(toImmutableList());
    }
}
