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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.util.MoreMath.rangeMax;
import static com.facebook.presto.util.MoreMath.rangeMin;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static java.lang.Double.NaN;

public class JoinStatsRule
        implements ComposableStatsCalculator.Rule
{
    private final FilterStatsCalculator filterStatsCalculator;

    public JoinStatsRule(FilterStatsCalculator filterStatsCalculator)
    {
        this.filterStatsCalculator = filterStatsCalculator;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(PlanNode node, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }
        JoinNode joinNode = (JoinNode) node;

        PlanNodeStatsEstimate leftStats = lookup.getStats(joinNode.getLeft(), session, types);
        PlanNodeStatsEstimate rightStats = lookup.getStats(joinNode.getRight(), session, types);

        switch (joinNode.getType()) {
            case INNER:
                return Optional.of(computeInnerJoinStats(joinNode, leftStats, rightStats, session, types));
            case LEFT:
                return Optional.of(computeLeftJoinStats(joinNode, leftStats, rightStats, session, types));
            case RIGHT:
                return Optional.of(computeRightJoinStats(joinNode, leftStats, rightStats, session, types));
            case FULL:
                return Optional.of(computeFullJoinStats(joinNode, leftStats, rightStats, session, types));
            default:
                return Optional.empty();
        }
    }

    private PlanNodeStatsEstimate computeFullJoinStats(JoinNode node, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats, Session session, Map<Symbol, Type> types)
    {
        PlanNodeStatsEstimate rightAntiJoinStats = calculateAntiJoinStats(node.getFilter(), flippedCriteria(node), rightStats, leftStats);
        return addAntiJoinStats(computeLeftJoinStats(node, leftStats, rightStats, session, types), rightAntiJoinStats, getRightJoinSymbols(node));
    }

    private PlanNodeStatsEstimate computeLeftJoinStats(JoinNode node, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats, Session session, Map<Symbol, Type> types)
    {
        PlanNodeStatsEstimate innerJoinStats = computeInnerJoinStats(node, leftStats, rightStats, session, types);
        PlanNodeStatsEstimate leftAntiJoinStats = calculateAntiJoinStats(node.getFilter(), node.getCriteria(), leftStats, rightStats);
        return addAntiJoinStats(innerJoinStats, leftAntiJoinStats, getLeftJoinSymbols(node));
    }

    private PlanNodeStatsEstimate computeRightJoinStats(JoinNode node, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats, Session session, Map<Symbol, Type> types)
    {
        PlanNodeStatsEstimate innerJoinStats = computeInnerJoinStats(node, leftStats, rightStats, session, types);
        PlanNodeStatsEstimate rightAntiJoinStats = calculateAntiJoinStats(node.getFilter(), flippedCriteria(node), rightStats, leftStats);
        return addAntiJoinStats(innerJoinStats, rightAntiJoinStats, getRightJoinSymbols(node));
    }

    private PlanNodeStatsEstimate computeInnerJoinStats(JoinNode node, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats, Session session, Map<Symbol, Type> types)
    {
        List<Expression> comparisons = node.getCriteria().stream()
                .map(criteria -> new ComparisonExpression(EQUAL, criteria.getLeft().toSymbolReference(), criteria.getRight().toSymbolReference()))
                .collect(toImmutableList());
        Expression predicate = combineConjuncts(combineConjuncts(comparisons), node.getFilter().orElse(TRUE_LITERAL));
        PlanNodeStatsEstimate crossJoinStats = crossJoinStats(node, leftStats, rightStats);
        return filterStatsCalculator.filterStats(crossJoinStats, predicate, session, types);
    }

    @VisibleForTesting
    PlanNodeStatsEstimate calculateAntiJoinStats(Optional<Expression> filter, List<JoinNode.EquiJoinClause> criteria, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats)
    {
        // TODO: add support for non-equality conditions (e.g: <=, !=, >)
        if (filter.isPresent()) {
            // non-equi filters are not supported
            return UNKNOWN_STATS;
        }

        PlanNodeStatsEstimate outputStats = leftStats;

        for (EquiJoinClause clause : criteria) {
            SymbolStatsEstimate leftColumnStats = leftStats.getSymbolStatistics(clause.getLeft());
            SymbolStatsEstimate rightColumnStats = rightStats.getSymbolStatistics(clause.getRight());

            StatisticRange rightRange = new StatisticRange(rightColumnStats.getLowValue(), rightColumnStats.getHighValue(), rightColumnStats.getDistinctValuesCount());
            StatisticRange antiRange = new StatisticRange(leftColumnStats.getLowValue(), leftColumnStats.getHighValue(), leftColumnStats.getDistinctValuesCount()).subtract(rightRange);

            // TODO: use NDVs from left and right StatisticRange when they are fixed
            double leftNDV = leftColumnStats.getDistinctValuesCount();
            double rightNDV = rightColumnStats.getDistinctValuesCount();

            if (leftNDV > rightNDV) {
                double selectedRangeFraction = leftColumnStats.getValuesFraction() * (leftNDV - rightNDV) / leftNDV;
                double scaleFactor = selectedRangeFraction + leftColumnStats.getNullsFraction();
                double newLeftNullsFraction = leftColumnStats.getNullsFraction() / scaleFactor;
                outputStats = outputStats.mapSymbolColumnStatistics(clause.getLeft(), columnStats ->
                        SymbolStatsEstimate.buildFrom(columnStats)
                                .setLowValue(antiRange.getLow())
                                .setHighValue(antiRange.getHigh())
                                .setNullsFraction(newLeftNullsFraction)
                                .setDistinctValuesCount(leftNDV - rightNDV)
                                .build());
                outputStats = outputStats.mapOutputRowCount(rowCount -> rowCount * scaleFactor);
            }
            else if (leftNDV <= rightNDV) {
                // only null values are left
                outputStats = outputStats.mapSymbolColumnStatistics(clause.getLeft(), columnStats ->
                        SymbolStatsEstimate.buildFrom(columnStats)
                                .setLowValue(NaN)
                                .setHighValue(NaN)
                                .setNullsFraction(1.0)
                                .setDistinctValuesCount(0.0)
                                .build());
                outputStats = outputStats.mapOutputRowCount(rowCount -> rowCount * leftColumnStats.getNullsFraction());
            }
            else {
                // either leftNDV or rightNDV is NaN
                return UNKNOWN_STATS;
            }
        }

        return outputStats;
    }

    @VisibleForTesting
    PlanNodeStatsEstimate addAntiJoinStats(PlanNodeStatsEstimate joinStats, PlanNodeStatsEstimate antiJoinStats, Set<Symbol> joinSymbols)
    {
        checkState(joinStats.getSymbolsWithKnownStatistics().containsAll(antiJoinStats.getSymbolsWithKnownStatistics()));

        double joinOutputRowCount = joinStats.getOutputRowCount();
        double antiJoinOutputRowCount = antiJoinStats.getOutputRowCount();
        double totalRowCount = joinOutputRowCount + antiJoinOutputRowCount;
        PlanNodeStatsEstimate outputStats = joinStats.mapOutputRowCount(rowCount -> rowCount + antiJoinOutputRowCount);

        for (Symbol symbol : antiJoinStats.getSymbolsWithKnownStatistics()) {
            outputStats = outputStats.mapSymbolColumnStatistics(symbol, joinColumnStats -> {
                SymbolStatsEstimate antiJoinColumnStats = antiJoinStats.getSymbolStatistics(symbol);
                // weighted average
                double newNullsFraction = (joinColumnStats.getNullsFraction() * joinOutputRowCount + antiJoinColumnStats.getNullsFraction() * antiJoinOutputRowCount) / totalRowCount;
                double distinctValues;
                if (joinSymbols.contains(symbol)) {
                    distinctValues = joinColumnStats.getDistinctValuesCount() + antiJoinColumnStats.getDistinctValuesCount();
                }
                else {
                    distinctValues = joinColumnStats.getDistinctValuesCount();
                }
                return SymbolStatsEstimate.buildFrom(joinColumnStats)
                        .setLowValue(rangeMin(joinColumnStats.getLowValue(), antiJoinColumnStats.getLowValue()))
                        .setHighValue(rangeMax(joinColumnStats.getHighValue(), antiJoinColumnStats.getHighValue()))
                        .setDistinctValuesCount(distinctValues)
                        .setNullsFraction(newNullsFraction)
                        .build();
            });
        }

        // add nulls to columns that don't exist in right stats
        for (Symbol symbol : difference(joinStats.getSymbolsWithKnownStatistics(), antiJoinStats.getSymbolsWithKnownStatistics())) {
            outputStats = outputStats.mapSymbolColumnStatistics(symbol, joinColumnStats ->
                    joinColumnStats.mapNullsFraction(nullsFraction -> (nullsFraction * joinOutputRowCount + antiJoinOutputRowCount) / totalRowCount));
        }

        return outputStats;
    }

    private PlanNodeStatsEstimate crossJoinStats(JoinNode node, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(leftStats.getOutputRowCount() * rightStats.getOutputRowCount());

        node.getLeft().getOutputSymbols().forEach(symbol -> builder.addSymbolStatistics(symbol, leftStats.getSymbolStatistics(symbol)));
        node.getRight().getOutputSymbols().forEach(symbol -> builder.addSymbolStatistics(symbol, rightStats.getSymbolStatistics(symbol)));

        return builder.build();
    }

    private Set<Symbol> getLeftJoinSymbols(JoinNode node)
    {
        return node.getCriteria().stream()
                .map(EquiJoinClause::getLeft)
                .collect(toImmutableSet());
    }

    private Set<Symbol> getRightJoinSymbols(JoinNode node)
    {
        return node.getCriteria().stream()
                .map(EquiJoinClause::getRight)
                .collect(toImmutableSet());
    }

    private List<JoinNode.EquiJoinClause> flippedCriteria(JoinNode node)
    {
        return node.getCriteria().stream()
                .map(criteria -> new JoinNode.EquiJoinClause(criteria.getRight(), criteria.getLeft()))
                .collect(toImmutableList());
    }
}
