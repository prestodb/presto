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

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static com.facebook.presto.cost.PlanNodeStatsAssertion.assertThat;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static java.lang.Double.NaN;

public class TestJoinStatsRule
        extends BaseStatsCalculatorTest
{
    private static final String LEFT_JOIN_COLUMN = "left_join_column";
    private static final String LEFT_JOIN_COLUMN_2 = "left_join_column_2";
    private static final String RIGHT_JOIN_COLUMN = "right_join_column";
    private static final String RIGHT_JOIN_COLUMN_2 = "right_join_column_2";
    private static final String LEFT_OTHER_COLUMN = "left_column";
    private static final String RIGHT_OTHER_COLUMN = "right_column";

    private static final double LEFT_ROWS_COUNT = 500.0;
    private static final double RIGHT_ROWS_COUNT = 1000.0;
    private static final double TOTAL_ROWS_COUNT = LEFT_ROWS_COUNT + RIGHT_ROWS_COUNT;
    private static final double LEFT_JOIN_COLUMN_NULLS = 0.3;
    private static final double LEFT_JOIN_COLUMN_2_NULLS = 0.4;
    private static final double LEFT_JOIN_COLUMN_NON_NULLS = 0.7;
    private static final double LEFT_JOIN_COLUMN_2_NON_NULLS = 1 - LEFT_JOIN_COLUMN_2_NULLS;
    private static final int LEFT_JOIN_COLUMN_NDV = 20;
    private static final int LEFT_JOIN_COLUMN_2_NDV = 50;
    private static final double RIGHT_JOIN_COLUMN_NULLS = 0.6;
    private static final double RIGHT_JOIN_COLUMN_2_NULLS = 0.8;
    private static final double RIGHT_JOIN_COLUMN_NON_NULLS = 0.4;
    private static final double RIGHT_JOIN_COLUMN_2_NON_NULLS = 1 - RIGHT_JOIN_COLUMN_2_NULLS;
    private static final int RIGHT_JOIN_COLUMN_NDV = 15;
    private static final int RIGHT_JOIN_COLUMN_2_NDV = 15;

    private static final SymbolStatistics LEFT_JOIN_COLUMN_STATS =
            symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, LEFT_JOIN_COLUMN_NULLS, LEFT_JOIN_COLUMN_NDV);
    private static final SymbolStatistics LEFT_JOIN_COLUMN_2_STATS =
            symbolStatistics(LEFT_JOIN_COLUMN_2, 0.0, 200.0, LEFT_JOIN_COLUMN_2_NULLS, LEFT_JOIN_COLUMN_2_NDV);
    private static final SymbolStatistics LEFT_OTHER_COLUMN_STATS =
            symbolStatistics(LEFT_OTHER_COLUMN, 42, 42, 0.42, 1);
    private static final SymbolStatistics RIGHT_JOIN_COLUMN_STATS =
            symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, RIGHT_JOIN_COLUMN_NULLS, RIGHT_JOIN_COLUMN_NDV);
    private static final SymbolStatistics RIGHT_JOIN_COLUMN_2_STATS =
            symbolStatistics(RIGHT_JOIN_COLUMN_2, 100.0, 200.0, RIGHT_JOIN_COLUMN_2_NULLS, RIGHT_JOIN_COLUMN_2_NDV);
    private static final SymbolStatistics RIGHT_OTHER_COLUMN_STATS =
            symbolStatistics(RIGHT_OTHER_COLUMN, 24, 24, 0.24, 1);
    private static final PlanNodeStatsEstimate LEFT_STATS = planNodeStats(LEFT_ROWS_COUNT,
            LEFT_JOIN_COLUMN_STATS,
            LEFT_OTHER_COLUMN_STATS);
    private static final PlanNodeStatsEstimate RIGHT_STATS = planNodeStats(RIGHT_ROWS_COUNT,
            RIGHT_JOIN_COLUMN_STATS,
            RIGHT_OTHER_COLUMN_STATS);

    private static final MetadataManager METADATA = createTestMetadataManager();
    private static final StatsNormalizer NORMALIZER = new StatsNormalizer();
    private static final JoinStatsRule JOIN_STATS_RULE = new JoinStatsRule(
            new FilterStatsCalculator(METADATA, new ScalarStatsCalculator(METADATA), NORMALIZER),
            NORMALIZER,
            1.0);

    @Test
    public void testStatsForInnerJoin()
    {
        double innerJoinRowCount = LEFT_ROWS_COUNT * RIGHT_ROWS_COUNT / LEFT_JOIN_COLUMN_NDV * LEFT_JOIN_COLUMN_NON_NULLS * RIGHT_JOIN_COLUMN_NON_NULLS;
        PlanNodeStatsEstimate innerJoinStats = planNodeStats(innerJoinRowCount,
                symbolStatistics(LEFT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                LEFT_OTHER_COLUMN_STATS, RIGHT_OTHER_COLUMN_STATS);

        assertJoinStats(INNER, LEFT_STATS, RIGHT_STATS, innerJoinStats);
    }

    @Test
    public void testStatsForInnerJoinWithRepeatedClause()
    {
        double innerJoinRowCount = LEFT_ROWS_COUNT * RIGHT_ROWS_COUNT / LEFT_JOIN_COLUMN_NDV * LEFT_JOIN_COLUMN_NON_NULLS * RIGHT_JOIN_COLUMN_NON_NULLS // driver join clause
                * UNKNOWN_FILTER_COEFFICIENT; // auxiliary join clause
        PlanNodeStatsEstimate innerJoinStats = planNodeStats(innerJoinRowCount,
                symbolStatistics(LEFT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                LEFT_OTHER_COLUMN_STATS, RIGHT_OTHER_COLUMN_STATS);

        tester().assertStatsFor(pb -> {
            Symbol leftJoinColumnSymbol = pb.symbol(LEFT_JOIN_COLUMN, BIGINT);
            Symbol rightJoinColumnSymbol = pb.symbol(RIGHT_JOIN_COLUMN, DOUBLE);
            Symbol leftOtherColumnSymbol = pb.symbol(LEFT_OTHER_COLUMN, BIGINT);
            Symbol rightOtherColumnSymbol = pb.symbol(RIGHT_OTHER_COLUMN, DOUBLE);
            return pb
                    .join(INNER, pb.values(leftJoinColumnSymbol, leftOtherColumnSymbol),
                            pb.values(rightJoinColumnSymbol, rightOtherColumnSymbol),
                            new EquiJoinClause(leftJoinColumnSymbol, rightJoinColumnSymbol), new EquiJoinClause(leftJoinColumnSymbol, rightJoinColumnSymbol));
        }).withSourceStats(0, LEFT_STATS)
                .withSourceStats(1, RIGHT_STATS)
                .check(stats -> stats.equalTo(innerJoinStats));
    }

    @Test
    public void testStatsForInnerJoinWithTwoEquiClauses()
    {
        double innerJoinRowCount =
                LEFT_ROWS_COUNT * RIGHT_ROWS_COUNT / LEFT_JOIN_COLUMN_2_NDV * LEFT_JOIN_COLUMN_2_NON_NULLS * RIGHT_JOIN_COLUMN_2_NON_NULLS // driver join clause
                        * UNKNOWN_FILTER_COEFFICIENT; // auxiliary join clause
        PlanNodeStatsEstimate innerJoinStats = planNodeStats(innerJoinRowCount,
                symbolStatistics(LEFT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                symbolStatistics(LEFT_JOIN_COLUMN_2, 100.0, 200.0, 0.0, RIGHT_JOIN_COLUMN_2_NDV),
                symbolStatistics(RIGHT_JOIN_COLUMN_2, 100.0, 200.0, 0.0, RIGHT_JOIN_COLUMN_2_NDV));

        tester().assertStatsFor(pb -> {
            Symbol leftJoinColumnSymbol = pb.symbol(LEFT_JOIN_COLUMN, BIGINT);
            Symbol rightJoinColumnSymbol = pb.symbol(RIGHT_JOIN_COLUMN, DOUBLE);
            Symbol leftJoinColumnSymbol2 = pb.symbol(LEFT_JOIN_COLUMN_2, BIGINT);
            Symbol rightJoinColumnSymbol2 = pb.symbol(RIGHT_JOIN_COLUMN_2, DOUBLE);
            return pb
                    .join(INNER, pb.values(leftJoinColumnSymbol, leftJoinColumnSymbol2),
                            pb.values(rightJoinColumnSymbol, rightJoinColumnSymbol2),
                            new EquiJoinClause(leftJoinColumnSymbol2, rightJoinColumnSymbol2), new EquiJoinClause(leftJoinColumnSymbol, rightJoinColumnSymbol));
        }).withSourceStats(0, planNodeStats(LEFT_ROWS_COUNT, LEFT_JOIN_COLUMN_STATS, LEFT_JOIN_COLUMN_2_STATS))
                .withSourceStats(1, planNodeStats(RIGHT_ROWS_COUNT, RIGHT_JOIN_COLUMN_STATS, RIGHT_JOIN_COLUMN_2_STATS))
                .check(stats -> stats.equalTo(innerJoinStats));
    }

    @Test
    public void testStatsForInnerJoinWithTwoEquiClausesAndNonEqualityFunction()
    {
        double innerJoinRowCount =
                LEFT_ROWS_COUNT * RIGHT_ROWS_COUNT / LEFT_JOIN_COLUMN_2_NDV * LEFT_JOIN_COLUMN_2_NON_NULLS * RIGHT_JOIN_COLUMN_2_NON_NULLS // driver join clause
                        * UNKNOWN_FILTER_COEFFICIENT // auxiliary join clause
                        * 0.3333333333; // LEFT_JOIN_COLUMN < 10 non equality filter
        PlanNodeStatsEstimate innerJoinStats = planNodeStats(innerJoinRowCount,
                symbolStatistics(LEFT_JOIN_COLUMN, 5.0, 10.0, 0.0, RIGHT_JOIN_COLUMN_NDV * 0.3333333333),
                symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                symbolStatistics(LEFT_JOIN_COLUMN_2, 100.0, 200.0, 0.0, RIGHT_JOIN_COLUMN_2_NDV),
                symbolStatistics(RIGHT_JOIN_COLUMN_2, 100.0, 200.0, 0.0, RIGHT_JOIN_COLUMN_2_NDV));

        tester().assertStatsFor(pb -> {
            Symbol leftJoinColumnSymbol = pb.symbol(LEFT_JOIN_COLUMN, BIGINT);
            Symbol rightJoinColumnSymbol = pb.symbol(RIGHT_JOIN_COLUMN, DOUBLE);
            Symbol leftJoinColumnSymbol2 = pb.symbol(LEFT_JOIN_COLUMN_2, BIGINT);
            Symbol rightJoinColumnSymbol2 = pb.symbol(RIGHT_JOIN_COLUMN_2, DOUBLE);
            ComparisonExpression leftJoinColumnLessThanTen = new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, leftJoinColumnSymbol.toSymbolReference(), new LongLiteral("10"));
            return pb
                    .join(INNER, pb.values(leftJoinColumnSymbol, leftJoinColumnSymbol2),
                            pb.values(rightJoinColumnSymbol, rightJoinColumnSymbol2),
                            ImmutableList.of(new EquiJoinClause(leftJoinColumnSymbol2, rightJoinColumnSymbol2), new EquiJoinClause(leftJoinColumnSymbol, rightJoinColumnSymbol)),
                            ImmutableList.of(leftJoinColumnSymbol, leftJoinColumnSymbol2, rightJoinColumnSymbol, rightJoinColumnSymbol2),
                            Optional.of(leftJoinColumnLessThanTen));
        }).withSourceStats(0, planNodeStats(LEFT_ROWS_COUNT, LEFT_JOIN_COLUMN_STATS, LEFT_JOIN_COLUMN_2_STATS))
                .withSourceStats(1, planNodeStats(RIGHT_ROWS_COUNT, RIGHT_JOIN_COLUMN_STATS, RIGHT_JOIN_COLUMN_2_STATS))
                .check(stats -> stats.equalTo(innerJoinStats));
    }

    @Test
    public void testJoinComplementStats()
    {
        PlanNodeStatsEstimate joinComplementStats = planNodeStats(LEFT_ROWS_COUNT * (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4),
                symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, LEFT_JOIN_COLUMN_NULLS / (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4), 5),
                LEFT_OTHER_COLUMN_STATS);

        assertThat(JOIN_STATS_RULE.calculateJoinComplementStats(
                Optional.empty(),
                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(LEFT_JOIN_COLUMN), new Symbol(RIGHT_JOIN_COLUMN))),
                LEFT_STATS, RIGHT_STATS)).equalTo(joinComplementStats);
    }

    @Test
    public void testRightJoinComplementStats()
    {
        PlanNodeStatsEstimate joinComplementStats = planNodeStats(RIGHT_ROWS_COUNT * RIGHT_JOIN_COLUMN_NULLS,
                symbolStatistics(RIGHT_JOIN_COLUMN, NaN, NaN, 1.0, 0),
                RIGHT_OTHER_COLUMN_STATS);

        assertThat(JOIN_STATS_RULE.calculateJoinComplementStats(
                Optional.empty(),
                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(RIGHT_JOIN_COLUMN), new Symbol(LEFT_JOIN_COLUMN))),
                RIGHT_STATS, LEFT_STATS)).equalTo(joinComplementStats);
    }

    @Test
    public void testLeftJoinComplementStatsWithNoClauses()
    {
        assertThat(JOIN_STATS_RULE.calculateJoinComplementStats(
                Optional.empty(),
                ImmutableList.of(),
                LEFT_STATS, RIGHT_STATS)).equalTo(LEFT_STATS.mapOutputRowCount(rowCount -> 0.0));
    }

    @Test
    public void testLeftJoinComplementStatsWithMultipleClauses()
    {
        PlanNodeStatsEstimate joinComplementStats = planNodeStats(LEFT_ROWS_COUNT * (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4),
                symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, LEFT_JOIN_COLUMN_NULLS / (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4), 5),
                LEFT_OTHER_COLUMN_STATS).mapOutputRowCount(rowCount -> rowCount / UNKNOWN_FILTER_COEFFICIENT);

        assertThat(JOIN_STATS_RULE.calculateJoinComplementStats(
                Optional.empty(),
                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(LEFT_JOIN_COLUMN), new Symbol(RIGHT_JOIN_COLUMN)), new JoinNode.EquiJoinClause(new Symbol(LEFT_OTHER_COLUMN), new Symbol(RIGHT_OTHER_COLUMN))),
                LEFT_STATS, RIGHT_STATS)).equalTo(joinComplementStats);
    }

    @Test
    public void testStatsForLeftAndRightJoin()
    {
        double innerJoinRowCount = LEFT_ROWS_COUNT * RIGHT_ROWS_COUNT / LEFT_JOIN_COLUMN_NDV * LEFT_JOIN_COLUMN_NON_NULLS * RIGHT_JOIN_COLUMN_NON_NULLS;
        double joinComplementRowCount = LEFT_ROWS_COUNT * (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4);
        double joinComplementColumnNulls = LEFT_JOIN_COLUMN_NULLS / (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4);
        double totalRowCount = innerJoinRowCount + joinComplementRowCount;

        PlanNodeStatsEstimate leftJoinStats = planNodeStats(
                totalRowCount,
                symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, joinComplementColumnNulls * joinComplementRowCount / totalRowCount, LEFT_JOIN_COLUMN_NDV),
                LEFT_OTHER_COLUMN_STATS,
                symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, joinComplementRowCount / totalRowCount, RIGHT_JOIN_COLUMN_NDV),
                symbolStatistics(RIGHT_OTHER_COLUMN, 24, 24, (0.24 * innerJoinRowCount + joinComplementRowCount) / totalRowCount, 1));

        assertJoinStats(LEFT, LEFT_STATS, RIGHT_STATS, leftJoinStats);
        assertJoinStats(RIGHT, RIGHT_JOIN_COLUMN, RIGHT_OTHER_COLUMN, LEFT_JOIN_COLUMN, LEFT_OTHER_COLUMN, RIGHT_STATS, LEFT_STATS, leftJoinStats);
    }

    @Test
    public void testStatsForFullJoin()
    {
        double innerJoinRowCount = LEFT_ROWS_COUNT * RIGHT_ROWS_COUNT / LEFT_JOIN_COLUMN_NDV * LEFT_JOIN_COLUMN_NON_NULLS * RIGHT_JOIN_COLUMN_NON_NULLS;
        double leftJoinComplementRowCount = LEFT_ROWS_COUNT * (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4);
        double leftJoinComplementColumnNulls = LEFT_JOIN_COLUMN_NULLS / (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4);
        double rightJoinComplementRowCount = RIGHT_ROWS_COUNT * RIGHT_JOIN_COLUMN_NULLS;
        double rightJoinComplementColumnNulls = 1.0;
        double totalRowCount = innerJoinRowCount + leftJoinComplementRowCount + rightJoinComplementRowCount;

        PlanNodeStatsEstimate leftJoinStats = planNodeStats(
                totalRowCount,
                symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, (leftJoinComplementColumnNulls * leftJoinComplementRowCount + rightJoinComplementRowCount) / totalRowCount, LEFT_JOIN_COLUMN_NDV),
                symbolStatistics(LEFT_OTHER_COLUMN, 42, 42, (0.42 * (innerJoinRowCount + leftJoinComplementRowCount) + rightJoinComplementRowCount) / totalRowCount, 1),
                symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, (rightJoinComplementColumnNulls * rightJoinComplementRowCount + leftJoinComplementRowCount) / totalRowCount, RIGHT_JOIN_COLUMN_NDV),
                symbolStatistics(RIGHT_OTHER_COLUMN, 24, 24, (0.24 * (innerJoinRowCount + rightJoinComplementRowCount) + leftJoinComplementRowCount) / totalRowCount, 1));

        assertJoinStats(FULL, LEFT_STATS, RIGHT_STATS, leftJoinStats);
    }

    @Test
    public void testAddJoinComplementStats()
    {
        double statsToAddNdv = 5;
        PlanNodeStatsEstimate statsToAdd = planNodeStats(RIGHT_ROWS_COUNT,
                symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 5.0, 0.2, statsToAddNdv));

        PlanNodeStatsEstimate addedStats = planNodeStats(TOTAL_ROWS_COUNT,
                symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, (LEFT_ROWS_COUNT * LEFT_JOIN_COLUMN_NULLS + RIGHT_ROWS_COUNT * 0.2) / TOTAL_ROWS_COUNT, LEFT_JOIN_COLUMN_NDV),
                symbolStatistics(LEFT_OTHER_COLUMN, 42, 42, (0.42 * LEFT_ROWS_COUNT + RIGHT_ROWS_COUNT) / TOTAL_ROWS_COUNT, 1));

        assertThat(JOIN_STATS_RULE.addJoinComplementStats(
                LEFT_STATS,
                LEFT_STATS,
                statsToAdd))
                .equalTo(addedStats);
    }

    private void assertJoinStats(JoinNode.Type joinType, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats, PlanNodeStatsEstimate resultStats)
    {
        assertJoinStats(joinType, LEFT_JOIN_COLUMN, LEFT_OTHER_COLUMN, RIGHT_JOIN_COLUMN, RIGHT_OTHER_COLUMN, leftStats, rightStats, resultStats);
    }

    private void assertJoinStats(JoinNode.Type joinType, String leftJoinColumn, String leftOtherColumn, String rightJoinColumn, String rightOtherColumn, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats, PlanNodeStatsEstimate resultStats)
    {
        tester().assertStatsFor(pb -> {
            Symbol leftJoinColumnSymbol = pb.symbol(leftJoinColumn, BIGINT);
            Symbol rightJoinColumnSymbol = pb.symbol(rightJoinColumn, DOUBLE);
            Symbol leftOtherColumnSymbol = pb.symbol(leftOtherColumn, BIGINT);
            Symbol rightOtherColumnSymbol = pb.symbol(rightOtherColumn, DOUBLE);
            return pb
                    .join(joinType, pb.values(leftJoinColumnSymbol, leftOtherColumnSymbol),
                            pb.values(rightJoinColumnSymbol, rightOtherColumnSymbol),
                            new EquiJoinClause(leftJoinColumnSymbol, rightJoinColumnSymbol));
        }).withSourceStats(0, leftStats)
                .withSourceStats(1, rightStats)
                .check(JOIN_STATS_RULE, stats -> stats.equalTo(resultStats));
    }

    private static PlanNodeStatsEstimate planNodeStats(double rowCount, SymbolStatistics... symbolStatistics)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount);
        for (SymbolStatistics symbolStatistic : symbolStatistics) {
            builder.addSymbolStatistics(symbolStatistic.symbol, symbolStatistic.estimate);
        }
        return builder.build();
    }

    private static SymbolStatistics symbolStatistics(String symbolName, double low, double high, double nullsFraction, double ndv)
    {
        return new SymbolStatistics(
                new Symbol(symbolName),
                SymbolStatsEstimate.builder()
                        .setLowValue(low)
                        .setHighValue(high)
                        .setNullsFraction(nullsFraction)
                        .setDistinctValuesCount(ndv)
                        .build());
    }

    private static class SymbolStatistics
    {
        final Symbol symbol;
        final SymbolStatsEstimate estimate;

        SymbolStatistics(Symbol symbol, SymbolStatsEstimate estimate)
        {
            this.symbol = symbol;
            this.estimate = estimate;
        }
    }
}
