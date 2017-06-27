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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

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
{
    private static final String LEFT_JOIN_COLUMN = "left_join_column";
    private static final String RIGHT_JOIN_COLUMN = "right_join_column";
    private static final String LEFT_OTHER_COLUMN = "left_column";
    private static final String RIGHT_OTHER_COLUMN = "right_column";

    private static final double LEFT_ROWS_COUNT = 500.0;
    private static final double RIGHT_ROWS_COUNT = 1000.0;
    private static final double TOTAL_ROWS_COUNT = LEFT_ROWS_COUNT + RIGHT_ROWS_COUNT;
    private static final double LEFT_JOIN_COLUMN_NULLS = 0.3;
    private static final double LEFT_JOIN_COLUMN_NON_NULLS = 0.7;
    private static final int LEFT_JOIN_COLUMN_NDV = 20;
    private static final double RIGHT_JOIN_COLUMN_NULLS = 0.6;
    private static final double RIGHT_JOIN_COLUMN_NON_NULLS = 0.4;
    private static final int RIGHT_JOIN_COLUMN_NDV = 15;

    private static final SymbolStatistics LEFT_OTHER_COLUMN_STATS =
            symbolStatistics(LEFT_OTHER_COLUMN, 42, 42, 0.42, 1);
    private static final SymbolStatistics RIGHT_OTHER_COLUMN_STATS =
            symbolStatistics(RIGHT_OTHER_COLUMN, 24, 24, 0.24, 1);
    private static final PlanNodeStatsEstimate LEFT_STATS = planNodeStats(LEFT_ROWS_COUNT,
            symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, LEFT_JOIN_COLUMN_NULLS, LEFT_JOIN_COLUMN_NDV),
            LEFT_OTHER_COLUMN_STATS);
    private static final PlanNodeStatsEstimate RIGHT_STATS = planNodeStats(RIGHT_ROWS_COUNT,
            symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, RIGHT_JOIN_COLUMN_NULLS, RIGHT_JOIN_COLUMN_NDV),
            RIGHT_OTHER_COLUMN_STATS);

    private static final JoinStatsRule JOIN_STATS_RULE = new JoinStatsRule(new FilterStatsCalculator(createTestMetadataManager()));

    private StatsCalculatorTester tester;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        tester = new StatsCalculatorTester();
    }

    @Test
    public void testStatsForInnerJoin()
            throws Exception
    {
        double innerJoinRowCount = LEFT_ROWS_COUNT * RIGHT_ROWS_COUNT / LEFT_JOIN_COLUMN_NDV * LEFT_JOIN_COLUMN_NON_NULLS * RIGHT_JOIN_COLUMN_NON_NULLS;
        PlanNodeStatsEstimate innerJoinStats = planNodeStats(innerJoinRowCount,
                symbolStatistics(LEFT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                LEFT_OTHER_COLUMN_STATS, RIGHT_OTHER_COLUMN_STATS);

        assertJoinStats(INNER, LEFT_STATS, RIGHT_STATS, innerJoinStats);
    }

    @Test
    public void testStatsForLeftAntiJoin()
    {
        PlanNodeStatsEstimate antiJoinStats = planNodeStats(LEFT_ROWS_COUNT * (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4),
                symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 5.0, LEFT_JOIN_COLUMN_NULLS / (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4), 5),
                LEFT_OTHER_COLUMN_STATS);

        assertThat(JOIN_STATS_RULE.calculateAntiJoinStats(
                Optional.empty(),
                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(LEFT_JOIN_COLUMN), new Symbol(RIGHT_JOIN_COLUMN))),
                LEFT_STATS, RIGHT_STATS)).equalTo(antiJoinStats);
    }

    @Test
    public void testStatsForRightAntiJoin()
    {
        PlanNodeStatsEstimate antiJoinStats = planNodeStats(RIGHT_ROWS_COUNT * RIGHT_JOIN_COLUMN_NULLS,
                symbolStatistics(RIGHT_JOIN_COLUMN, NaN, NaN, 1.0, 0),
                RIGHT_OTHER_COLUMN_STATS);

        assertThat(JOIN_STATS_RULE.calculateAntiJoinStats(
                Optional.empty(),
                ImmutableList.of(new JoinNode.EquiJoinClause(new Symbol(RIGHT_JOIN_COLUMN), new Symbol(LEFT_JOIN_COLUMN))),
                RIGHT_STATS, LEFT_STATS)).equalTo(antiJoinStats);
    }

    @Test
    public void testStatsForLeftAndRightJoin()
    {
        double innerJoinRowCount = LEFT_ROWS_COUNT * RIGHT_ROWS_COUNT / LEFT_JOIN_COLUMN_NDV * LEFT_JOIN_COLUMN_NON_NULLS * RIGHT_JOIN_COLUMN_NON_NULLS;
        double antiJoinRowCount = LEFT_ROWS_COUNT * (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4);
        double antiJoinColumnNulls = LEFT_JOIN_COLUMN_NULLS / (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4);
        double totalRowCount = innerJoinRowCount + antiJoinRowCount;

        PlanNodeStatsEstimate leftJoinStats = planNodeStats(
                totalRowCount,
                symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, antiJoinColumnNulls * antiJoinRowCount / totalRowCount, LEFT_JOIN_COLUMN_NDV),
                LEFT_OTHER_COLUMN_STATS,
                symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, antiJoinRowCount / totalRowCount, RIGHT_JOIN_COLUMN_NDV),
                symbolStatistics(RIGHT_OTHER_COLUMN, 24, 24, (0.24 * innerJoinRowCount + antiJoinRowCount) / totalRowCount, 1));

        assertJoinStats(LEFT, LEFT_STATS, RIGHT_STATS, leftJoinStats);
        assertJoinStats(RIGHT, RIGHT_JOIN_COLUMN, RIGHT_OTHER_COLUMN, LEFT_JOIN_COLUMN, LEFT_OTHER_COLUMN, RIGHT_STATS, LEFT_STATS, leftJoinStats);
    }

    @Test
    public void testStatsForFullJoin()
    {
        double innerJoinRowCount = LEFT_ROWS_COUNT * RIGHT_ROWS_COUNT / LEFT_JOIN_COLUMN_NDV * LEFT_JOIN_COLUMN_NON_NULLS * RIGHT_JOIN_COLUMN_NON_NULLS;
        double leftAntiJoinRowCount = LEFT_ROWS_COUNT * (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4);
        double leftAntiJoinColumnNulls = LEFT_JOIN_COLUMN_NULLS / (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4);
        double rightAntiJoinRowCount = RIGHT_ROWS_COUNT * RIGHT_JOIN_COLUMN_NULLS;
        double rightAntiJoinColumnNulls = 1.0;
        double totalRowCount = innerJoinRowCount + leftAntiJoinRowCount + rightAntiJoinRowCount;

        PlanNodeStatsEstimate leftJoinStats = planNodeStats(
                totalRowCount,
                symbolStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, (leftAntiJoinColumnNulls * leftAntiJoinRowCount + rightAntiJoinRowCount) / totalRowCount, LEFT_JOIN_COLUMN_NDV),
                symbolStatistics(LEFT_OTHER_COLUMN, 42, 42, (0.42 * (innerJoinRowCount + leftAntiJoinRowCount) + rightAntiJoinRowCount) / totalRowCount, 1),
                symbolStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, (rightAntiJoinColumnNulls * rightAntiJoinRowCount + leftAntiJoinRowCount) / totalRowCount, RIGHT_JOIN_COLUMN_NDV),
                symbolStatistics(RIGHT_OTHER_COLUMN, 24, 24, (0.24 * (innerJoinRowCount + rightAntiJoinRowCount) + leftAntiJoinRowCount) / totalRowCount, 1));

        assertJoinStats(FULL, LEFT_STATS, RIGHT_STATS, leftJoinStats);
    }

    @Test
    public void testAddAntiJoinStats()
    {
        PlanNodeStatsEstimate statsToAdd = planNodeStats(RIGHT_ROWS_COUNT,
                symbolStatistics(LEFT_JOIN_COLUMN, -5.0, 5.0, 0.2, 5));

        PlanNodeStatsEstimate addedStats = planNodeStats(TOTAL_ROWS_COUNT,
                symbolStatistics(LEFT_JOIN_COLUMN, -5.0, 20.0, (LEFT_ROWS_COUNT * LEFT_JOIN_COLUMN_NULLS + RIGHT_ROWS_COUNT * 0.2) / TOTAL_ROWS_COUNT, 25),
                symbolStatistics(LEFT_OTHER_COLUMN, 42, 42, (0.42 * LEFT_ROWS_COUNT + RIGHT_ROWS_COUNT) / TOTAL_ROWS_COUNT, 1));

        assertThat(JOIN_STATS_RULE.addAntiJoinStats(LEFT_STATS, statsToAdd, ImmutableSet.of(new Symbol(LEFT_JOIN_COLUMN)))).equalTo(addedStats);
    }

    private void assertJoinStats(JoinNode.Type joinType, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats, PlanNodeStatsEstimate resultStats)
    {
        assertJoinStats(joinType, LEFT_JOIN_COLUMN, LEFT_OTHER_COLUMN, RIGHT_JOIN_COLUMN, RIGHT_OTHER_COLUMN, leftStats, rightStats, resultStats);
    }

    private void assertJoinStats(JoinNode.Type joinType, String leftJoinColumn, String leftOtherColumn, String rightJoinColumn, String rightOtherColumn, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats, PlanNodeStatsEstimate resultStats)
    {
        tester.assertStatsFor(pb -> {
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
                .check(stats -> stats.equalTo(resultStats));
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
