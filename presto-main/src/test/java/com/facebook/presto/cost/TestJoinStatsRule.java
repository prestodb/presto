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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static com.facebook.presto.cost.PlanNodeStatsAssertion.assertThat;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static java.lang.Double.NaN;
import static org.testng.Assert.assertEquals;

public class TestJoinStatsRule
        extends BaseStatsCalculatorTest
{
    private static final VariableReferenceExpression LEFT_JOIN_COLUMN = new VariableReferenceExpression("left_join_column", BIGINT);
    private static final VariableReferenceExpression LEFT_JOIN_COLUMN_2 = new VariableReferenceExpression("left_join_column_2", BIGINT);
    private static final VariableReferenceExpression RIGHT_JOIN_COLUMN = new VariableReferenceExpression("right_join_column", DOUBLE);
    private static final VariableReferenceExpression RIGHT_JOIN_COLUMN_2 = new VariableReferenceExpression("right_join_column_2", DOUBLE);
    private static final VariableReferenceExpression LEFT_OTHER_COLUMN = new VariableReferenceExpression("left_column", BIGINT);
    private static final VariableReferenceExpression RIGHT_OTHER_COLUMN = new VariableReferenceExpression("right_column", DOUBLE);

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

    private static final VariableStatistics LEFT_JOIN_COLUMN_STATS =
            variableStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, LEFT_JOIN_COLUMN_NULLS, LEFT_JOIN_COLUMN_NDV);
    private static final VariableStatistics LEFT_JOIN_COLUMN_2_STATS =
            variableStatistics(LEFT_JOIN_COLUMN_2, 0.0, 200.0, LEFT_JOIN_COLUMN_2_NULLS, LEFT_JOIN_COLUMN_2_NDV);
    private static final VariableStatistics LEFT_OTHER_COLUMN_STATS =
            variableStatistics(LEFT_OTHER_COLUMN, 42, 42, 0.42, 1);
    private static final VariableStatistics RIGHT_JOIN_COLUMN_STATS =
            variableStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, RIGHT_JOIN_COLUMN_NULLS, RIGHT_JOIN_COLUMN_NDV);
    private static final VariableStatistics RIGHT_JOIN_COLUMN_2_STATS =
            variableStatistics(RIGHT_JOIN_COLUMN_2, 100.0, 200.0, RIGHT_JOIN_COLUMN_2_NULLS, RIGHT_JOIN_COLUMN_2_NDV);
    private static final VariableStatistics RIGHT_OTHER_COLUMN_STATS =
            variableStatistics(RIGHT_OTHER_COLUMN, 24, 24, 0.24, 1);
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
                variableStatistics(LEFT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                variableStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                LEFT_OTHER_COLUMN_STATS, RIGHT_OTHER_COLUMN_STATS);

        assertJoinStats(INNER, LEFT_STATS, RIGHT_STATS, innerJoinStats);
    }

    @Test
    public void testStatsForInnerJoinWithRepeatedClause()
    {
        double innerJoinRowCount = LEFT_ROWS_COUNT * RIGHT_ROWS_COUNT / LEFT_JOIN_COLUMN_NDV * LEFT_JOIN_COLUMN_NON_NULLS * RIGHT_JOIN_COLUMN_NON_NULLS // driver join clause
                * UNKNOWN_FILTER_COEFFICIENT; // auxiliary join clause
        PlanNodeStatsEstimate innerJoinStats = planNodeStats(innerJoinRowCount,
                variableStatistics(LEFT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                variableStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                LEFT_OTHER_COLUMN_STATS, RIGHT_OTHER_COLUMN_STATS);

        tester().assertStatsFor(pb -> {
            VariableReferenceExpression leftJoinColumnVariable = pb.variable(LEFT_JOIN_COLUMN);
            VariableReferenceExpression rightJoinColumnVariable = pb.variable(RIGHT_JOIN_COLUMN);
            VariableReferenceExpression leftOtherColumnVariable = pb.variable(LEFT_OTHER_COLUMN);
            VariableReferenceExpression rightOtherColumnVariable = pb.variable(RIGHT_OTHER_COLUMN);
            return pb
                    .join(INNER, pb.values(leftJoinColumnVariable, leftOtherColumnVariable),
                            pb.values(rightJoinColumnVariable, rightOtherColumnVariable),
                            new EquiJoinClause(pb.variable(leftJoinColumnVariable), pb.variable(rightJoinColumnVariable)), new EquiJoinClause(pb.variable(leftJoinColumnVariable), pb.variable(rightJoinColumnVariable)));
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
                variableStatistics(LEFT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                variableStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                variableStatistics(LEFT_JOIN_COLUMN_2, 100.0, 200.0, 0.0, RIGHT_JOIN_COLUMN_2_NDV),
                variableStatistics(RIGHT_JOIN_COLUMN_2, 100.0, 200.0, 0.0, RIGHT_JOIN_COLUMN_2_NDV));

        tester().assertStatsFor(pb -> {
            VariableReferenceExpression leftJoinColumnVariable = pb.variable(LEFT_JOIN_COLUMN);
            VariableReferenceExpression rightJoinColumnVariable = pb.variable(RIGHT_JOIN_COLUMN);
            VariableReferenceExpression leftJoinColumnVariable2 = pb.variable(LEFT_JOIN_COLUMN_2);
            VariableReferenceExpression rightJoinColumnVariable2 = pb.variable(RIGHT_JOIN_COLUMN_2);
            return pb
                    .join(INNER, pb.values(leftJoinColumnVariable, leftJoinColumnVariable2),
                            pb.values(rightJoinColumnVariable, rightJoinColumnVariable2),
                            new EquiJoinClause(pb.variable(leftJoinColumnVariable2), pb.variable(rightJoinColumnVariable2)), new EquiJoinClause(pb.variable(leftJoinColumnVariable), pb.variable(rightJoinColumnVariable)));
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
                variableStatistics(LEFT_JOIN_COLUMN, 5.0, 10.0, 0.0, RIGHT_JOIN_COLUMN_NDV * 0.3333333333),
                variableStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, 0.0, RIGHT_JOIN_COLUMN_NDV),
                variableStatistics(LEFT_JOIN_COLUMN_2, 100.0, 200.0, 0.0, RIGHT_JOIN_COLUMN_2_NDV),
                variableStatistics(RIGHT_JOIN_COLUMN_2, 100.0, 200.0, 0.0, RIGHT_JOIN_COLUMN_2_NDV));

        tester().assertStatsFor(pb -> {
            VariableReferenceExpression leftJoinColumn = pb.variable(LEFT_JOIN_COLUMN);
            VariableReferenceExpression rightJoinColumn = pb.variable(RIGHT_JOIN_COLUMN);
            VariableReferenceExpression leftJoinColumn2 = pb.variable(LEFT_JOIN_COLUMN_2);
            VariableReferenceExpression rightJoinColumn2 = pb.variable(RIGHT_JOIN_COLUMN_2);
            ComparisonExpression leftJoinColumnLessThanTen = new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, new SymbolReference(leftJoinColumn.getName()), new LongLiteral("10"));
            return pb
                    .join(INNER, pb.values(leftJoinColumn, leftJoinColumn2),
                            pb.values(rightJoinColumn, rightJoinColumn2),
                            ImmutableList.of(new EquiJoinClause(leftJoinColumn2, rightJoinColumn2), new EquiJoinClause(leftJoinColumn, rightJoinColumn)),
                            ImmutableList.of(leftJoinColumn, leftJoinColumn2, rightJoinColumn, rightJoinColumn2),
                            Optional.of(castToRowExpression(leftJoinColumnLessThanTen)));
        }).withSourceStats(0, planNodeStats(LEFT_ROWS_COUNT, LEFT_JOIN_COLUMN_STATS, LEFT_JOIN_COLUMN_2_STATS))
                .withSourceStats(1, planNodeStats(RIGHT_ROWS_COUNT, RIGHT_JOIN_COLUMN_STATS, RIGHT_JOIN_COLUMN_2_STATS))
                .check(stats -> stats.equalTo(innerJoinStats));
    }

    @Test
    public void testJoinComplementStats()
    {
        PlanNodeStatsEstimate expected = planNodeStats(LEFT_ROWS_COUNT * (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4),
                variableStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, LEFT_JOIN_COLUMN_NULLS / (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4), 5),
                LEFT_OTHER_COLUMN_STATS);
        PlanNodeStatsEstimate actual = JOIN_STATS_RULE.calculateJoinComplementStats(
                Optional.empty(),
                ImmutableList.of(new EquiJoinClause(LEFT_JOIN_COLUMN, RIGHT_JOIN_COLUMN)),
                LEFT_STATS,
                RIGHT_STATS);
        assertEquals(actual, expected);
    }

    @Test
    public void testRightJoinComplementStats()
    {
        PlanNodeStatsEstimate expected = NORMALIZER.normalize(
                planNodeStats(
                        RIGHT_ROWS_COUNT * RIGHT_JOIN_COLUMN_NULLS,
                        variableStatistics(RIGHT_JOIN_COLUMN, NaN, NaN, 1.0, 0),
                        RIGHT_OTHER_COLUMN_STATS));
        PlanNodeStatsEstimate actual = JOIN_STATS_RULE.calculateJoinComplementStats(
                Optional.empty(),
                ImmutableList.of(new EquiJoinClause(RIGHT_JOIN_COLUMN, LEFT_JOIN_COLUMN)),
                RIGHT_STATS,
                LEFT_STATS);
        assertEquals(actual, expected);
    }

    @Test
    public void testLeftJoinComplementStatsWithNoClauses()
    {
        PlanNodeStatsEstimate expected = NORMALIZER.normalize(LEFT_STATS.mapOutputRowCount(rowCount -> 0.0));
        PlanNodeStatsEstimate actual = JOIN_STATS_RULE.calculateJoinComplementStats(
                Optional.empty(),
                ImmutableList.of(),
                LEFT_STATS,
                RIGHT_STATS);
        assertEquals(actual, expected);
    }

    @Test
    public void testLeftJoinComplementStatsWithMultipleClauses()
    {
        PlanNodeStatsEstimate expected = planNodeStats(
                LEFT_ROWS_COUNT * (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4),
                variableStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, LEFT_JOIN_COLUMN_NULLS / (LEFT_JOIN_COLUMN_NULLS + LEFT_JOIN_COLUMN_NON_NULLS / 4), 5),
                LEFT_OTHER_COLUMN_STATS)
                .mapOutputRowCount(rowCount -> rowCount / UNKNOWN_FILTER_COEFFICIENT);
        PlanNodeStatsEstimate actual = JOIN_STATS_RULE.calculateJoinComplementStats(
                Optional.empty(),
                ImmutableList.of(
                        new EquiJoinClause(LEFT_JOIN_COLUMN, RIGHT_JOIN_COLUMN),
                        new EquiJoinClause(LEFT_OTHER_COLUMN, RIGHT_OTHER_COLUMN)),
                LEFT_STATS,
                RIGHT_STATS);
        assertEquals(actual, expected);
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
                variableStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, joinComplementColumnNulls * joinComplementRowCount / totalRowCount, LEFT_JOIN_COLUMN_NDV),
                LEFT_OTHER_COLUMN_STATS,
                variableStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, joinComplementRowCount / totalRowCount, RIGHT_JOIN_COLUMN_NDV),
                variableStatistics(RIGHT_OTHER_COLUMN, 24, 24, (0.24 * innerJoinRowCount + joinComplementRowCount) / totalRowCount, 1));

        assertJoinStats(LEFT, LEFT_STATS, RIGHT_STATS, leftJoinStats);
        assertJoinStats(RIGHT, RIGHT_JOIN_COLUMN, RIGHT_OTHER_COLUMN, LEFT_JOIN_COLUMN, LEFT_OTHER_COLUMN, RIGHT_STATS, LEFT_STATS, leftJoinStats);
    }

    @Test
    public void testLeftJoinMissingStats()
    {
        PlanNodeStatsEstimate leftStats = planNodeStats(
                1,
                new VariableStatistics(LEFT_JOIN_COLUMN, VariableStatsEstimate.unknown()),
                new VariableStatistics(LEFT_OTHER_COLUMN, VariableStatsEstimate.unknown()));
        PlanNodeStatsEstimate rightStats = planNodeStats(
                1,
                new VariableStatistics(RIGHT_JOIN_COLUMN, VariableStatsEstimate.unknown()),
                new VariableStatistics(RIGHT_OTHER_COLUMN, VariableStatsEstimate.unknown()));
        assertJoinStats(LEFT, leftStats, rightStats, PlanNodeStatsEstimate.unknown());
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
                variableStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, (leftJoinComplementColumnNulls * leftJoinComplementRowCount + rightJoinComplementRowCount) / totalRowCount, LEFT_JOIN_COLUMN_NDV),
                variableStatistics(LEFT_OTHER_COLUMN, 42, 42, (0.42 * (innerJoinRowCount + leftJoinComplementRowCount) + rightJoinComplementRowCount) / totalRowCount, 1),
                variableStatistics(RIGHT_JOIN_COLUMN, 5.0, 20.0, (rightJoinComplementColumnNulls * rightJoinComplementRowCount + leftJoinComplementRowCount) / totalRowCount, RIGHT_JOIN_COLUMN_NDV),
                variableStatistics(RIGHT_OTHER_COLUMN, 24, 24, (0.24 * (innerJoinRowCount + rightJoinComplementRowCount) + leftJoinComplementRowCount) / totalRowCount, 1));

        assertJoinStats(FULL, LEFT_STATS, RIGHT_STATS, leftJoinStats);
    }

    @Test
    public void testAddJoinComplementStats()
    {
        double statsToAddNdv = 5;
        PlanNodeStatsEstimate statsToAdd = planNodeStats(RIGHT_ROWS_COUNT,
                variableStatistics(LEFT_JOIN_COLUMN, 0.0, 5.0, 0.2, statsToAddNdv));

        PlanNodeStatsEstimate addedStats = planNodeStats(TOTAL_ROWS_COUNT,
                variableStatistics(LEFT_JOIN_COLUMN, 0.0, 20.0, (LEFT_ROWS_COUNT * LEFT_JOIN_COLUMN_NULLS + RIGHT_ROWS_COUNT * 0.2) / TOTAL_ROWS_COUNT, LEFT_JOIN_COLUMN_NDV),
                variableStatistics(LEFT_OTHER_COLUMN, 42, 42, (0.42 * LEFT_ROWS_COUNT + RIGHT_ROWS_COUNT) / TOTAL_ROWS_COUNT, 1));

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

    private void assertJoinStats(
            JoinNode.Type joinType,
            VariableReferenceExpression leftJoinColumn,
            VariableReferenceExpression leftOtherColumn,
            VariableReferenceExpression rightJoinColumn,
            VariableReferenceExpression rightOtherColumn,
            PlanNodeStatsEstimate leftStats,
            PlanNodeStatsEstimate rightStats,
            PlanNodeStatsEstimate resultStats)
    {
        tester().assertStatsFor(pb -> {
            VariableReferenceExpression leftJoinColumnVariable = pb.variable(leftJoinColumn);
            VariableReferenceExpression rightJoinColumnVariable = pb.variable(rightJoinColumn);
            VariableReferenceExpression leftOtherColumnVariable = pb.variable(leftOtherColumn);
            VariableReferenceExpression rightOtherColumnVariable = pb.variable(rightOtherColumn);
            return pb
                    .join(joinType, pb.values(leftJoinColumnVariable, leftOtherColumnVariable),
                            pb.values(rightJoinColumnVariable, rightOtherColumnVariable),
                            new EquiJoinClause(leftJoinColumnVariable, rightJoinColumnVariable));
        }).withSourceStats(0, leftStats)
                .withSourceStats(1, rightStats)
                .check(JOIN_STATS_RULE, stats -> stats.equalTo(resultStats));
    }

    private static PlanNodeStatsEstimate planNodeStats(double rowCount, VariableStatistics... variableStatistics)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount);
        for (VariableStatistics symbolStatistic : variableStatistics) {
            builder.addVariableStatistics(symbolStatistic.variable, symbolStatistic.estimate);
        }
        return builder.build();
    }

    private static VariableStatistics variableStatistics(VariableReferenceExpression variable, double low, double high, double nullsFraction, double ndv)
    {
        return new VariableStatistics(
                variable,
                VariableStatsEstimate.builder()
                        .setLowValue(low)
                        .setHighValue(high)
                        .setNullsFraction(nullsFraction)
                        .setDistinctValuesCount(ndv)
                        .build());
    }

    private static class VariableStatistics
    {
        final VariableReferenceExpression variable;
        final VariableStatsEstimate estimate;

        VariableStatistics(String variableName, VariableStatsEstimate estimate)
        {
            this(new VariableReferenceExpression(variableName, BIGINT), estimate);
        }

        VariableStatistics(VariableReferenceExpression variable, VariableStatsEstimate estimate)
        {
            this.variable = variable;
            this.estimate = estimate;
        }
    }
}
