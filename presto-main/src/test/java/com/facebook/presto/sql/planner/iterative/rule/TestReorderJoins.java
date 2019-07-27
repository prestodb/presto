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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.EQUAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestReorderJoins
{
    private RuleTester tester;

    // TWO_ROWS are used to prevent node from being scalar
    private static final ImmutableList<List<RowExpression>> TWO_ROWS = ImmutableList.of(ImmutableList.of(), ImmutableList.of());

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(
                ImmutableList.of(),
                ImmutableMap.of(
                        JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name(),
                        JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.AUTOMATIC.name()),
                Optional.of(4));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testKeepsOutputSymbols()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.variable("A1"), p.variable("A2")), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.variable("B1")), TWO_ROWS),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A2")),
                                Optional.empty()))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5000)
                        .addVariableStatistics(ImmutableMap.of(
                                new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 100, 100),
                                new VariableReferenceExpression("A2", BIGINT), new VariableStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0, "A2", 1)),
                        values(ImmutableMap.of("B1", 0)))
                        .withExactOutputs("A2"));
    }

    @Test
    public void testReplicatesAndFlipsWhenOneTableMuchSmaller()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.variable("A1")), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.variable("B1")), TWO_ROWS),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A1"), p.variable("B1")),
                                Optional.empty()))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testRepartitionsWhenRequiredBySession()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.variable("A1")), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.variable("B1")), TWO_ROWS),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A1"), p.variable("B1")),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testRepartitionsWhenBothTablesEqual()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.variable("A1")), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.variable("B1")), TWO_ROWS),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A1"), p.variable("B1")),
                                Optional.empty()))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesUnrestrictedWhenRequiredBySession()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.variable("A1")), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.variable("B1")), TWO_ROWS),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A1"), p.variable("B1")),
                                Optional.empty()))
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "1kB")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatedScalarJoinEvenWhereSessionRequiresRepartitioned()
    {
        PlanMatchPattern expectedPlan = join(
                INNER,
                ImmutableList.of(equiJoinClause("A1", "B1")),
                Optional.empty(),
                Optional.of(REPLICATED),
                values(ImmutableMap.of("A1", 0)),
                values(ImmutableMap.of("B1", 0)));

        PlanNodeStatsEstimate valuesA = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10000)
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                .build();
        PlanNodeStatsEstimate valuesB = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10000)
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                .build();

        assertReorderJoins()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.variable("A1")), // matches isAtMostScalar
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.variable("B1")), TWO_ROWS),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A1"), p.variable("B1")),
                                Optional.empty()))
                .overrideStats("valuesA", valuesA)
                .overrideStats("valuesB", valuesB)
                .matches(expectedPlan);

        assertReorderJoins()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.variable("B1")), TWO_ROWS),
                                p.values(new PlanNodeId("valuesA"), p.variable("A1")), // matches isAtMostScalar
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A1"), p.variable("B1")),
                                Optional.empty()))
                .overrideStats("valuesA", valuesA)
                .overrideStats("valuesB", valuesB)
                .matches(expectedPlan);
    }

    @Test
    public void testDoesNotFireForCrossJoin()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.variable("A1")), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.variable("B1")), TWO_ROWS),
                                ImmutableList.of(),
                                ImmutableList.of(p.variable("A1"), p.variable("B1")),
                                Optional.empty()))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoStats()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.variable("A1")), TWO_ROWS),
                                p.values(new PlanNodeId("valuesB"), p.variable("B1")),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A1")),
                                Optional.empty()))
                .overrideStats("valuesA", PlanNodeStatsEstimate.unknown())
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonDeterministicFilter()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.variable("A1")),
                                p.values(new PlanNodeId("valuesB"), p.variable("B1")),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A1"), p.variable("B1")),
                                Optional.of(castToRowExpression(new ComparisonExpression(LESS_THAN, new SymbolReference("A1"), new FunctionCall(QualifiedName.of("random"), ImmutableList.of()))))))
                .doesNotFire();
    }

    @Test
    public void testPredicatesPushedDown()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.join(
                                        INNER,
                                        p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.variable("A1")), TWO_ROWS),
                                        p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.variable("B1"), p.variable("B2")), TWO_ROWS),
                                        ImmutableList.of(),
                                        ImmutableList.of(p.variable("A1"), p.variable("B1"), p.variable("B2")),
                                        Optional.empty()),
                                p.values(new PlanNodeId("valuesC"), ImmutableList.of(p.variable("C1")), TWO_ROWS),
                                ImmutableList.of(
                                        new EquiJoinClause(p.variable("B2"), p.variable("C1"))),
                                ImmutableList.of(p.variable("A1")),
                                Optional.of(castToRowExpression(new ComparisonExpression(EQUAL, new SymbolReference("A1"), new SymbolReference("B1"))))))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 100, 10)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(5)
                        .addVariableStatistics(ImmutableMap.of(
                                new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 100, 5),
                                new VariableReferenceExpression("B2", BIGINT), new VariableStatsEstimate(0, 100, 0, 100, 5)))
                        .build())
                .overrideStats("valuesC", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("C1", BIGINT), new VariableStatsEstimate(0, 100, 0, 100, 100)))
                        .build())
                .matches(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("C1", "B2")),
                                values("C1"),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("A1", "B1")),
                                        values("A1"),
                                        values("B1", "B2"))));
    }

    @Test
    public void testSmallerJoinFirst()
    {
        assertReorderJoins()
                .on(p ->
                        p.join(
                                INNER,
                                p.join(
                                        INNER,
                                        p.values(new PlanNodeId("valuesA"), ImmutableList.of(p.variable("A1")), TWO_ROWS),
                                        p.values(new PlanNodeId("valuesB"), ImmutableList.of(p.variable("B1"), p.variable("B2")), TWO_ROWS),
                                        ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                        ImmutableList.of(p.variable("A1"), p.variable("B1"), p.variable("B2")),
                                        Optional.empty()),
                                p.values(new PlanNodeId("valuesC"), ImmutableList.of(p.variable("C1")), TWO_ROWS),
                                ImmutableList.of(
                                        new EquiJoinClause(p.variable("B2"), p.variable("C1"))),
                                ImmutableList.of(p.variable("A1")),
                                Optional.of(castToRowExpression(new ComparisonExpression(EQUAL, new SymbolReference("A1"), new SymbolReference("B1"))))))
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(40)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 100, 10)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addVariableStatistics(ImmutableMap.of(
                                new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 100, 10),
                                new VariableReferenceExpression("B2", BIGINT), new VariableStatsEstimate(0, 100, 0, 100, 10)))
                        .build())
                .overrideStats("valuesC", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("C1", BIGINT), new VariableStatsEstimate(99, 199, 0, 100, 100)))
                        .build())
                .matches(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("A1", "B1")),
                                values("A1"),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("C1", "B2")),
                                        values("C1"),
                                        values("B1", "B2"))));
    }

    @Test
    public void testReplicatesWhenNotRestricted()
    {
        int aRows = 10_000;
        int bRows = 10;

        PlanNodeStatsEstimate probeSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 10)))
                .build();
        PlanNodeStatsEstimate buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 10)))
                .build();

        // B table is small enough to be replicated in AUTOMATIC_RESTRICTED mode
        assertReorderJoins()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1")),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1")),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A1"), p.variable("B1")),
                                Optional.empty()))
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));

        probeSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();

        // B table exceeds AUTOMATIC_RESTRICTED limit therefore it is partitioned
        assertReorderJoins()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1")),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1")),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1"), p.variable("B1"))),
                                ImmutableList.of(p.variable("A1"), p.variable("B1")),
                                Optional.empty()))
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    private RuleAssert assertReorderJoins()
    {
        return tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)));
    }
}
