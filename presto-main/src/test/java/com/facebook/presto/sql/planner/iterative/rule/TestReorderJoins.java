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

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.cost.SymbolStatsEstimate;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.ComparisonExpressionType;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.cost.PlanNodeStatsEstimate.UNKNOWN_STATS;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.tree.ComparisonExpressionType.EQUAL;
import static com.facebook.presto.testing.LocalQueryRunner.queryRunnerWithFakeNodeCountForStats;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestReorderJoins
{
    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("join_distribution_type", "automatic")
                .setSystemProperty("join_reordering_strategy", "COST_BASED")
                .build();
        LocalQueryRunner queryRunner = queryRunnerWithFakeNodeCountForStats(session, 4);
        tester = new RuleTester(queryRunner);
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
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT), p.symbol("A2", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A2", BIGINT)),
                                Optional.empty()))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(5000)
                                .setSymbolStatistics(ImmutableMap.of(
                                        new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 100, 100),
                                        new Symbol("A2"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                                .build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(10000)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                                .build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0, "A2", 1)),
                        values(ImmutableMap.of("B1", 0))
                ));
    }

    @Test
    public void testReplicatesAndFlipsWhenOneTableMuchSmaller()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(100)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                                .build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(10000)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                                .build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))
                ));
    }

    @Test
    public void testRepartitionsWhenRequiredBySession()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty("join_distribution_type", "REPARTITIONED")
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(100)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                                .build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(10000)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                                .build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))
                ));
    }

    @Test
    public void testRepartitionsWhenBothTablesEqual()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(10000)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                                .build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(10000)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                                .build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))
                ));
    }

    @Test
    public void testReplicatesWhenRequiredBySession()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty("join_distribution_type", "REPLICATED")
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(10000)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                                .build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(10000)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                                .build()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))
                ));
    }

    @Test
    public void testDoesNotFireForCrossJoin()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(10000)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                                .build(),
                        new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(10000)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                                .build()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithNoStats()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .withStatsCalculator(new UnknownStatsCalculator())
                .on(p ->
                        p.join(
                                INNER,
                                p.tableScan(ImmutableList.of(p.symbol("A1", BIGINT)), ImmutableMap.of(p.symbol("A1", BIGINT), new TestingColumnHandle("A1"))),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                Optional.empty()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonDeterministicFilter()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.of(new ComparisonExpression(ComparisonExpressionType.LESS_THAN, p.symbol("A1", BIGINT).toSymbolReference(), new FunctionCall(QualifiedName.of("random"), ImmutableList.of())))))
                .doesNotFire();
    }

    @Test
    public void testPredicatesPushedDown()
    {
        tester.assertThat(new ReorderJoins(new CostComparator(1, 1, 1)))
                .on(p ->
                        p.join(
                                INNER,
                                p.join(
                                        INNER,
                                        p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                        p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT), p.symbol("B2", BIGINT)),
                                        ImmutableList.of(),
                                        ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT), p.symbol("B2", BIGINT)),
                                        Optional.empty()),
                                p.values(new PlanNodeId("valuesC"), p.symbol("C1", BIGINT)),
                                ImmutableList.of(
                                        new JoinNode.EquiJoinClause(p.symbol("B2", BIGINT), p.symbol("C1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT)),
                                Optional.of(new ComparisonExpression(EQUAL, p.symbol("A1", BIGINT).toSymbolReference(), p.symbol("B1", BIGINT).toSymbolReference()))))
                .withStats(ImmutableMap.of(
                        new PlanNodeId("valuesA"),
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(10)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 100, 10)))
                                .build(),
                        new PlanNodeId("valuesB"),
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(5)
                                .setSymbolStatistics(ImmutableMap.of(
                                        new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 100, 10),
                                        new Symbol("B2"), new SymbolStatsEstimate(0, 100, 0, 100, 10)))
                                .build(),
                        new PlanNodeId("valuesC"),
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(1000)
                                .setSymbolStatistics(ImmutableMap.of(new Symbol("C1"), new SymbolStatsEstimate(0, 100, 0, 100, 100)))
                                .build()))
                .matches(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("C1", "B2")),
                                values("C1"),
                                join(
                                        INNER,
                                        ImmutableList.of(equiJoinClause("A1", "B1")),
                                        values("A1"),
                                        values("B1", "B2"))
                        )
            );
    }

    private static class UnknownStatsCalculator
            implements StatsCalculator
    {
        @Override
        public PlanNodeStatsEstimate calculateStats(
                PlanNode planNode,
                Lookup lookup,
                Session session,
                Map<Symbol, Type> types)
        {
            PlanNodeStatsEstimate.Builder statsBuilder = PlanNodeStatsEstimate.buildFrom(UNKNOWN_STATS);
            planNode.getOutputSymbols()
                    .forEach(symbol -> statsBuilder.addSymbolStatistics(symbol, SymbolStatsEstimate.UNKNOWN_STATS));
            return statsBuilder.build();
        }
    }
}
