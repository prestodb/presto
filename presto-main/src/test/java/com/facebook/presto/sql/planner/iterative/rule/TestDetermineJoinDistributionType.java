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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingStatsCalculator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.testing.LocalQueryRunner.queryRunnerWithFakeNodeCountForStats;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

@Test(singleThreaded = true)
public class TestDetermineJoinDistributionType
{
    private RuleTester tester;
    private StatsCalculator statsCalculator;

    @BeforeClass
    public void setUp()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("join_distribution_type", "automatic")
                .build();
        LocalQueryRunner queryRunner = queryRunnerWithFakeNodeCountForStats(session, 4);
        tester = new RuleTester(queryRunner);
        statsCalculator = queryRunner.getStatsCalculator();
    }

    @Test
    public void testReplicatesAndFlipsWhenOneTableMuchSmaller()
    {
        StatsCalculator testingStatsCalculator = new TestingStatsCalculator(statsCalculator, ImmutableMap.of(
                new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                        .build(),
                new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build()));

        tester.assertThat(new DetermineJoinDistributionType(new CostComparator(1, 1, 1)))
                .withStatsCalculator(testingStatsCalculator)
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
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
        StatsCalculator testingStatsCalculator = new TestingStatsCalculator(statsCalculator, ImmutableMap.of(
                new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                        .build(),
                new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build()));

        tester.assertThat(new DetermineJoinDistributionType(new CostComparator(1, 1, 1)))
                .withStatsCalculator(testingStatsCalculator)
                .on(p ->
                        p.join(
                                INNER,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty("join_distribution_type", "REPARTITIONED")
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
        StatsCalculator testingStatsCalculator = new TestingStatsCalculator(statsCalculator, ImmutableMap.of(
                new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build(),
                new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build()));

        tester.assertThat(new DetermineJoinDistributionType(new CostComparator(1, 1, 1)))
                .withStatsCalculator(testingStatsCalculator)
                .on(p ->
                        p.join(
                                INNER,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
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
        StatsCalculator testingStatsCalculator = new TestingStatsCalculator(statsCalculator, ImmutableMap.of(
                new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build(),
                new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build()));

        tester.assertThat(new DetermineJoinDistributionType(new CostComparator(1, 1, 1)))
                .withStatsCalculator(testingStatsCalculator)
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty("join_distribution_type", "REPLICATED")
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
    public void testRepartitionsForFullOuterJoin()
    {
        StatsCalculator testingStatsCalculator = new TestingStatsCalculator(statsCalculator, ImmutableMap.of(
                new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build(),
                new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build()));

        tester.assertThat(new DetermineJoinDistributionType(new CostComparator(1, 1, 1)))
                .withStatsCalculator(testingStatsCalculator)
                .on(p ->
                        p.join(
                                FULL,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        FULL,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))
                ));
    }

    @Test
    public void testRepartitionsForRightOuterJoin()
    {
        StatsCalculator testingStatsCalculator = new TestingStatsCalculator(statsCalculator, ImmutableMap.of(
                new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build(),
                new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build()));

        tester.assertThat(new DetermineJoinDistributionType(new CostComparator(1, 1, 1)))
                .withStatsCalculator(testingStatsCalculator)
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        RIGHT,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))
                ));
    }

    @Test
    public void testReplicatesForLeftOuterJoin()
    {
        StatsCalculator testingStatsCalculator = new TestingStatsCalculator(statsCalculator, ImmutableMap.of(
                new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build(),
                new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build()));

        tester.assertThat(new DetermineJoinDistributionType(new CostComparator(75, 10, 15)))
                .withStatsCalculator(testingStatsCalculator)
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        LEFT,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))
                ));
    }

    @Test
    public void testReplicatesAndFlipsForRightOuterJoin()
    {
        StatsCalculator testingStatsCalculator = new TestingStatsCalculator(statsCalculator, ImmutableMap.of(
                new PlanNodeId("valuesA"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build(),
                new PlanNodeId("valuesB"), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000000)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build()));

        tester.assertThat(new DetermineJoinDistributionType(new CostComparator(75, 10, 15)))
                .withStatsCalculator(testingStatsCalculator)
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        LEFT,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))
                ));
    }
}
