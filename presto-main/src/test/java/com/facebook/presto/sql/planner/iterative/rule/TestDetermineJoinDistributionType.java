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
import com.facebook.presto.cost.SymbolStatsEstimate;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.DistributionType;
import com.facebook.presto.sql.planner.plan.JoinNode.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.enforceSingleRow;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expressions;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;

@Test(singleThreaded = true)
public class TestDetermineJoinDistributionType
{
    private static final CostComparator COST_COMPARATOR = new CostComparator(1, 1, 1);

    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(ImmutableList.of(), ImmutableMap.of(), Optional.of(4));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @Test
    public void testDetermineDistributionType()
    {
        testDetermineDistributionType(JoinDistributionType.PARTITIONED, INNER, DistributionType.PARTITIONED);
        testDetermineDistributionType(JoinDistributionType.BROADCAST, INNER, DistributionType.REPLICATED);
        testDetermineDistributionType(JoinDistributionType.AUTOMATIC, INNER, DistributionType.PARTITIONED);
    }

    @Test
    public void testDetermineDistributionTypeForLeftOuter()
    {
        testDetermineDistributionType(JoinDistributionType.PARTITIONED, LEFT, DistributionType.PARTITIONED);
        testDetermineDistributionType(JoinDistributionType.BROADCAST, LEFT, DistributionType.REPLICATED);
        testDetermineDistributionType(JoinDistributionType.AUTOMATIC, LEFT, DistributionType.PARTITIONED);
    }

    private void testDetermineDistributionType(JoinDistributionType sessionDistributedJoin, Type joinType, DistributionType expectedDistribution)
    {
        assertDetermineJoinDistributionType()
                .on(p ->
                        p.join(
                                joinType,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, sessionDistributedJoin.name())
                .matches(join(
                        joinType,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(expectedDistribution),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testRepartitionRightOuter()
    {
        testRepartitionRightOuter(JoinDistributionType.PARTITIONED, FULL);
        testRepartitionRightOuter(JoinDistributionType.PARTITIONED, RIGHT);
        testRepartitionRightOuter(JoinDistributionType.BROADCAST, FULL);
        testRepartitionRightOuter(JoinDistributionType.BROADCAST, RIGHT);
        testRepartitionRightOuter(JoinDistributionType.AUTOMATIC, FULL);
        testRepartitionRightOuter(JoinDistributionType.AUTOMATIC, RIGHT);
    }

    private void testRepartitionRightOuter(JoinDistributionType sessionDistributedJoin, Type joinType)
    {
        assertDetermineJoinDistributionType()
                .on(p ->
                        p.join(
                                joinType,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, sessionDistributedJoin.name())
                .matches(join(
                        joinType,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(DistributionType.PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicateScalar()
    {
        assertDetermineJoinDistributionType()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.enforceSingleRow(
                                        p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11")))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(DistributionType.REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        enforceSingleRow(values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testReplicateNoEquiCriteria()
    {
        testReplicateNoEquiCriteria(INNER);
        testReplicateNoEquiCriteria(LEFT);
    }

    private void testReplicateNoEquiCriteria(Type joinType)
    {
        assertDetermineJoinDistributionType()
                .on(p ->
                        p.join(
                                joinType,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.of(expression("A1 * B1 > 100"))))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .matches(join(
                        joinType,
                        ImmutableList.of(),
                        Optional.of("A1 * B1 > 100"),
                        Optional.of(DistributionType.REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testRetainDistributionType()
    {
        assertDetermineJoinDistributionType()
                .on(p ->
                        p.join(
                                INNER,
                                p.values(ImmutableList.of(p.symbol("A1")), ImmutableList.of(expressions("10"), expressions("11"))),
                                p.values(ImmutableList.of(p.symbol("B1")), ImmutableList.of(expressions("50"), expressions("11"))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(DistributionType.REPLICATED)))
                .doesNotFire();
    }

    @Test
    public void testFlipAndReplicateWhenOneTableMuchSmaller()
    {
        int aRows = 100;
        int bRows = 10_000;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testPartitionWhenRequiredBySession()
    {
        int aRows = 100;
        int bRows = 10_000;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testPartitionWhenBothTablesEqual()
    {
        int aRows = 10_000;
        int bRows = 10_000;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesWhenRequiredBySession()
    {
        int aRows = 10_000;
        int bRows = 10_000;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testPartitionFullOuterJoin()
    {
        int aRows = 10_000;
        int bRows = 10;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        FULL,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testPartitionRightOuterJoin()
    {
        int aRows = 10_000;
        int bRows = 10;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        RIGHT,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicateLeftOuterJoin()
    {
        int aRows = 10_000;
        int bRows = 10;
        assertDetermineJoinDistributionType(new CostComparator(75, 10, 15))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        LEFT,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testFlipAndReplicateRightOuterJoin()
    {
        int aRows = 10;
        int bRows = 1_000_000;
        assertDetermineJoinDistributionType(new CostComparator(75, 10, 15))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        LEFT,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesWhenNotRestricted()
    {
        int aRows = 10_000;
        int bRows = 10;

        PlanNodeStatsEstimate probeSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000, 10)))
                .build();
        PlanNodeStatsEstimate buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000, 10)))
                .build();

        // B table is small enough to be replicated in AUTOMATIC_RESTRICTED mode
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));

        probeSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("A1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addSymbolStatistics(ImmutableMap.of(new Symbol("B1"), new SymbolStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();

        // B table exceeds AUTOMATIC_RESTRICTED limit therefore it is partitioned
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.symbol("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.symbol("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT))),
                                ImmutableList.of(p.symbol("A1", BIGINT), p.symbol("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    private RuleAssert assertDetermineJoinDistributionType()
    {
        return assertDetermineJoinDistributionType(COST_COMPARATOR);
    }

    private RuleAssert assertDetermineJoinDistributionType(CostComparator costComparator)
    {
        return tester.assertThat(new DetermineJoinDistributionType(costComparator));
    }
}
