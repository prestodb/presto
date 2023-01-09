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

import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.DistributionType;
import com.facebook.presto.sql.planner.plan.JoinNode.Type;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.enforceSingleRow;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.Lookup.noLookup;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType.getFirstKnownOutputSizeInBytes;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType.getSourceTablesSizeInBytes;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.castToRowExpression;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.FULL;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static java.lang.Double.NaN;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestDetermineJoinDistributionType
{
    private static final CostComparator COST_COMPARATOR = new CostComparator(1, 1, 1);
    private static final int NODES_COUNT = 4;

    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(ImmutableList.of(), ImmutableMap.of(), Optional.of(NODES_COUNT));
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
                                p.values(
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(
                                        ImmutableList.of(p.variable("B1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                                p.values(
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(
                                        ImmutableList.of(p.variable("B1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                                p.values(
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.enforceSingleRow(
                                        p.values(
                                                ImmutableList.of(p.variable("B1")),
                                                ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L)))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                                p.values(
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(
                                        ImmutableList.of(p.variable("B1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                ImmutableList.of(),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.of(castToRowExpression("A1 * B1 > 100"))))
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
                                p.values(
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(
                                        ImmutableList.of(p.variable("B1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(DistributionType.REPLICATED),
                                ImmutableMap.of()))
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
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
    public void testFlipAndReplicateWhenOneTableMuchSmallerAndJoinCardinalityUnknown()
    {
        int aRows = 100;
        int bRows = 10_000;
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        // set symbol stats to unknown, so the join cardinality cannot be estimated
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), VariableStatsEstimate.unknown()))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        // set symbol stats to unknown, so the join cardinality cannot be estimated
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), VariableStatsEstimate.unknown()))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                FULL,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                LEFT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
    public void testFlipAndReplicateRightOuterJoinWhenJoinCardinalityUnknown()
    {
        int aRows = 10;
        int bRows = 1_000_000;
        assertDetermineJoinDistributionType(new CostComparator(75, 10, 15))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        // set symbol stats to unknown, so the join cardinality cannot be estimated
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), VariableStatsEstimate.unknown()))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        // set symbol stats to unknown, so the join cardinality cannot be estimated
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), VariableStatsEstimate.unknown()))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 10)))
                .build();
        PlanNodeStatsEstimate buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 10)))
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
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
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
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
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
    public void testChoosesLeftWhenCriteriaEmpty()
    {
        int aRows = 1_000__00;
        int bRows = 1_0;
        assertDetermineJoinDistributionType(new CostComparator(75, 10, 15))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "1PB")
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        LEFT,
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testChoosesRightWhenFallsBackToSyntactic()
    {
        int aRows = 1_000__00;
        int bRows = 1_0;
        assertDetermineJoinDistributionType(new CostComparator(75, 10, 15))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty()))
                .matches(join(
                        RIGHT,
                        ImmutableList.of(),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testReplicatesWhenSourceIsSmall()
    {
        VarcharType variableType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 10_000;
        int bRows = 10;

        // output size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate aStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addVariableStatistics(ImmutableMap.of(
                    new VariableReferenceExpression(Optional.empty(), "A1", variableType),
                    new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        // output size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate bStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addVariableStatistics(ImmutableMap.of(
                    new VariableReferenceExpression(Optional.empty(), "B1", variableType),
                    new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        // output size does not exceed JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate bSourceStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addVariableStatistics(ImmutableMap.of(
                    new VariableReferenceExpression(Optional.empty(), "B1", variableType),
                    new VariableStatsEstimate(0, 100, 0, 64, 10)))
                .build();

        // immediate join sources exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit but build tables are small
        // therefore replicated distribution type is chosen
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("filterB", bStatsEstimate)
                .overrideStats("valuesB", bSourceStatsEstimate)
                .on(p -> {
                    VariableReferenceExpression a1 = p.variable("A1", variableType);
                    VariableReferenceExpression b1 = p.variable("B1", variableType);
                    return p.join(
                        INNER,
                        p.values(new PlanNodeId("valuesA"), aRows, a1),
                        p.filter(new PlanNodeId("filterB"), TRUE_CONSTANT, p.values(new PlanNodeId("valuesB"), bRows, b1)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                        ImmutableList.of(a1, b1),
                        Optional.empty());
                })
                .matches(join(
                    INNER,
                    ImmutableList.of(equiJoinClause("A1", "B1")),
                    Optional.empty(),
                    Optional.of(REPLICATED),
                    values(ImmutableMap.of("A1", 0)),
                    filter("true", values(ImmutableMap.of("B1", 0)))));

        // same but with join sides reversed
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("filterB", bStatsEstimate)
                .overrideStats("valuesB", bSourceStatsEstimate)
                .on(p -> {
                    VariableReferenceExpression a1 = p.variable("A1", variableType);
                    VariableReferenceExpression b1 = p.variable("B1", variableType);
                    return p.join(
                        INNER,
                        p.filter(new PlanNodeId("filterB"), TRUE_CONSTANT, p.values(new PlanNodeId("valuesB"), bRows, b1)),
                        p.values(new PlanNodeId("valuesA"), aRows, a1),
                        ImmutableList.of(new JoinNode.EquiJoinClause(b1, a1)),
                        ImmutableList.of(b1, a1),
                        Optional.empty());
                })
                .matches(join(
                    INNER,
                    ImmutableList.of(equiJoinClause("A1", "B1")),
                    Optional.empty(),
                    Optional.of(REPLICATED),
                    values(ImmutableMap.of("A1", 0)),
                    filter("true", values(ImmutableMap.of("B1", 0)))));

        // only probe side (with small tables) source stats are available, join sides should be flipped
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", PlanNodeStatsEstimate.unknown())
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown())
                .overrideStats("valuesB", bSourceStatsEstimate)
                .on(p -> {
                    VariableReferenceExpression a1 = p.variable("A1", variableType);
                    VariableReferenceExpression b1 = p.variable("B1", variableType);
                    return p.join(
                        LEFT,
                        p.filter(new PlanNodeId("filterB"), TRUE_CONSTANT, p.values(new PlanNodeId("valuesB"), bRows, b1)),
                        p.values(new PlanNodeId("valuesA"), aRows, a1),
                        ImmutableList.of(new JoinNode.EquiJoinClause(b1, a1)),
                        ImmutableList.of(b1, a1),
                        Optional.empty());
                })
                .matches(join(
                    RIGHT,
                    ImmutableList.of(equiJoinClause("A1", "B1")),
                    Optional.empty(),
                    Optional.of(PARTITIONED),
                    values(ImmutableMap.of("A1", 0)),
                    filter("true", values(ImmutableMap.of("B1", 0)))));
    }

    @Test
    public void testFlipWhenSizeDifferenceLarge()
    {
        VarcharType variableType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 10_000;
        int bRows = 1_000;

        // output size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate aStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addVariableStatistics(ImmutableMap.of(
                    new VariableReferenceExpression(Optional.empty(), "A1", variableType),
                    new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        // output size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate bStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addVariableStatistics(ImmutableMap.of(
                    new VariableReferenceExpression(Optional.empty(), "B1", variableType),
                    new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();

        // source tables size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit but one side is significantly bigger than the other
        // therefore repartitioned distribution type is chosen with the smaller side on build
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("valuesB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    VariableReferenceExpression a1 = p.variable("A1", variableType);
                    VariableReferenceExpression b1 = p.variable("B1", variableType);
                    return p.join(
                        INNER,
                        p.values(new PlanNodeId("valuesA"), aRows, a1),
                        p.filter(
                            new PlanNodeId("filterB"),
                            TRUE_CONSTANT,
                            p.values(new PlanNodeId("valuesB"), bRows, b1)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(a1, b1)),
                        ImmutableList.of(a1, b1),
                        Optional.empty());
                })
                .matches(join(
                    INNER,
                    ImmutableList.of(equiJoinClause("A1", "B1")),
                    Optional.empty(),
                    Optional.of(PARTITIONED),
                    values(ImmutableMap.of("A1", 0)),
                    filter("true", values(ImmutableMap.of("B1", 0)))));

        // same but with join sides reversed
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("valuesB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    VariableReferenceExpression a1 = p.variable("A1", variableType);
                    VariableReferenceExpression b1 = p.variable("B1", variableType);
                    return p.join(
                        INNER,
                        p.filter(
                            new PlanNodeId("filterB"),
                            TRUE_CONSTANT,
                            p.values(new PlanNodeId("valuesB"), bRows, b1)),
                        p.values(new PlanNodeId("valuesA"), aRows, a1),
                        ImmutableList.of(new JoinNode.EquiJoinClause(b1, a1)),
                        ImmutableList.of(b1, a1),
                        Optional.empty());
                })
                .matches(join(
                    INNER,
                    ImmutableList.of(equiJoinClause("A1", "B1")),
                    Optional.empty(),
                    Optional.of(PARTITIONED),
                    values(ImmutableMap.of("A1", 0)),
                    filter("true", values(ImmutableMap.of("B1", 0)))));

        // Use REPLICATED join type for cross join
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("valuesB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    VariableReferenceExpression a1 = p.variable("A1", variableType);
                    VariableReferenceExpression b1 = p.variable("B1", variableType);
                    return p.join(
                        INNER,
                        p.filter(
                            new PlanNodeId("filterB"),
                            TRUE_CONSTANT,
                            p.values(new PlanNodeId("valuesB"), bRows, b1)),
                        p.values(new PlanNodeId("valuesA"), aRows, a1),
                        ImmutableList.of(),
                        ImmutableList.of(b1, a1),
                        Optional.empty());
                })
                .matches(join(
                    INNER,
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.of(REPLICATED),
                    filter("true", values(ImmutableMap.of("B1", 0))),
                    values(ImmutableMap.of("A1", 0))));

        // Don't flip sides when both are similar in size
        bStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addVariableStatistics(ImmutableMap.of(
                    new VariableReferenceExpression(Optional.empty(), "B1", variableType),
                    new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        assertDetermineJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", aStatsEstimate)
                .overrideStats("valuesB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    VariableReferenceExpression a1 = p.variable("A1", variableType);
                    VariableReferenceExpression b1 = p.variable("B1", variableType);
                    return p.join(
                        INNER,
                        p.filter(
                            new PlanNodeId("filterB"),
                            TRUE_CONSTANT,
                            p.values(new PlanNodeId("valuesB"), aRows, b1)),
                        p.values(new PlanNodeId("valuesA"), aRows, a1),
                        ImmutableList.of(new JoinNode.EquiJoinClause(b1, a1)),
                        ImmutableList.of(b1, a1),
                        Optional.empty());
                })
                .matches(join(
                    INNER,
                    ImmutableList.of(equiJoinClause("B1", "A1")),
                    Optional.empty(),
                    Optional.of(PARTITIONED),
                    filter("true", values(ImmutableMap.of("B1", 0))),
                    values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testGetSourceTablesSizeInBytes()
    {
        PlanBuilder planBuilder = new PlanBuilder(tester.getSession(), new PlanNodeIdAllocator(), tester.getMetadata());
        VariableReferenceExpression variable = planBuilder.variable("col");
        VariableReferenceExpression sourceVariable1 = planBuilder.variable("source1");
        VariableReferenceExpression sourceVariable2 = planBuilder.variable("soruce2");

        // missing source stats
        assertEquals(
                getSourceTablesSizeInBytes(
                        planBuilder.values(variable),
                        noLookup(),
                        node -> PlanNodeStatsEstimate.unknown()),
                NaN);

        // two source plan nodes
        PlanNodeStatsEstimate sourceStatsEstimate1 = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10)
                .build();
        PlanNodeStatsEstimate sourceStatsEstimate2 = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(20)
                .build();
        assertEquals(
                getSourceTablesSizeInBytes(
                        planBuilder.union(
                                ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                        .put(variable, sourceVariable1)
                                        .put(variable, sourceVariable2)
                                        .build(),
                                ImmutableList.of(planBuilder.tableScan(
                                        ImmutableList.of(sourceVariable1),
                                        ImmutableMap.of(sourceVariable1, new TestingColumnHandle("col"))),
                                        planBuilder.values(new PlanNodeId("valuesNode"), sourceVariable2))),
                        noLookup(),
                        node -> {
                            if (node instanceof TableScanNode) {
                                return sourceStatsEstimate1;
                            }

                            if (node instanceof ValuesNode) {
                                return sourceStatsEstimate2;
                            }

                            return PlanNodeStatsEstimate.unknown();
                        }),
                270.0);

        // join node
        assertEquals(
                getSourceTablesSizeInBytes(
                        planBuilder.join(
                                INNER,
                                planBuilder.values(sourceVariable1),
                                planBuilder.values(sourceVariable2)),
                        noLookup(),
                        node -> sourceStatsEstimate1),
                NaN);

        // unnest node
        assertEquals(
                getSourceTablesSizeInBytes(
                        planBuilder.unnest(
                                planBuilder.values(sourceVariable1),
                                ImmutableList.of(),
                                ImmutableMap.of(sourceVariable1, ImmutableList.of(sourceVariable1)),
                                Optional.empty()),
                        noLookup(),
                        node -> sourceStatsEstimate1),
                NaN);
    }

    @Test
    public void testGetApproximateSourceSizeInBytes()
    {
        PlanBuilder planBuilder = new PlanBuilder(tester.getSession(), new PlanNodeIdAllocator(), tester.getMetadata());
        VariableReferenceExpression variable = planBuilder.variable("col");
        VariableReferenceExpression sourceVariable1 = planBuilder.variable("source1");
        VariableReferenceExpression sourceVariable2 = planBuilder.variable("source2");

        // missing source stats
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.values(variable),
                        noLookup(),
                        node -> PlanNodeStatsEstimate.unknown()),
                NaN);

        // two source plan nodes
        PlanNodeStatsEstimate sourceStatsEstimate1 = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1000)
                .build();
        PlanNodeStatsEstimate sourceStatsEstimate2 = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(2000)
                .build();
        PlanNodeStatsEstimate filterStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(250)
                .build();
        PlanNodeStatsEstimate limitStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(20)
                .build();
        double sourceRowCount = sourceStatsEstimate1.getOutputRowCount() + sourceStatsEstimate2.getOutputRowCount();
        double unionInputRowCount = filterStatsEstimate.getOutputRowCount() + limitStatsEstimate.getOutputRowCount();
        double sourceSizeInBytes = sourceRowCount + sourceRowCount * BIGINT.getFixedSize();
        // un-estimated union with non-expanding source
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.union(
                                ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                        .put(variable, sourceVariable1)
                                        .put(variable, sourceVariable2)
                                        .build(),
                                ImmutableList.of(
                                        planBuilder.filter(
                                                TRUE_CONSTANT,
                                                planBuilder.tableScan(
                                                        ImmutableList.of(sourceVariable1),
                                                        ImmutableMap.of(sourceVariable1, new TestingColumnHandle("col")))),
                                        planBuilder.limit(20, planBuilder.values(sourceVariable2)))),
                        noLookup(),
                        node -> {
                            if (node instanceof TableScanNode) {
                                return sourceStatsEstimate1;
                            }
                            if (node instanceof FilterNode) {
                                return filterStatsEstimate;
                            }
                            if (node instanceof ValuesNode) {
                                return sourceStatsEstimate2;
                            }
                            if (node instanceof LimitNode) {
                                return limitStatsEstimate;
                            }

                            return PlanNodeStatsEstimate.unknown();
                        }),
                (unionInputRowCount / sourceRowCount) * sourceSizeInBytes);

        // join node with known estimate
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.join(
                                INNER,
                                planBuilder.values(sourceVariable1),
                                planBuilder.values(sourceVariable2)),
                        noLookup(),
                        node -> sourceStatsEstimate1),
                sourceStatsEstimate1.getOutputRowCount() * 2 * (BIGINT.getFixedSize() + 1));

        // un-estimated join with non-expanding source
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.join(
                                INNER,
                                planBuilder.tableScan(
                                        ImmutableList.of(sourceVariable1),
                                        ImmutableMap.of(sourceVariable1, new TestingColumnHandle("col"))),
                                planBuilder.values(sourceVariable2)),
                        noLookup(),
                        node -> {
                            if (node instanceof TableScanNode) {
                                return sourceStatsEstimate1;
                            }
                            if (node instanceof ValuesNode) {
                                return sourceStatsEstimate2;
                            }

                            return PlanNodeStatsEstimate.unknown();
                        }),
                NaN);

        // un-estimated union with estimated expanding source
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.union(
                                ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                        .put(variable, sourceVariable1)
                                        .put(variable, sourceVariable2)
                                        .build(),
                                ImmutableList.of(
                                        planBuilder.unnest(
                                                planBuilder.values(sourceVariable1),
                                                ImmutableList.of(),
                                                ImmutableMap.of(sourceVariable1, ImmutableList.of(sourceVariable1)),
                                                Optional.empty()),
                                        planBuilder.values(sourceVariable2))),
                        noLookup(),
                        node -> {
                            if (node instanceof UnnestNode) {
                                return sourceStatsEstimate1;
                            }
                            if (node instanceof ValuesNode) {
                                return sourceStatsEstimate2;
                            }

                            return PlanNodeStatsEstimate.unknown();
                        }),
                sourceSizeInBytes);

        // un-estimated union with un-estimated expanding source
        assertEquals(
                getFirstKnownOutputSizeInBytes(
                        planBuilder.union(
                                ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                        .put(variable, sourceVariable1)
                                        .put(variable, sourceVariable2)
                                        .build(),
                                ImmutableList.of(
                                        planBuilder.unnest(
                                                planBuilder.values(sourceVariable1),
                                                ImmutableList.of(),
                                                ImmutableMap.of(sourceVariable1, ImmutableList.of(sourceVariable1)),
                                                Optional.empty()),
                                        planBuilder.values(sourceVariable2))),
                        noLookup(),
                        node -> {
                            if (node instanceof ValuesNode) {
                                return sourceStatsEstimate2;
                            }

                            return PlanNodeStatsEstimate.unknown();
                        }),
                NaN);
    }

    private RuleAssert assertDetermineJoinDistributionType()
    {
        return assertDetermineJoinDistributionType(COST_COMPARATOR);
    }

    private RuleAssert assertDetermineJoinDistributionType(CostComparator costComparator)
    {
        return tester.assertThat(new DetermineJoinDistributionType(costComparator, new TaskCountEstimator(() -> NODES_COUNT)));
    }
}
