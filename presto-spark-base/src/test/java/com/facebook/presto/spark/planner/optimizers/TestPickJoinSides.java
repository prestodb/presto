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
package com.facebook.presto.spark.planner.optimizers;

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spark.PrestoSparkSessionProperties;
import com.facebook.presto.spark.PrestoSparkSessionPropertyManagerProvider;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.spark.PrestoSparkSessionProperties.ADAPTIVE_JOIN_SIDE_SWITCHING_ENABLED;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.remoteSource;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;
import static com.facebook.presto.sql.planner.plan.JoinNode.flipType;
import static com.facebook.presto.testing.TestngUtils.toDataProvider;

@Test(singleThreaded = true)
public class TestPickJoinSides
{
    private static final int NODES_COUNT = 4;

    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(
                ImmutableList.of(),
                ImmutableMap.of(),
                new PrestoSparkSessionPropertyManagerProvider(new SystemSessionProperties(), new PrestoSparkSessionProperties()).get(),
                Optional.of(NODES_COUNT),
                new TpchConnectorFactory(1));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
    }

    @DataProvider(name = "joinTypes")
    public static Object[][] joinTypes()
    {
        return Arrays.stream(JoinType.values())
                .collect(toDataProvider());
    }

    @Test(dataProvider = "joinTypes")
    public void testFlipsWhenProbeSmaller(JoinType joinType)
    {
        int aSize = 100;
        int bSize = 10_000;
        assertPickJoinSides()
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setTotalSize(aSize)
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setTotalSize(bSize)
                        .build())
                .on(p ->
                        p.join(
                                joinType,
                                p.values(
                                        new PlanNodeId("valuesA"),
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(
                                        new PlanNodeId("valuesB"),
                                        ImmutableList.of(p.variable("B1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                ImmutableMap.of()))
                .matches(join(
                        flipType(joinType),
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test(dataProvider = "joinTypes")
    public void testDoesNotFireWhenTablesSameSize(JoinType joinType)
    {
        int aSize = 100;
        int bSize = 100;
        assertPickJoinSides()
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setTotalSize(aSize)
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setTotalSize(bSize)
                        .build())
                .on(p ->
                        p.join(
                                joinType,
                                p.values(
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(
                                        ImmutableList.of(p.variable("B1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                ImmutableMap.of()))
                .doesNotFire();
    }

    @Test(dataProvider = "joinTypes")
    public void testFlipWhenOneTableMuchSmallerAndJoinCardinalityUnknown(JoinType joinType)
    {
        int aRows = 100;
        int bRows = 10_000;
        assertPickJoinSides()
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
                                joinType,
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                ImmutableMap.of()))
                .matches(join(
                        flipType(joinType),
                        ImmutableList.of(equiJoinClause("B1", "A1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("B1", 0)),
                        values(ImmutableMap.of("A1", 0))));
    }

    @Test
    public void testFlipsWhenSourceIsSmall()
    {
        VarcharType variableType = createUnboundedVarcharType(); // variable width so that average row size is respected
        int aRows = 10_000;
        int bRows = 10;

        // output size does not exceed JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate bSourceStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addVariableStatistics(ImmutableMap.of(
                        new VariableReferenceExpression(Optional.empty(), "B1", variableType),
                        new VariableStatsEstimate(0, 100, 0, 64, 10)))
                .build();

        // only probe side (with small tables) source stats are available, join sides should be flipped
        assertPickJoinSides()
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
                            ImmutableList.of(new EquiJoinClause(b1, a1)),
                            ImmutableList.of(b1, a1),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of(PARTITIONED),
                            ImmutableMap.of());
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
                .setTotalSize(aRows)
                .addVariableStatistics(ImmutableMap.of(
                        new VariableReferenceExpression(Optional.empty(), "A1", variableType),
                        new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        // output size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit
        PlanNodeStatsEstimate bStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .setTotalSize(bRows)
                .addVariableStatistics(ImmutableMap.of(
                        new VariableReferenceExpression(Optional.empty(), "B1", variableType),
                        new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();

        // source tables size exceeds JOIN_MAX_BROADCAST_TABLE_SIZE limit but one side is significantly bigger than the other
        // therefore we keep the smaller side to the build
        assertPickJoinSides()
                .overrideStats("remoteSourceA", aStatsEstimate)
                .overrideStats("remoteSourceB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    VariableReferenceExpression a1 = p.variable("A1", variableType);
                    VariableReferenceExpression b1 = p.variable("B1", variableType);
                    return p.join(
                            INNER,
                            p.remoteSource(new PlanNodeId("remoteSourceA"), ImmutableList.of(new PlanFragmentId(1)), ImmutableList.of(a1)),
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_CONSTANT,
                                    p.remoteSource(new PlanNodeId("remoteSourceB"), ImmutableList.of(new PlanFragmentId(2)), ImmutableList.of(b1))),
                            ImmutableList.of(new EquiJoinClause(a1, b1)),
                            ImmutableList.of(a1, b1),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of(PARTITIONED),
                            ImmutableMap.of());
                })
                .doesNotFire();

        // same but with join sides reversed
        assertPickJoinSides()
                .setSystemProperty(TASK_CONCURRENCY, "2")
                .overrideStats("remoteSourceA", aStatsEstimate)
                .overrideStats("remoteSourceB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    VariableReferenceExpression a1 = p.variable("A1", variableType);
                    VariableReferenceExpression b1 = p.variable("B1", variableType);
                    return p.join(
                            INNER,
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_CONSTANT,
                                    p.remoteSource(new PlanNodeId("remoteSourceB"), ImmutableList.of(new PlanFragmentId(2)), ImmutableList.of(b1))),
                            p.exchange(e -> e.scope(ExchangeNode.Scope.LOCAL)
                                    .fixedHashDistributionPartitioningScheme(ImmutableList.of(a1), ImmutableList.of(a1))
                                    .addInputsSet(a1)
                                    .addSource(p.remoteSource(new PlanNodeId("remoteSourceA"), ImmutableList.of(new PlanFragmentId(1)), ImmutableList.of(a1)))),
                            ImmutableList.of(new EquiJoinClause(b1, a1)),
                            ImmutableList.of(b1, a1),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of(PARTITIONED),
                            ImmutableMap.of());
                })
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("A1", "B1")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        remoteSource(ImmutableList.of(new PlanFragmentId(1)), ImmutableMap.of("A1", 0)),
                        exchange(ExchangeNode.Scope.LOCAL, ExchangeNode.Type.REPARTITION,
                                filter("true",
                                        remoteSource(ImmutableList.of(new PlanFragmentId(2)), ImmutableMap.of("B1", 0))))));

        // Don't flip sides when both are similar in size
        bStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(aRows)
                .addVariableStatistics(ImmutableMap.of(
                        new VariableReferenceExpression(Optional.empty(), "B1", variableType),
                        new VariableStatsEstimate(0, 100, 0, 640000d * 10000, 10)))
                .build();
        assertPickJoinSides()
                .overrideStats("remoteSourceA", aStatsEstimate)
                .overrideStats("remoteSourceB", bStatsEstimate)
                .overrideStats("filterB", PlanNodeStatsEstimate.unknown()) // unestimated term to trigger size based join ordering
                .on(p -> {
                    VariableReferenceExpression a1 = p.variable("A1", variableType);
                    VariableReferenceExpression b1 = p.variable("B1", variableType);
                    return p.join(
                            INNER,
                            p.filter(
                                    new PlanNodeId("filterB"),
                                    TRUE_CONSTANT,
                                    p.remoteSource(new PlanNodeId("remoteSourceB"), ImmutableList.of(new PlanFragmentId(2)), ImmutableList.of(b1))),
                            p.remoteSource(new PlanNodeId("remoteSourceA"), ImmutableList.of(new PlanFragmentId(1)), ImmutableList.of(a1)),
                            ImmutableList.of(new EquiJoinClause(b1, a1)),
                            ImmutableList.of(b1, a1),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of(PARTITIONED),
                            ImmutableMap.of());
                })
                .doesNotFire();
    }

    public void testDoesNotFireWhenDisabled()
    {
        int aSize = 100;
        int bSize = 10_000;
        tester.assertThat(new PickJoinSides(tester.getMetadata()))
                .setSystemProperty(ADAPTIVE_JOIN_SIDE_SWITCHING_ENABLED, "false")
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setTotalSize(aSize)
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setTotalSize(bSize)
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(
                                        ImmutableList.of(p.variable("B1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                ImmutableMap.of()))
                .doesNotFire();
    }

    public void testDoesNotFireForReplicatedJoin()
    {
        int aSize = 100;
        int bSize = 10_000;
        assertPickJoinSides()
                .setSystemProperty(ADAPTIVE_JOIN_SIDE_SWITCHING_ENABLED, "false")
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setTotalSize(aSize)
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setTotalSize(bSize)
                        .build())
                .on(p ->
                        p.join(
                                INNER,
                                p.values(
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(
                                        ImmutableList.of(p.variable("B1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                ImmutableList.of(new EquiJoinClause(p.variable("A1", BIGINT), p.variable("B1", BIGINT))),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(REPLICATED),
                                ImmutableMap.of()))
                .doesNotFire();
    }

    public void testDoesNotFireForRightCrossJoin()
    {
        int aSize = 100;
        int bSize = 10_000;
        assertPickJoinSides()
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setTotalSize(aSize)
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setTotalSize(bSize)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression(Optional.empty(), "B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.join(
                                RIGHT,
                                p.values(
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(
                                        ImmutableList.of(p.variable("B1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                ImmutableList.of(),
                                ImmutableList.of(p.variable("A1", BIGINT), p.variable("B1", BIGINT)),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(PARTITIONED),
                                ImmutableMap.of()))
                .doesNotFire();
    }

    private RuleAssert assertPickJoinSides()
    {
        return tester.assertThat(new PickJoinSides(tester.getMetadata()))
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .setSystemProperty(ADAPTIVE_JOIN_SIDE_SWITCHING_ENABLED, "true");
    }
}
