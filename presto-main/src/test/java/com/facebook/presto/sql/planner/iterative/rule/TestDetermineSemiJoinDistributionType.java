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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;
import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.SemiJoinNode.DistributionType.REPLICATED;

@Test(singleThreaded = true)
public class TestDetermineSemiJoinDistributionType
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
    public void testRetainDistributionType()
    {
        assertDetermineSemiJoinDistributionType()
                .on(p ->
                        p.semiJoin(
                                p.values(
                                        ImmutableList.of(p.variable("A1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 10L), constantExpressions(BIGINT, 11L))),
                                p.values(
                                        ImmutableList.of(p.variable("B1")),
                                        ImmutableList.of(constantExpressions(BIGINT, 50L), constantExpressions(BIGINT, 11L))),
                                p.variable("A1"),
                                p.variable("B1"),
                                p.variable("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(REPLICATED)))
                .doesNotFire();
    }

    @Test
    public void testPartitionWhenRequiredBySession()
    {
        int aRows = 10_000;
        int bRows = 100;
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 6400, 100)))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 100)))
                        .build())
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                p.variable("A1"),
                                p.variable("B1"),
                                p.variable("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesWhenRequiredBySession()
    {
        int aRows = 10_000;
        int bRows = 10_000;
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.BROADCAST.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "1B")
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), VariableStatsEstimate.unknown()))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), VariableStatsEstimate.unknown()))
                        .build())
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                p.variable("A1"),
                                p.variable("B1"),
                                p.variable("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(REPLICATED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testPartitionsWhenBothTablesEqual()
    {
        int aRows = 10_000;
        int bRows = 10_000;
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), VariableStatsEstimate.unknown()))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), VariableStatsEstimate.unknown()))
                        .build())
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                p.variable("A1"),
                                p.variable("B1"),
                                p.variable("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    @Test
    public void testReplicatesWhenFilterMuchSmaller()
    {
        int aRows = 10_000;
        int bRows = 100;
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(aRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), VariableStatsEstimate.unknown()))
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(bRows)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), VariableStatsEstimate.unknown()))
                        .build())
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                p.variable("A1"),
                                p.variable("B1"),
                                p.variable("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
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
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("A1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 10)))
                .build();
        PlanNodeStatsEstimate buildSideStatsEstimate = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(bRows)
                .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("B1", BIGINT), new VariableStatsEstimate(0, 100, 0, 640000, 10)))
                .build();

        // B table is small enough to be replicated in AUTOMATIC_RESTRICTED mode
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                p.variable("A1"),
                                p.variable("B1"),
                                p.variable("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
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
        assertDetermineSemiJoinDistributionType()
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.AUTOMATIC.name())
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "100MB")
                .overrideStats("valuesA", probeSideStatsEstimate)
                .overrideStats("valuesB", buildSideStatsEstimate)
                .on(p ->
                        p.semiJoin(
                                p.values(new PlanNodeId("valuesA"), aRows, p.variable("A1", BIGINT)),
                                p.values(new PlanNodeId("valuesB"), bRows, p.variable("B1", BIGINT)),
                                p.variable("A1"),
                                p.variable("B1"),
                                p.variable("output"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()))
                .matches(semiJoin(
                        "A1",
                        "B1",
                        "output",
                        Optional.of(PARTITIONED),
                        values(ImmutableMap.of("A1", 0)),
                        values(ImmutableMap.of("B1", 0))));
    }

    private RuleAssert assertDetermineSemiJoinDistributionType()
    {
        return assertDetermineSemiJoinDistributionType(COST_COMPARATOR);
    }

    private RuleAssert assertDetermineSemiJoinDistributionType(CostComparator costComparator)
    {
        return tester.assertThat(new DetermineSemiJoinDistributionType(costComparator, new TaskCountEstimator(() -> NODES_COUNT)));
    }
}
