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

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.GroupIdMatcher;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.ADD_EXCHANGE_BELOW_PARTIAL_AGGREGATION_OVER_GROUP_ID;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestAddExchangesBelowPartialAggregationOverGroupIdRuleSet
        extends BaseRuleTest
{
    private static AddExchangesBelowPartialAggregationOverGroupIdRuleSet.AddExchangesBelowExchangePartialAggregationGroupId belowExchangeRule(RuleTester ruleTester)
    {
        TaskCountEstimator taskCountEstimator = new TaskCountEstimator(() -> 4);
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig();
        return new AddExchangesBelowPartialAggregationOverGroupIdRuleSet(
                taskCountEstimator,
                taskManagerConfig,
                ruleTester.getMetadata(),
                false
        ).belowExchangeRule();
    }

    private static AddExchangesBelowPartialAggregationOverGroupIdRuleSet.AddExchangesBelowProjectionPartialAggregationGroupId belowProjectionRule(RuleTester ruleTester)
    {
        TaskCountEstimator taskCountEstimator = new TaskCountEstimator(() -> 4);
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig();
        return new AddExchangesBelowPartialAggregationOverGroupIdRuleSet(
                taskCountEstimator,
                taskManagerConfig,
                ruleTester.getMetadata(),
                false
        ).belowProjectionRule();
    }

    @DataProvider
    public static Object[][] testDataProvider()
    {
        return new Object[][] {
                {1000.0, 10_000.0, 1_000_000.0, "groupingKey3"},
                {1000.0, 2_000_000.0, 1_000_000.0, "groupingKey2"},
                {1000.0, 1000.0, 1000.0, "groupingKey1"}
        };
    }

    @DataProvider
    public static Object[][] testDataProviderMissingStats()
    {
        return new Object[][] {
                {Double.NaN, 10_000.0, 1_000_000.0},
                {1000.0, Double.NaN, 1_000_000.0},
                {1000.0, 10_000.0, Double.NaN}
        };
    }

    @Test(dataProvider = "testDataProvider")
    public void testAddExchangesWithoutProjection(double groupingKey1NDV, double groupingKey2NDV, double groupingKey3NDV, String expectedRepartitionSymbol)
    {
        buildRuleAssert(groupingKey1NDV, groupingKey2NDV, groupingKey3NDV, false)
                .matches(exchange(
                        REMOTE_STREAMING,
                        GATHER,
                        aggregation(
                                singleGroupingSet(ImmutableList.of("groupingKey1", "groupingKey2", "groupingKey3", "groupId")),
                                ImmutableMap.of(),
                                ImmutableList.of(),
                                ImmutableMap.of(),
                                Optional.empty(),
                                PARTIAL,
                                node(GroupIdNode.class,
                                        exchange(
                                                LOCAL,
                                                REPARTITION,
                                                ImmutableList.of(),
                                                ImmutableSet.of(expectedRepartitionSymbol),
                                                exchange(
                                                        REMOTE_STREAMING,
                                                        REPARTITION,
                                                        ImmutableList.of(),
                                                        ImmutableSet.of(expectedRepartitionSymbol),
                                                        values("groupingKey1", "groupingKey2", "groupingKey3"))))
                                        .with(new GroupIdMatcher(ImmutableList.of(
                                                ImmutableList.of("groupingKey1", "groupingKey2"),
                                                ImmutableList.of("groupingKey1", "groupingKey3")), ImmutableMap.of(), "groupId")))));
    }

    @Test(dataProvider = "testDataProvider")
    public void testAddExchangesWithProjection(double groupingKey1NDV, double groupingKey2NDV, double groupingKey3NDV, String expectedRepartitionSymbol)
    {
        buildRuleAssert(groupingKey1NDV, groupingKey2NDV, groupingKey3NDV, true)
                .matches(exchange(
                        REMOTE_STREAMING,
                        GATHER,
                        project(
                                ImmutableMap.of(
                                        "groupingKey1", expression("groupingKey1"),
                                        "groupingKey2", expression("groupingKey2"),
                                        "groupingKey3", expression("groupingKey3")),
                                aggregation(
                                        singleGroupingSet(ImmutableList.of("groupingKey1", "groupingKey2", "groupingKey3", "groupId")),
                                        ImmutableMap.of(),
                                        ImmutableList.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        node(GroupIdNode.class,
                                                exchange(
                                                        LOCAL,
                                                        REPARTITION,
                                                        ImmutableList.of(),
                                                        ImmutableSet.of(expectedRepartitionSymbol),
                                                        exchange(
                                                                REMOTE_STREAMING,
                                                                REPARTITION,
                                                                ImmutableList.of(),
                                                                ImmutableSet.of(expectedRepartitionSymbol),
                                                                values("groupingKey1", "groupingKey2", "groupingKey3"))))
                                                .with(new GroupIdMatcher(ImmutableList.of(
                                                        ImmutableList.of("groupingKey1", "groupingKey2"),
                                                        ImmutableList.of("groupingKey1", "groupingKey3")), ImmutableMap.of(), "groupId"))))));
    }

    @Test(dataProvider = "testDataProviderMissingStats")
    public void testDoesNotFireIfAnySourceSymbolIsMissingStats(double groupingKey1NDV, double groupingKey2NDV, double groupingKey3NDV)
    {
        buildRuleAssert(groupingKey1NDV, groupingKey2NDV, groupingKey3NDV, true).doesNotFire();
        buildRuleAssert(groupingKey1NDV, groupingKey2NDV, groupingKey3NDV, false).doesNotFire();
    }

    @Test
    public void testDoesNotFireIfSessionPropertyIsDisabled()
    {
        buildRuleAssert(1000D, 1000D, 1000D, false)
                .setSystemProperty(ADD_EXCHANGE_BELOW_PARTIAL_AGGREGATION_OVER_GROUP_ID, "false")
                .doesNotFire();
    }

    private RuleAssert buildRuleAssert(double groupingKey1NDV, double groupingKey2NDV, double groupingKey3NDV, boolean withProjection)
    {
        RuleTester ruleTester = tester();
        String groupIdSourceId = "groupIdSourceId";
        return ruleTester.assertThat(withProjection ? belowProjectionRule(ruleTester) : belowExchangeRule(ruleTester))
                .setSystemProperty(ADD_EXCHANGE_BELOW_PARTIAL_AGGREGATION_OVER_GROUP_ID, "true")
                .overrideStats(groupIdSourceId, PlanNodeStatsEstimate
                        .builder()
                        .setOutputRowCount(100_000_000)
                        .addVariableStatistics(ImmutableMap.of(
                                new VariableReferenceExpression(Optional.empty(), "groupingKey1", BIGINT), VariableStatsEstimate.builder().setDistinctValuesCount(groupingKey1NDV).build(),
                                new VariableReferenceExpression(Optional.empty(), "groupingKey2", BIGINT), VariableStatsEstimate.builder().setDistinctValuesCount(groupingKey2NDV).build(),
                                new VariableReferenceExpression(Optional.empty(), "groupingKey3", BIGINT), VariableStatsEstimate.builder().setDistinctValuesCount(groupingKey3NDV).build()))
                        .build())
                .on(p -> {
                    VariableReferenceExpression groupingKey1 = p.variable("groupingKey1", BIGINT);
                    VariableReferenceExpression groupingKey2 = p.variable("groupingKey2", BIGINT);
                    VariableReferenceExpression groupingKey3 = p.variable("groupingKey3", BIGINT);
                    VariableReferenceExpression groupId = p.variable("groupId", BIGINT);

                    PlanNode partialAgg = p.aggregation(builder -> builder
                            .singleGroupingSet(groupingKey1, groupingKey2, groupingKey3, groupId)
                            .step(PARTIAL)
                            .source(p.groupId(
                                    ImmutableList.of(
                                            ImmutableList.of(groupingKey1, groupingKey2),
                                            ImmutableList.of(groupingKey1, groupingKey3)),
                                    ImmutableList.of(),
                                    groupId,
                                    p.values(new PlanNodeId(groupIdSourceId), groupingKey1, groupingKey2, groupingKey3))));

                    return p.exchange(
                            exchangeBuilder -> exchangeBuilder
                                    .scope(REMOTE_STREAMING)
                                    .partitioningScheme(new PartitioningScheme(Partitioning.create(
                                            FIXED_ARBITRARY_DISTRIBUTION,
                                            ImmutableList.of()),
                                            ImmutableList.copyOf(ImmutableList.of(groupingKey1, groupingKey2, groupingKey3, groupId))))
                                    .addInputsSet(groupingKey1, groupingKey2, groupingKey3, groupId)
                                    .addSource(withProjection ? p.project(identityAssignments(partialAgg.getOutputVariables()), partialAgg) : partialAgg));
                });
    }
}
