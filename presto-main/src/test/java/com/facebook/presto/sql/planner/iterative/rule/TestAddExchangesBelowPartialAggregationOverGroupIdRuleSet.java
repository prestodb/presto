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
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
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
                ruleTester.getSqlParser()).belowExchangeRule();
    }

    @Test
    public void testAddExchangesWithoutProjection()
    {
        testAddExchangesWithoutProjection(1000, 10_000, 1_000_000, ImmutableSet.of("groupingKey3"));
        testAddExchangesWithoutProjection(1000, 1000, 1000, ImmutableSet.of("groupingKey1"));
        // stats not available on any symbol make the rule to not fire
        testAddExchangesWithoutProjection(1000, 10_000, Double.NaN, ImmutableSet.of());
        testAddExchangesWithoutProjection(1000, Double.NaN, 10_000, ImmutableSet.of());
        testAddExchangesWithoutProjection(1000, 10_000, Double.NaN, ImmutableSet.of());
    }

    // empty partitionedBy means exchanges should not be added
    private void testAddExchangesWithoutProjection(double groupingKey1NDV, double groupingKey2NDV, double groupingKey3NDV, Set<String> partitionedBy)
    {
        RuleTester ruleTester = tester();
        String groupIdSourceId = "groupIdSourceId";
        RuleAssert ruleAssert = ruleTester.assertThat(belowExchangeRule(ruleTester))
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
                    return p.exchange(
                            exchangeBuilder -> exchangeBuilder
                                    .scope(REMOTE_STREAMING)
                                    .partitioningScheme(new PartitioningScheme(Partitioning.create(
                                            FIXED_ARBITRARY_DISTRIBUTION,
                                            ImmutableList.of()),
                                            ImmutableList.copyOf(ImmutableList.of(groupingKey1, groupingKey2, groupingKey3, groupId))))
                                    .addInputsSet(groupingKey1, groupingKey2, groupingKey3, groupId)
                                    .addSource(
                                            p.aggregation(builder -> builder
                                                    .singleGroupingSet(groupingKey1, groupingKey2, groupingKey3, groupId)
                                                    .step(PARTIAL)
                                                    .source(p.groupId(
                                                            ImmutableList.of(
                                                                    ImmutableList.of(groupingKey1, groupingKey2),
                                                                    ImmutableList.of(groupingKey1, groupingKey3)),
                                                            ImmutableList.of(),
                                                            groupId,
                                                            p.values(new PlanNodeId(groupIdSourceId), groupingKey1, groupingKey2, groupingKey3))))));
                });

        if (partitionedBy.isEmpty()) {
            ruleAssert.doesNotFire();
        }
        else {
            ruleAssert
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
                                                    partitionedBy,
                                                    exchange(
                                                            REMOTE_STREAMING,
                                                            REPARTITION,
                                                            ImmutableList.of(),
                                                            partitionedBy,
                                                            values("groupingKey1", "groupingKey2", "groupingKey3"))))
                                            .with(new GroupIdMatcher(ImmutableList.of(
                                                    ImmutableList.of("groupingKey1", "groupingKey2"),
                                                    ImmutableList.of("groupingKey1", "groupingKey3")), ImmutableMap.of(), "groupId")))));
        }
    }
}
