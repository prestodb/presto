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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ExchangeNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PRE_AGGREGATE_BEFORE_GROUPING_SETS;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPreAggregateBeforeGroupId
        extends BaseRuleTest
{
    @Test
    public void testPreAggregatesBeforeGroupId()
    {
        tester().assertThat(new PreAggregateBeforeGroupId(getFunctionManager()))
                .setSystemProperty(PRE_AGGREGATE_BEFORE_GROUPING_SETS, "true")
                .on(p -> {
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression z = p.variable("z", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression groupId = p.variable("groupId", BIGINT);

                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("sum_x", BIGINT),
                                    p.rowExpression("sum(x)"))
                            .groupingSets(new AggregationNode.GroupingSetDescriptor(
                                    ImmutableList.of(y, z, groupId),
                                    3,
                                    ImmutableSet.of()))
                            .groupIdVariable(groupId)
                            .step(AggregationNode.Step.PARTIAL)
                            .source(p.groupId(
                                    ImmutableList.of(
                                            ImmutableList.of(y, z),
                                            ImmutableList.of(y)),
                                    ImmutableList.of(x),
                                    groupId,
                                    p.values(y, z, x))));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("sum_x", functionCall("sum", ImmutableList.of("sum_0"))),
                                AggregationNode.Step.INTERMEDIATE,
                                node(GroupIdNode.class,
                                        aggregation(
                                                ImmutableMap.of("sum_0", functionCall("sum", ImmutableList.of("sum"))),
                                                AggregationNode.Step.INTERMEDIATE,
                                                node(ExchangeNode.class,
                                                        aggregation(
                                                                ImmutableMap.of("sum", functionCall("sum", ImmutableList.of("x"))),
                                                                AggregationNode.Step.PARTIAL,
                                                                values("y", "z", "x")))))));
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new PreAggregateBeforeGroupId(getFunctionManager()))
                .setSystemProperty(PRE_AGGREGATE_BEFORE_GROUPING_SETS, "false")
                .on(p -> {
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression z = p.variable("z", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression groupId = p.variable("groupId", BIGINT);

                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("sum_x", BIGINT),
                                    p.rowExpression("sum(x)"))
                            .groupingSets(new AggregationNode.GroupingSetDescriptor(
                                    ImmutableList.of(y, z, groupId),
                                    3,
                                    ImmutableSet.of()))
                            .groupIdVariable(groupId)
                            .step(AggregationNode.Step.PARTIAL)
                            .source(p.groupId(
                                    ImmutableList.of(
                                            ImmutableList.of(y, z),
                                            ImmutableList.of(y)),
                                    ImmutableList.of(x),
                                    groupId,
                                    p.values(y, z, x))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnSingleStep()
    {
        tester().assertThat(new PreAggregateBeforeGroupId(getFunctionManager()))
                .setSystemProperty(PRE_AGGREGATE_BEFORE_GROUPING_SETS, "true")
                .on(p -> {
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression z = p.variable("z", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression groupId = p.variable("groupId", BIGINT);

                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("sum_x", BIGINT),
                                    p.rowExpression("sum(x)"))
                            .groupingSets(new AggregationNode.GroupingSetDescriptor(
                                    ImmutableList.of(y, z, groupId),
                                    3,
                                    ImmutableSet.of()))
                            .groupIdVariable(groupId)
                            .step(AggregationNode.Step.SINGLE)
                            .source(p.groupId(
                                    ImmutableList.of(
                                            ImmutableList.of(y, z),
                                            ImmutableList.of(y)),
                                    ImmutableList.of(x),
                                    groupId,
                                    p.values(y, z, x))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonGroupIdSource()
    {
        tester().assertThat(new PreAggregateBeforeGroupId(getFunctionManager()))
                .setSystemProperty(PRE_AGGREGATE_BEFORE_GROUPING_SETS, "true")
                .on(p -> {
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("sum_x", BIGINT),
                                    p.rowExpression("sum(x)"))
                            .singleGroupingSet(x)
                            .step(AggregationNode.Step.PARTIAL)
                            .source(p.values(x)));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnDistinctAggregation()
    {
        tester().assertThat(new PreAggregateBeforeGroupId(getFunctionManager()))
                .setSystemProperty(PRE_AGGREGATE_BEFORE_GROUPING_SETS, "true")
                .on(p -> {
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression z = p.variable("z", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression groupId = p.variable("groupId", BIGINT);

                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("sum_x", BIGINT),
                                    p.rowExpression("sum(x)"),
                                    true)
                            .groupingSets(new AggregationNode.GroupingSetDescriptor(
                                    ImmutableList.of(y, z, groupId),
                                    3,
                                    ImmutableSet.of()))
                            .groupIdVariable(groupId)
                            .step(AggregationNode.Step.PARTIAL)
                            .source(p.groupId(
                                    ImmutableList.of(
                                            ImmutableList.of(y, z),
                                            ImmutableList.of(y)),
                                    ImmutableList.of(x),
                                    groupId,
                                    p.values(y, z, x))));
                })
                .doesNotFire();
    }

    @Test
    public void testPreAggregatesMultipleAggregations()
    {
        tester().assertThat(new PreAggregateBeforeGroupId(getFunctionManager()))
                .setSystemProperty(PRE_AGGREGATE_BEFORE_GROUPING_SETS, "true")
                .on(p -> {
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression z = p.variable("z", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression w = p.variable("w", BIGINT);
                    VariableReferenceExpression groupId = p.variable("groupId", BIGINT);

                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("sum_x", BIGINT),
                                    p.rowExpression("sum(x)"))
                            .addAggregation(
                                    p.variable("count_w", BIGINT),
                                    p.rowExpression("count(w)"))
                            .groupingSets(new AggregationNode.GroupingSetDescriptor(
                                    ImmutableList.of(y, z, groupId),
                                    3,
                                    ImmutableSet.of()))
                            .groupIdVariable(groupId)
                            .step(AggregationNode.Step.PARTIAL)
                            .source(p.groupId(
                                    ImmutableList.of(
                                            ImmutableList.of(y, z),
                                            ImmutableList.of(y)),
                                    ImmutableList.of(x, w),
                                    groupId,
                                    p.values(y, z, x, w))));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of(
                                        "sum_x", functionCall("sum", ImmutableList.of("sum_0")),
                                        "count_w", functionCall("count", ImmutableList.of("count_0"))),
                                AggregationNode.Step.INTERMEDIATE,
                                node(GroupIdNode.class,
                                        aggregation(
                                                ImmutableMap.of(
                                                        "sum_0", functionCall("sum", ImmutableList.of("sum")),
                                                        "count_0", functionCall("count", ImmutableList.of("count"))),
                                                AggregationNode.Step.INTERMEDIATE,
                                                node(ExchangeNode.class,
                                                        aggregation(
                                                                ImmutableMap.of(
                                                                        "sum", functionCall("sum", ImmutableList.of("x")),
                                                                        "count", functionCall("count", ImmutableList.of("w"))),
                                                                AggregationNode.Step.PARTIAL,
                                                                values("y", "z", "x", "w")))))));
    }

    @Test
    public void testPreAggregatesWithCountStar()
    {
        tester().assertThat(new PreAggregateBeforeGroupId(getFunctionManager()))
                .setSystemProperty(PRE_AGGREGATE_BEFORE_GROUPING_SETS, "true")
                .on(p -> {
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression z = p.variable("z", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression groupId = p.variable("groupId", BIGINT);

                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("sum_x", BIGINT),
                                    p.rowExpression("sum(x)"))
                            .addAggregation(
                                    p.variable("count_star", BIGINT),
                                    p.rowExpression("count()"))
                            .groupingSets(new AggregationNode.GroupingSetDescriptor(
                                    ImmutableList.of(y, z, groupId),
                                    3,
                                    ImmutableSet.of()))
                            .groupIdVariable(groupId)
                            .step(AggregationNode.Step.PARTIAL)
                            .source(p.groupId(
                                    ImmutableList.of(
                                            ImmutableList.of(y, z),
                                            ImmutableList.of(y)),
                                    ImmutableList.of(x),
                                    groupId,
                                    p.values(y, z, x))));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of(
                                        "sum_x", functionCall("sum", ImmutableList.of("sum_0")),
                                        "count_star", functionCall("count", ImmutableList.of("count_0"))),
                                AggregationNode.Step.INTERMEDIATE,
                                node(GroupIdNode.class,
                                        aggregation(
                                                ImmutableMap.of(
                                                        "sum_0", functionCall("sum", ImmutableList.of("sum")),
                                                        "count_0", functionCall("count", ImmutableList.of("count"))),
                                                AggregationNode.Step.INTERMEDIATE,
                                                node(ExchangeNode.class,
                                                        aggregation(
                                                                ImmutableMap.of(
                                                                        "sum", functionCall("sum", ImmutableList.of("x")),
                                                                        "count", functionCall("count", ImmutableList.of())),
                                                                AggregationNode.Step.PARTIAL,
                                                                values("y", "z", "x")))))));
    }

    @Test
    public void testFiresThroughProjectNode()
    {
        tester().assertThat(new PreAggregateBeforeGroupId(getFunctionManager()))
                .setSystemProperty(PRE_AGGREGATE_BEFORE_GROUPING_SETS, "true")
                .on(p -> {
                    VariableReferenceExpression y = p.variable("y", BIGINT);
                    VariableReferenceExpression z = p.variable("z", BIGINT);
                    VariableReferenceExpression x = p.variable("x", BIGINT);
                    VariableReferenceExpression groupId = p.variable("groupId", BIGINT);

                    // Simulate a Project node (e.g., hash generation) between Agg and GroupId
                    Assignments identityAssignments = Assignments.builder()
                            .put(y, y)
                            .put(z, z)
                            .put(x, x)
                            .put(groupId, groupId)
                            .build();

                    return p.aggregation(a -> a
                            .addAggregation(
                                    p.variable("sum_x", BIGINT),
                                    p.rowExpression("sum(x)"))
                            .groupingSets(new AggregationNode.GroupingSetDescriptor(
                                    ImmutableList.of(y, z, groupId),
                                    3,
                                    ImmutableSet.of()))
                            .groupIdVariable(groupId)
                            .step(AggregationNode.Step.PARTIAL)
                            .source(p.project(
                                    identityAssignments,
                                    p.groupId(
                                            ImmutableList.of(
                                                    ImmutableList.of(y, z),
                                                    ImmutableList.of(y)),
                                            ImmutableList.of(x),
                                            groupId,
                                            p.values(y, z, x)))));
                })
                .matches(
                        aggregation(
                                ImmutableMap.of("sum_x", functionCall("sum", ImmutableList.of("sum_0"))),
                                AggregationNode.Step.INTERMEDIATE,
                                node(GroupIdNode.class,
                                        aggregation(
                                                ImmutableMap.of("sum_0", functionCall("sum", ImmutableList.of("sum"))),
                                                AggregationNode.Step.INTERMEDIATE,
                                                node(ExchangeNode.class,
                                                        aggregation(
                                                                ImmutableMap.of("sum", functionCall("sum", ImmutableList.of("x"))),
                                                                AggregationNode.Step.PARTIAL,
                                                                values("y", "z", "x")))))));
    }
}
