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

import com.facebook.presto.cost.PartialAggregationStatsEstimate;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PARTIAL_AGGREGATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.USE_PARTIAL_AGGREGATION_HISTORY;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.relational.Expressions.variable;

public class TestPushPartialAggregationThroughExchange
        extends BaseRuleTest
{
    @Test
    public void testPartialAggregationAdded()
    {
        tester().assertThat(new PushPartialAggregationThroughExchange(getFunctionManager()))
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    return p.aggregation(ab -> ab
                            .source(
                                    p.exchange(e -> e
                                            .addSource(p.values(a))
                                            .addInputsSet(a)
                                            .singleDistributionPartitioningScheme(a)))
                            .addAggregation(p.variable("SUM", DOUBLE), p.rowExpression("SUM(a)"))
                            .globalGrouping()
                            .step(PARTIAL));
                })
                .matches(exchange(
                        project(
                                aggregation(
                                        ImmutableMap.of("SUM", functionCall("sum", ImmutableList.of("a"))),
                                        PARTIAL,
                                        values("a")))));
    }

    @Test
    public void testNoPartialAggregationWhenDisabled()
    {
        tester().assertThat(new PushPartialAggregationThroughExchange(getFunctionManager()))
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "NEVER")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a");
                    return p.aggregation(ab -> ab
                            .source(
                                    p.exchange(e -> e
                                            .addSource(p.values(a))
                                            .addInputsSet(a)
                                            .singleDistributionPartitioningScheme(a)))
                            .addAggregation(p.variable("SUM", DOUBLE), p.rowExpression("SUM(a)"))
                            .globalGrouping()
                            .step(PARTIAL));
                })
                .doesNotFire();
    }

    @Test
    public void testNoPartialAggregationWhenReductionBelowThreshold()
    {
        tester().assertThat(new PushPartialAggregationThroughExchange(getFunctionManager()))
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", DOUBLE);
                    VariableReferenceExpression b = p.variable("b", DOUBLE);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.exchange(e -> e
                                            .addSource(p.values(new PlanNodeId("values"), a, b))
                                            .addInputsSet(a, b)
                                            .singleDistributionPartitioningScheme(a, b)))
                            .addAggregation(p.variable("SUM", DOUBLE), p.rowExpression("SUM(a)"))
                            .singleGroupingSet(b)
                            .step(SINGLE));
                })
                .overrideStats("values", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfident(true)
                        .build())
                .doesNotFire();
    }

    @Test
    public void testNoPartialAggregationWhenReductionBelowThresholdUsingPartialAggregationStats()
    {
        tester().assertThat(new PushPartialAggregationThroughExchange(getFunctionManager()))
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .setSystemProperty(USE_PARTIAL_AGGREGATION_HISTORY, "true")
                .on(p -> constructAggregation(p))
                .overrideStats("aggregation", PlanNodeStatsEstimate.builder()
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfident(true)
                        .setPartialAggregationStatsEstimate(new PartialAggregationStatsEstimate(1000, 800, 10, 10))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testNoPartialAggregationWhenReductionAboveThresholdUsingPartialAggregationStats()
    {
        // when use_partial_aggregation_history=true, we use row count reduction (instead of bytes) to decide if partial aggregation is useful
        tester().assertThat(new PushPartialAggregationThroughExchange(getFunctionManager()))
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .setSystemProperty(USE_PARTIAL_AGGREGATION_HISTORY, "true")
                .on(p -> constructAggregation(p))
                .overrideStats("aggregation", PlanNodeStatsEstimate.builder()
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfident(true)
                        .setPartialAggregationStatsEstimate(new PartialAggregationStatsEstimate(1000, 300, 10, 10))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testNoPartialAggregationWhenRowReductionBelowThreshold()
    {
        tester().assertThat(new PushPartialAggregationThroughExchange(getFunctionManager()))
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .setSystemProperty(USE_PARTIAL_AGGREGATION_HISTORY, "true")
                .on(p -> constructAggregation(p))
                .overrideStats("aggregation", PlanNodeStatsEstimate.builder()
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfident(true)
                        .setPartialAggregationStatsEstimate(new PartialAggregationStatsEstimate(0, 300, 10, 8))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testPartialAggregationWhenRowReductionAboveThreshold()
    {
        tester().assertThat(new PushPartialAggregationThroughExchange(getFunctionManager()))
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .setSystemProperty(USE_PARTIAL_AGGREGATION_HISTORY, "true")
                .on(p -> constructAggregation(p))
                .overrideStats("aggregation", PlanNodeStatsEstimate.builder()
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfident(true)
                        .setPartialAggregationStatsEstimate(new PartialAggregationStatsEstimate(0, 300, 10, 1))
                        .build())
                .matches(aggregation(ImmutableMap.of("sum", functionCall("sum", ImmutableList.of("sum0"))),
                        aggregation(
                                ImmutableMap.of("sum0", functionCall("sum", ImmutableList.of("a"))),
                                exchange(
                                        values("a", "b")))));
    }

    @Test
    public void testPartialAggregationEnabledWhenNotConfident()
    {
        tester().assertThat(new PushPartialAggregationThroughExchange(getFunctionManager()))
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .on(p -> {
                    VariableReferenceExpression a = p.variable("a", DOUBLE);
                    VariableReferenceExpression b = p.variable("b", DOUBLE);
                    return p.aggregation(ab -> ab
                            .source(
                                    p.exchange(e -> e
                                            .addSource(p.values(new PlanNodeId("values"), a, b))
                                            .addInputsSet(a, b)
                                            .singleDistributionPartitioningScheme(a, b)))
                            .addAggregation(p.variable("SUM", DOUBLE), p.rowExpression("SUM(a)"))
                            .singleGroupingSet(b)
                            .step(PARTIAL));
                })
                .overrideStats("values", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(variable("b", DOUBLE), new VariableStatsEstimate(0, 100, 0, 8, 800))
                        .setConfident(false)
                        .build())
                .matches(exchange(
                        project(
                                aggregation(
                                        ImmutableMap.of("SUM", functionCall("sum", ImmutableList.of("a"))),
                                        PARTIAL,
                                        values("a", "b")))));
    }

    private static AggregationNode constructAggregation(PlanBuilder p)
    {
        VariableReferenceExpression a = p.variable("a", DOUBLE);
        VariableReferenceExpression b = p.variable("b", DOUBLE);
        return p.aggregation(ab -> ab
                .source(
                        p.exchange(e -> e
                                .addSource(p.values(new PlanNodeId("values"), a, b))
                                .addInputsSet(a, b)
                                .singleDistributionPartitioningScheme(
                                        ImmutableList.of(a, b))))
                .addAggregation(p.variable("sum", DOUBLE), p.rowExpression("sum(a)"))
                .singleGroupingSet(b)
                .setPlanNodeId(new PlanNodeId("aggregation")));
    }
}
