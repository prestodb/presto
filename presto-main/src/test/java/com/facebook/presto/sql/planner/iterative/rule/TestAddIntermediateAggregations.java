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
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.ENABLE_INTERMEDIATE_AGGREGATIONS;
import static com.facebook.presto.SystemSessionProperties.TASK_CONCURRENCY;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.INTERMEDIATE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignmentsAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestAddIntermediateAggregations
        extends BaseRuleTest
{
    @Test
    public void testBasic()
    {
        ExpectedValueProvider<FunctionCall> aggregationPattern = PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol()));

        tester().assertThat(new AddIntermediateAggregations())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(af -> {
                    p.variable("a", BIGINT);
                    p.variable("b", BIGINT);
                    p.variable("c", BIGINT);
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("c"), p.rowExpression("count(b)"))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE_STREAMING,
                                            p.aggregation(ap -> ap.globalGrouping()
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.variable("b"), p.rowExpression("count(a)"))
                                                    .source(
                                                            p.values(p.variable("a"))))));
                }))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                ImmutableMap.of(),
                                Optional.empty(),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        aggregation(
                                                globalAggregation(),
                                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                INTERMEDIATE,
                                                exchange(LOCAL, REPARTITION,
                                                        exchange(REMOTE_STREAMING, GATHER,
                                                                aggregation(
                                                                        globalAggregation(),
                                                                        ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                                        ImmutableMap.of(),
                                                                        Optional.empty(),
                                                                        INTERMEDIATE,
                                                                        exchange(LOCAL, GATHER,
                                                                                aggregation(
                                                                                        globalAggregation(),
                                                                                        ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                                                        ImmutableMap.of(),
                                                                                        Optional.empty(),
                                                                                        PARTIAL,
                                                                                        values(ImmutableMap.of("a", 0)))))))))));
    }

    @Test
    public void testNoInputCount()
    {
        // COUNT(*) is a special class of aggregation that doesn't take any input that should be tested
        ExpectedValueProvider<FunctionCall> rawInputCount = PlanMatchPattern.functionCall("count", false, ImmutableList.of());
        ExpectedValueProvider<FunctionCall> partialInputCount = PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol()));

        tester().assertThat(new AddIntermediateAggregations())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(af -> {
                    p.variable("b");
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("c"), p.rowExpression("count(b)"))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE_STREAMING,
                                            p.aggregation(ap -> ap.globalGrouping()
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.variable("b"), p.rowExpression("count(*)"))
                                                    .source(
                                                            p.values(p.variable("a"))))));
                }))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.empty(), partialInputCount),
                                ImmutableMap.of(),
                                Optional.empty(),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        aggregation(
                                                globalAggregation(),
                                                ImmutableMap.of(Optional.empty(), partialInputCount),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                INTERMEDIATE,
                                                exchange(LOCAL, REPARTITION,
                                                        exchange(REMOTE_STREAMING, GATHER,
                                                                aggregation(
                                                                        globalAggregation(),
                                                                        ImmutableMap.of(Optional.empty(), partialInputCount),
                                                                        ImmutableMap.of(),
                                                                        Optional.empty(),
                                                                        INTERMEDIATE,
                                                                        exchange(LOCAL, GATHER,
                                                                                aggregation(
                                                                                        globalAggregation(),
                                                                                        ImmutableMap.of(Optional.empty(), rawInputCount),
                                                                                        ImmutableMap.of(),
                                                                                        Optional.empty(),
                                                                                        PARTIAL,
                                                                                        values(ImmutableMap.of("a", 0)))))))))));
    }

    @Test
    public void testMultipleExchanges()
    {
        ExpectedValueProvider<FunctionCall> aggregationPattern = PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol()));

        tester().assertThat(new AddIntermediateAggregations())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(af -> {
                    p.variable("a");
                    p.variable("b");
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("c"), p.rowExpression("count(b)"))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE_STREAMING,
                                            p.gatheringExchange(
                                                    ExchangeNode.Scope.REMOTE_STREAMING,
                                                    p.aggregation(ap -> ap.globalGrouping()
                                                            .step(AggregationNode.Step.PARTIAL)
                                                            .addAggregation(p.variable("b"), p.rowExpression("count(a)"))
                                                            .source(
                                                                    p.values(p.variable("a")))))));
                }))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                ImmutableMap.of(),
                                Optional.empty(),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        aggregation(
                                                globalAggregation(),
                                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                INTERMEDIATE,
                                                exchange(LOCAL, REPARTITION,
                                                        exchange(REMOTE_STREAMING, GATHER,
                                                                exchange(REMOTE_STREAMING, GATHER,
                                                                        aggregation(
                                                                                globalAggregation(),
                                                                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                                                ImmutableMap.of(),
                                                                                Optional.empty(),
                                                                                INTERMEDIATE,
                                                                                exchange(LOCAL, GATHER,
                                                                                        aggregation(
                                                                                                globalAggregation(),
                                                                                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                                                                ImmutableMap.of(),
                                                                                                Optional.empty(),
                                                                                                PARTIAL,
                                                                                                values(ImmutableMap.of("a", 0))))))))))));
    }

    @Test
    public void testSessionDisable()
    {
        tester().assertThat(new AddIntermediateAggregations())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "false")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(af -> {
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE_STREAMING,
                                            p.aggregation(ap -> ap.globalGrouping()
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.variable("b"), expression("count(a)"), ImmutableList.of(BIGINT))
                                                    .source(
                                                            p.values(p.variable("a"))))));
                }))
                .doesNotFire();
    }

    @Test
    public void testNoLocalParallel()
    {
        ExpectedValueProvider<FunctionCall> aggregationPattern = PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol()));

        tester().assertThat(new AddIntermediateAggregations())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "1")
                .on(p -> p.aggregation(af -> {
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE_STREAMING,
                                            p.aggregation(ap -> ap.globalGrouping()
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.variable("b"), expression("count(a)"), ImmutableList.of(BIGINT))
                                                    .source(
                                                            p.values(p.variable("a"))))));
                }))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                ImmutableMap.of(),
                                Optional.empty(),
                                FINAL,
                                exchange(REMOTE_STREAMING, GATHER,
                                        aggregation(
                                                globalAggregation(),
                                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                INTERMEDIATE,
                                                exchange(LOCAL, GATHER,
                                                        aggregation(
                                                                globalAggregation(),
                                                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                                ImmutableMap.of(),
                                                                Optional.empty(),
                                                                PARTIAL,
                                                                values(ImmutableMap.of("a", 0))))))));
    }

    @Test
    public void testWithGroups()
    {
        tester().assertThat(new AddIntermediateAggregations())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(af -> {
                    af.singleGroupingSet(p.variable("c"))
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE_STREAMING,
                                            p.aggregation(ap -> ap.singleGroupingSet(p.variable("b"))
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.variable("b"), expression("count(a)"), ImmutableList.of(BIGINT))
                                                    .source(
                                                            p.values(p.variable("a"))))));
                }))
                .doesNotFire();
    }

    @Test
    public void testInterimProject()
    {
        ExpectedValueProvider<FunctionCall> aggregationPattern = PlanMatchPattern.functionCall("count", false, ImmutableList.of(anySymbol()));

        tester().assertThat(new AddIntermediateAggregations())
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .setSystemProperty(TASK_CONCURRENCY, "4")
                .on(p -> p.aggregation(af -> {
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE_STREAMING,
                                            p.project(
                                                    identityAssignmentsAsSymbolReferences(p.variable("b")),
                                                    p.aggregation(ap -> ap.globalGrouping()
                                                            .step(AggregationNode.Step.PARTIAL)
                                                            .addAggregation(p.variable("b"), expression("count(a)"), ImmutableList.of(BIGINT))
                                                            .source(
                                                                    p.values(p.variable("a")))))));
                }))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                ImmutableMap.of(),
                                Optional.empty(),
                                FINAL,
                                exchange(LOCAL, GATHER,
                                        aggregation(
                                                globalAggregation(),
                                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                INTERMEDIATE,
                                                exchange(LOCAL, REPARTITION,
                                                        exchange(REMOTE_STREAMING, GATHER,
                                                                project(
                                                                        aggregation(
                                                                                globalAggregation(),
                                                                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                                                ImmutableMap.of(),
                                                                                Optional.empty(),
                                                                                INTERMEDIATE,
                                                                                exchange(LOCAL, GATHER,
                                                                                        aggregation(
                                                                                                globalAggregation(),
                                                                                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                                                                                ImmutableMap.of(),
                                                                                                Optional.empty(),
                                                                                                PARTIAL,
                                                                                                values(ImmutableMap.of("a", 0))))))))))));
    }
}
