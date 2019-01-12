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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.assertions.ExpectedValueProvider;
import io.prestosql.sql.planner.assertions.PlanMatchPattern;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.tree.FunctionCall;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.SystemSessionProperties.ENABLE_INTERMEDIATE_AGGREGATIONS;
import static io.prestosql.SystemSessionProperties.TASK_CONCURRENCY;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.aggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.FINAL;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.INTERMEDIATE;
import static io.prestosql.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPARTITION;

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
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.symbol("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE,
                                            p.aggregation(ap -> ap.globalGrouping()
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.symbol("b"), expression("count(a)"), ImmutableList.of(BIGINT))
                                                    .source(
                                                            p.values(p.symbol("a"))))));
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
                                                        exchange(REMOTE, GATHER,
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
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.symbol("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE,
                                            p.aggregation(ap -> ap.globalGrouping()
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.symbol("b"), expression("count(*)"), ImmutableList.of(BIGINT))
                                                    .source(
                                                            p.values(p.symbol("a"))))));
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
                                                        exchange(REMOTE, GATHER,
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
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.symbol("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE,
                                            p.gatheringExchange(
                                                    ExchangeNode.Scope.REMOTE,
                                                    p.aggregation(ap -> ap.globalGrouping()
                                                            .step(AggregationNode.Step.PARTIAL)
                                                            .addAggregation(p.symbol("b"), expression("count(a)"), ImmutableList.of(BIGINT))
                                                            .source(
                                                                    p.values(p.symbol("a")))))));
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
                                                        exchange(REMOTE, GATHER,
                                                                exchange(REMOTE, GATHER,
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
                            .addAggregation(p.symbol("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE,
                                            p.aggregation(ap -> ap.globalGrouping()
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.symbol("b"), expression("count(a)"), ImmutableList.of(BIGINT))
                                                    .source(
                                                            p.values(p.symbol("a"))))));
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
                            .addAggregation(p.symbol("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE,
                                            p.aggregation(ap -> ap.globalGrouping()
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.symbol("b"), expression("count(a)"), ImmutableList.of(BIGINT))
                                                    .source(
                                                            p.values(p.symbol("a"))))));
                }))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(Optional.empty(), aggregationPattern),
                                ImmutableMap.of(),
                                Optional.empty(),
                                FINAL,
                                exchange(REMOTE, GATHER,
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
                    af.singleGroupingSet(p.symbol("c"))
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.symbol("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE,
                                            p.aggregation(ap -> ap.singleGroupingSet(p.symbol("b"))
                                                    .step(AggregationNode.Step.PARTIAL)
                                                    .addAggregation(p.symbol("b"), expression("count(a)"), ImmutableList.of(BIGINT))
                                                    .source(
                                                            p.values(p.symbol("a"))))));
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
                            .addAggregation(p.symbol("c"), expression("count(b)"), ImmutableList.of(BIGINT))
                            .source(
                                    p.gatheringExchange(
                                            ExchangeNode.Scope.REMOTE,
                                            p.project(
                                                    Assignments.identity(p.symbol("b")),
                                                    p.aggregation(ap -> ap.globalGrouping()
                                                            .step(AggregationNode.Step.PARTIAL)
                                                            .addAggregation(p.symbol("b"), expression("count(a)"), ImmutableList.of(BIGINT))
                                                            .source(
                                                                    p.values(p.symbol("a")))))));
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
                                                        exchange(REMOTE, GATHER,
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
