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

import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.spi.plan.AggregationNode.groupingSets;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestEvaluateArbitraryNull
        extends BaseRuleTest
{
    @Test
    public void testSingleArbitraryNull()
    {
        tester().assertThat(new EvaluateArbitraryNull())
                .on(p -> p.aggregation(builder -> builder
                        .source(p.values())
                        .addAggregation(p.variable("arbitrary", UNKNOWN), p.rowExpression("arbitrary(null)"))
                        .globalGrouping()))
                .matches(
                        project(
                                ImmutableMap.of("arbitrary", expression("null")),
                                values(ImmutableList.of())));
    }

    @Test
    public void testMultipleArbitrary()
    {
        tester().assertThat(new EvaluateArbitraryNull())
                .on(p -> p.aggregation(builder -> {
                    p.variable("col", BIGINT);
                    builder
                            .source(p.values(p.variable("col")))
                            .addAggregation(p.variable("arbitrary_1", UNKNOWN), p.rowExpression("arbitrary(null)"))
                            .addAggregation(p.variable("arbitrary_2"), p.rowExpression("arbitrary(col)"))
                            .globalGrouping();
                }))
                .matches(
                        project(
                                ImmutableMap.of("arbitrary_1", expression("null")),
                                aggregation(
                                        ImmutableMap.of("arbitrary_2", functionCall("arbitrary", ImmutableList.of("col"))),
                                        project(
                                                ImmutableMap.of("col", expression("col")),
                                                values("col")))));
    }

    @Test
    public void testMultipleAggregations()
    {
        tester().assertThat(new EvaluateArbitraryNull())
                .on(p -> p.aggregation(builder -> {
                    p.variable("col", BIGINT);
                    builder
                            .source(p.values(p.variable("col")))
                            .addAggregation(p.variable("arbitrary", UNKNOWN), p.rowExpression("arbitrary(null)"))
                            .addAggregation(p.variable("count"), p.rowExpression("count(col)"))
                            .globalGrouping();
                }))
                .matches(
                        project(
                                ImmutableMap.of("arbitrary", expression("null")),
                                aggregation(
                                        ImmutableMap.of("count", functionCall("count", ImmutableList.of("col"))),
                                        project(
                                                ImmutableMap.of("col", expression("col")),
                                                values("col")))));
    }

    @Test
    public void testWithGroupingSets()
    {
        tester().assertThat(new EvaluateArbitraryNull())
                .on(p -> p.aggregation(builder -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    builder
                            .source(p.values(p.variable("col1"), p.variable("col2")))
                            .addAggregation(p.variable("arbitrary", UNKNOWN), p.rowExpression("arbitrary(null)"))
                            .groupingSets(groupingSets(ImmutableList.of(p.variable("col1"), p.variable("col2")), 2, ImmutableSet.of(0)));
                }))
                .matches(
                        project(
                                ImmutableMap.of("arbitrary", expression("null")),
                                aggregation(
                                        ImmutableMap.of(),
                                        project(
                                                ImmutableMap.of("col1", expression("col1"), "col2", expression("col2")),
                                                values("col1", "col2")))));
    }

    @Test
    public void testNonNullArbitrary()
    {
        tester().assertThat(new EvaluateArbitraryNull())
                .on(p -> p.aggregation(builder -> builder
                        .source(p.values(p.variable("col")))
                        .addAggregation(
                                p.variable("arbitrary"),
                                p.rowExpression("arbitrary(col)"))
                        .globalGrouping()))
                .doesNotFire();
    }

    @Test
    public void testNoArbitrary()
    {
        tester().assertThat(new EvaluateArbitraryNull())
                .on(p -> p.aggregation(builder -> builder
                        .source(p.values(p.variable("col")))
                        .addAggregation(
                                p.variable("count"),
                                p.rowExpression("count(col)"))
                        .globalGrouping()))
                .doesNotFire();
    }
}
