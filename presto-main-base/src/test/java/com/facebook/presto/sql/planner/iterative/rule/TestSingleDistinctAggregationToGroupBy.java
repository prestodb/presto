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

import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.tree.FunctionCall;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestSingleDistinctAggregationToGroupBy
        extends BaseRuleTest
{
    @Test
    public void testNoDistinct()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(input1)"))))
                .doesNotFire();
    }

    @Test
    public void testMultipleDistincts()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(DISTINCT input1)"), true)
                        .addAggregation(p.variable("output2"), p.rowExpression("count(DISTINCT input2)"), true)))
                .doesNotFire();
    }

    @Test
    public void testMixedDistinctAndNonDistinct()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(DISTINCT input1)"), true)
                        .addAggregation(p.variable("output2"), p.rowExpression("count(input2)"))))
                .doesNotFire();
    }

    @Test
    public void testDistinctWithFilter()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(
                                p.variable("output"),
                                p.rowExpression("count(DISTINCT input1) filter (where input2 > 0)"),
                                Optional.of(p.rowExpression("input2 > 0")),
                                Optional.empty(),
                                true,
                                Optional.empty())))
                .doesNotFire();
    }

    @Test
    public void testSingleAggregation()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input")))
                        .addAggregation(p.variable("output"), p.rowExpression("count(DISTINCT input)"), true)))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.of(
                                        Optional.of("output"),
                                        functionCall("count", ImmutableList.of("input"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                aggregation(
                                        singleGroupingSet("input"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("input"))));
    }

    @Test
    public void testMultipleAggregations()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input")))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(DISTINCT input)"), true)
                        .addAggregation(p.variable("output2"), p.rowExpression("sum(DISTINCT input)"), true)))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.<Optional<String>, ExpectedValueProvider<FunctionCall>>builder()
                                        .put(Optional.of("output1"), functionCall("count", ImmutableList.of("input")))
                                        .put(Optional.of("output2"), functionCall("sum", ImmutableList.of("input")))
                                        .build(),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                aggregation(
                                        singleGroupingSet("input"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("input"))));
    }

    @Test
    public void testMultipleInputs()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("x"), p.variable("y")))
                        .addAggregation(p.variable("output1"), p.rowExpression("corr(DISTINCT x, y)"), true)
                        .addAggregation(p.variable("output2"), p.rowExpression("corr(DISTINCT y, x)"), true)))
                .matches(
                        aggregation(
                                globalAggregation(),
                                ImmutableMap.<Optional<String>, ExpectedValueProvider<FunctionCall>>builder()
                                        .put(Optional.of("output1"), functionCall("corr", ImmutableList.of("x", "y")))
                                        .put(Optional.of("output2"), functionCall("corr", ImmutableList.of("y", "x")))
                                        .build(),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                aggregation(
                                        singleGroupingSet("x", "y"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("x", "y"))));
    }
}
