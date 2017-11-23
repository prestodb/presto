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
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestSingleMarkDistinctToGroupBy
        extends BaseRuleTest
{
    @Test
    public void testMultipleDistincts()
            throws Exception
    {
        tester().assertThat(new SingleMarkDistinctToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output1"), expression("count(input1)"), ImmutableList.of(BIGINT), p.symbol("marker1"))
                        .addAggregation(p.symbol("output2"), expression("count(input2)"), ImmutableList.of(BIGINT), p.symbol("marker2"))
                        .source(
                                p.markDistinct(
                                        p.symbol("marker1"),
                                        ImmutableList.of(p.symbol("input1")),
                                        p.markDistinct(
                                                p.symbol("marker2"),
                                                ImmutableList.of(p.symbol("input2")),
                                                p.values(
                                                        p.symbol("input1"),
                                                        p.symbol("input2")))))))
                .doesNotFire();
    }

    @Test
    public void testDistinctWithFilter()
            throws Exception
    {
        tester().assertThat(new SingleMarkDistinctToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output"), expression("count(input1) filter (where input2 > 0)"), ImmutableList.of(BIGINT), p.symbol("marker"))
                        .source(
                                p.markDistinct(
                                        p.symbol("marker"),
                                        ImmutableList.of(p.symbol("input1")),
                                        p.values(
                                                p.symbol("input1"),
                                                p.symbol("input2"))))))
                .doesNotFire();
    }

    @Test
    public void testBasic()
    {
        tester().assertThat(new SingleMarkDistinctToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .addAggregation(p.symbol("output"), expression("count(input)"), ImmutableList.of(BIGINT), p.symbol("marker"))
                        .source(
                                p.markDistinct(
                                        p.symbol("marker"),
                                        ImmutableList.of(p.symbol("input")),
                                        p.values(p.symbol("input"))))))
                .matches(
                        aggregation(
                                ImmutableList.of(ImmutableList.of()),
                                ImmutableMap.of(
                                        Optional.of("output"),
                                        functionCall("count", ImmutableList.of("input"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                aggregation(
                                        ImmutableList.of(ImmutableList.of("input")),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("input"))));
    }
}
