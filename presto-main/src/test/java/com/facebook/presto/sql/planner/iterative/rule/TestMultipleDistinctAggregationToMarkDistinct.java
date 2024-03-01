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
import org.testng.annotations.Test;

import java.util.Optional;

public class TestMultipleDistinctAggregationToMarkDistinct
        extends BaseRuleTest
{
    @Test
    public void testNoDistinct()
    {
        tester().assertThat(new SingleDistinctAggregationToGroupBy())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(input1)"))
                        .addAggregation(p.variable("output2"), p.rowExpression("count(input2)"))))
                .doesNotFire();
    }

    @Test
    public void testSingleDistinct()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(DISTINCT input1)"), true)))
                .doesNotFire();
    }

    @Test
    public void testMultipleAggregations()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input")))
                        .addAggregation(p.variable("output1"), p.rowExpression("count(DISTINCT input)"), true)
                        .addAggregation(p.variable("output2"), p.rowExpression("sum(DISTINCT input)"), true)))
                .doesNotFire();
    }

    @Test
    public void testDistinctWithFilter()
    {
        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(
                                p.variable("output1"),
                                p.rowExpression("count(DISTINCT input1)"),
                                Optional.of(p.rowExpression("input2 > 0")),
                                Optional.empty(),
                                true,
                                Optional.empty())
                        .addAggregation(
                                p.variable("output2"),
                                p.rowExpression("count(DISTINCT input2)"),
                                Optional.of(p.rowExpression("input1 > 0")),
                                Optional.empty(),
                                true,
                                Optional.empty())))
                .doesNotFire();

        tester().assertThat(new MultipleDistinctAggregationToMarkDistinct())
                .on(p -> p.aggregation(builder -> builder
                        .globalGrouping()
                        .source(p.values(p.variable("input1"), p.variable("input2")))
                        .addAggregation(
                                p.variable("output1"),
                                p.rowExpression("count(DISTINCT input1)"),
                                Optional.of(p.rowExpression("input2 > 0")),
                                Optional.empty(),
                                true,
                                Optional.empty())
                        .addAggregation(p.variable("output2"), p.rowExpression("count(DISTINCT input2)"), true)))
                .doesNotFire();
    }
}
