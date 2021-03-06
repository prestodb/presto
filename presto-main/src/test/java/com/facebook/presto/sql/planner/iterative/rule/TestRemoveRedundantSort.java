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
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestRemoveRedundantSort
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new RemoveRedundantSort())
                .on(p ->
                        p.sort(
                                ImmutableList.of(p.variable("c")),
                                p.aggregation(builder -> builder
                                        .addAggregation(p.variable("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .globalGrouping()
                                        .source(p.values(p.variable("foo"))))))
                .matches(
                        node(AggregationNode.class,
                                node(ValuesNode.class)));
    }

    @Test
    public void testForZeroCardinality()
    {
        tester().assertThat(new RemoveRedundantSort())
                .on(p ->
                        p.sort(
                                ImmutableList.of(p.variable("c")),
                                p.values(p.variable("foo"))))
                .matches(node(ValuesNode.class));
    }

    @Test
    public void doesNotFire()
    {
        tester().assertThat(new RemoveRedundantSort())
                .on(p ->
                        p.sort(
                                ImmutableList.of(p.variable("c")),
                                p.aggregation(builder -> builder
                                        .addAggregation(p.variable("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .singleGroupingSet(p.variable("foo"))
                                        .source(p.values(20, p.variable("foo"))))))
                .doesNotFire();
    }
}
