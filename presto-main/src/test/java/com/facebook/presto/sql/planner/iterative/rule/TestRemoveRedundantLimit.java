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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.constantExpressions;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestRemoveRedundantLimit
        extends BaseRuleTest
{
    @Test
    public void test()
    {
        tester().assertThat(new RemoveRedundantLimit())
                .on(p ->
                        p.limit(
                                10,
                                p.aggregation(builder -> builder
                                        .addAggregation(p.variable("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .globalGrouping()
                                        .source(p.values(p.variable("foo"))))))
                .matches(
                        node(AggregationNode.class,
                                node(ValuesNode.class)));
    }

    @Test
    public void testRemoveLimitWithTies()
    {
        tester().assertThat(new RemoveRedundantLimit())
                .on(p -> {
                    VariableReferenceExpression c = p.variable("c");
                    return p.limit(
                            10,
                            p.values(5, c));
                })
                .matches(values("c"));
    }

    @Test
    public void testForZeroLimit()
    {
        tester().assertThat(new EvaluateZeroLimit())
                .on(p ->
                        p.limit(
                                0,
                                p.filter(
                                        expression("b > 5"),
                                        p.values(
                                                ImmutableList.of(p.variable("a"), p.variable("b")),
                                                ImmutableList.of(
                                                        constantExpressions(BIGINT, 1L, 10L),
                                                constantExpressions(BIGINT, 2L, 11L))))))
                .matches(values(ImmutableMap.of()));
    }

    @Test
    public void doesNotFire()
    {
        tester().assertThat(new EvaluateZeroLimit())
                .on(p ->
                        p.limit(
                                10,
                                p.aggregation(builder -> builder
                                        .addAggregation(p.variable("c"), expression("count(foo)"), ImmutableList.of(BIGINT))
                                        .singleGroupingSet(p.variable("foo"))
                                        .source(p.values(20, p.variable("foo"))))))
                .doesNotFire();
    }
}
