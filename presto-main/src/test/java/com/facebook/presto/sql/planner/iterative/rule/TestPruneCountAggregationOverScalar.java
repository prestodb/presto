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

import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.trait.CardinalityTrait;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPruneCountAggregationOverScalar
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnNestedCountAggregate()
    {
        tester().assertThat(new PruneCountAggregationOverScalar())
                .on(p ->
                        p.aggregation((a) -> a
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        new FunctionCall(QualifiedName.of("count"), ImmutableList.of()), ImmutableList.of(BigintType.BIGINT))
                                .globalGrouping()
                                .step(AggregationNode.Step.SINGLE)
                                .source(
                                        p.aggregation((aggregationBuilder) -> aggregationBuilder
                                                .source(p.tableScan(ImmutableList.of(), ImmutableMap.of()))
                                                .globalGrouping()
                                                .step(AggregationNode.Step.SINGLE)))))
                .doesNotFire();
    }

    @Test
    public void testFiresOnCountAggregateOverValues()
    {
        tester().assertThat(new PruneCountAggregationOverScalar())
                .on(p ->
                        p.aggregation((a) -> a
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                                        ImmutableList.of(BigintType.BIGINT))
                                .step(AggregationNode.Step.SINGLE)
                                .globalGrouping()
                                .source(p.values(ImmutableList.of(p.symbol("orderkey")), ImmutableList.of(p.expressions("1"))))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnCountAggregateOverEnforceSingleRow()
    {
        tester().assertThat(new PruneCountAggregationOverScalar())
                .on(p ->
                        p.aggregation((a) -> a
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                                        ImmutableList.of(BigintType.BIGINT))
                                .step(AggregationNode.Step.SINGLE)
                                .globalGrouping()
                                .source(p.enforceSingleRow(p.tableScan(ImmutableList.of(), ImmutableMap.of())))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireCountAggregateOverPlanWithNoExactCardinality()
    {
        tester().assertThat(new PruneCountAggregationOverScalar())
                .on(p ->
                        p.aggregation((a) -> a
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                                        ImmutableList.of(BigintType.BIGINT))
                                .step(AggregationNode.Step.SINGLE)
                                .globalGrouping()
                                .source(p.nodeWithTrait(CardinalityTrait.atMostScalar()))))
                .doesNotFire();
    }

    @Test
    public void testFireCountAggregateOverPlanWithExactCardinality()
    {
        tester().assertThat(new PruneCountAggregationOverScalar())
                .on(p ->
                        p.aggregation((a) -> a
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                                        ImmutableList.of(BigintType.BIGINT))
                                .step(AggregationNode.Step.SINGLE)
                                .globalGrouping()
                                .source(p.nodeWithTrait(CardinalityTrait.scalar()))))
                .matches(values(
                        ImmutableList.of("count_1"),
                        ImmutableList.of(ImmutableList.of(PlanBuilder.expression("1")))));
    }
}
