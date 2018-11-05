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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;

public class TestPruneCountAggregationOverScalar
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnNonNestedAggregate()
    {
        tester().assertThat(new PruneCountAggregationOverScalar())
                .on(p ->
                        p.aggregation((a) -> a
                                .globalGrouping()
                                .addAggregation(
                                        p.symbol("count_1", BigintType.BIGINT),
                                        new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                                        ImmutableList.of(BigintType.BIGINT))
                                .source(
                                        p.tableScan(ImmutableList.of(), ImmutableMap.of())))
                ).doesNotFire();
    }

    @Test
    public void testFiresOnNestedCountAggregate()
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
                .matches(values(ImmutableMap.of("count_1", 0)));
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
                .matches(values(ImmutableMap.of("count_1", 0)));
    }

    @Test
    public void testFiresOnCountAggregateOverEnforceSingleRow()
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
                .matches(values(ImmutableMap.of("count_1", 0)));
    }

    @Test
    public void testDoesNotFireOnNestedCountAggregateWithNonEmptyGroupBy()
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
                                .source(
                                        p.aggregation(aggregationBuilder -> {
                                            aggregationBuilder
                                                    .source(p.tableScan(ImmutableList.of(), ImmutableMap.of())).groupingSets(singleGroupingSet(ImmutableList.of(p.symbol("orderkey"))));
                                            aggregationBuilder
                                                    .source(p.tableScan(ImmutableList.of(), ImmutableMap.of()));
                                        }))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNestedNonCountAggregate()
    {
        tester().assertThat(new PruneCountAggregationOverScalar())
                .on(p -> {
                    Symbol totalPrice = p.symbol("total_price", DOUBLE);
                    AggregationNode inner = p.aggregation((a) -> a
                            .addAggregation(totalPrice,
                                    new FunctionCall(QualifiedName.of("sum"), ImmutableList.of(new SymbolReference("totalprice"))),
                                    ImmutableList.of(DOUBLE))
                            .globalGrouping()
                            .source(
                                    p.project(
                                            Assignments.of(totalPrice, totalPrice.toSymbolReference()),
                                            p.tableScan(
                                                    new TableHandle(
                                                            new ConnectorId("local"),
                                                            new TpchTableHandle("local", "orders", TINY_SCALE_FACTOR)),
                                                    ImmutableList.of(totalPrice),
                                                    ImmutableMap.of(totalPrice, new TpchColumnHandle(totalPrice.getName(), DOUBLE))))));

                    return p.aggregation((a) -> a
                            .addAggregation(
                                    p.symbol("sum_outer", DOUBLE),
                                    new FunctionCall(QualifiedName.of("sum"), ImmutableList.of(new SymbolReference("sum_inner"))),
                                    ImmutableList.of(DOUBLE))
                            .globalGrouping()
                            .source(inner));
                }).doesNotFire();
    }
}
