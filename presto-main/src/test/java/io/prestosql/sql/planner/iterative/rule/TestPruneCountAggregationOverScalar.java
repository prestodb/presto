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
import io.prestosql.connector.ConnectorId;
import io.prestosql.metadata.TableHandle;
import io.prestosql.plugin.tpch.TpchColumnHandle;
import io.prestosql.plugin.tpch.TpchTableHandle;
import io.prestosql.spi.type.BigintType;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.planner.plan.AggregationNode.singleGroupingSet;

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
                                                            new TpchTableHandle("orders", TINY_SCALE_FACTOR)),
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
