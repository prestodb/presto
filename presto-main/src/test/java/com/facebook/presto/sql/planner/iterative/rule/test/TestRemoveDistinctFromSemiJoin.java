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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.RemoveDistinctFromSemiJoin;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;

public class TestRemoveDistinctFromSemiJoin
{
    private final RuleTester tester = new RuleTester();

    @Test
    public void test()
            throws Exception
    {
        tester.assertThat(new RemoveDistinctFromSemiJoin())
                .on(p -> {
                    Symbol sourceKey = p.symbol("custkey", BigintType.BIGINT);
                    Symbol filteringSourceKey = p.symbol("custkey_1", BigintType.BIGINT);
                    Symbol outputKey = p.symbol("orderkey", BigintType.BIGINT);
                    return p.semiJoin(
                            p.tableScan(
                                    new TableHandle(
                                            new ConnectorId("local"),
                                            new TpchTableHandle("local", "orders", TINY_SCALE_FACTOR)),
                                    ImmutableList.of(sourceKey),
                                    ImmutableMap.of(sourceKey, new TpchColumnHandle("custkey", BIGINT))),
                            p.project(Assignments.of(filteringSourceKey, expression("x")),
                                    p.aggregation(ab -> ab.step(AggregationNode.Step.SINGLE)
                                            .groupingSets(ImmutableList.of(ImmutableList.of(filteringSourceKey)))
                                            .source(
                                                    p.tableScan(
                                                            new TableHandle(
                                                                    new ConnectorId("local"),
                                                                    new TpchTableHandle("local", "customer", TINY_SCALE_FACTOR)),
                                                            ImmutableList.of(filteringSourceKey),
                                                            ImmutableMap.of(filteringSourceKey, new TpchColumnHandle("custkey", BIGINT)))
                                            )
                                    .build())
                            ),
                            sourceKey, filteringSourceKey, outputKey
                    );
                })
                .matches(
                        semiJoin("Source", "Filter", "Output",
                                tableScan("orders", ImmutableMap.of("Source", "custkey")),
                                project(tableScan("customer", ImmutableMap.of("Filter", "custkey")))
                        )
                );
    }

    @Test
    public void testDoesNotFire()
    {
        tester.assertThat(new RemoveDistinctFromSemiJoin())
                .on(p -> {
                    Symbol sourceKey = p.symbol("custkey", BigintType.BIGINT);
                    Symbol filteringSourceKey = p.symbol("custkey_1", BigintType.BIGINT);
                    Symbol outputKey = p.symbol("orderkey", BigintType.BIGINT);
                    return p.semiJoin(
                            p.tableScan(
                                    new TableHandle(
                                            new ConnectorId("local"),
                                            new TpchTableHandle("local", "orders", TINY_SCALE_FACTOR)),
                                    ImmutableList.of(sourceKey),
                                    ImmutableMap.of(sourceKey, new TpchColumnHandle("custkey", BIGINT))),
                            p.project(Assignments.of(filteringSourceKey, expression("x")),
                                    p.aggregation(ab -> ab.step(AggregationNode.Step.SINGLE)
                                            .groupingSets(ImmutableList.of(ImmutableList.of(filteringSourceKey)))
                                            .addAggregation(p.symbol("max", BigintType.BIGINT), expression("max(custkey_1)"), ImmutableList.of(BIGINT))
                                            .source(p.tableScan(
                                                            new TableHandle(
                                                                    new ConnectorId("local"),
                                                                    new TpchTableHandle("local", "customer", TINY_SCALE_FACTOR)),
                                                            ImmutableList.of(filteringSourceKey),
                                                            ImmutableMap.of(filteringSourceKey, new TpchColumnHandle("custkey", BIGINT)))
                                            )
                                            .build())
                            ),
                            sourceKey, filteringSourceKey, outputKey
                    );
                }).doesNotFire();
    }
}
