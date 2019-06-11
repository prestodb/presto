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

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;

public class TestPruneTableScanColumns
        extends BaseRuleTest
{
    @Test
    public void testNotAllOutputsReferenced()
    {
        tester().assertThat(new PruneTableScanColumns())
                .on(p ->
                {
                    Symbol orderdate = p.symbol("orderdate", DATE);
                    VariableReferenceExpression orderdateVariable = new VariableReferenceExpression(orderdate.getName(), DATE);
                    Symbol totalprice = p.symbol("totalprice", DOUBLE);
                    VariableReferenceExpression totalpriceVariable = new VariableReferenceExpression(totalprice.getName(), DOUBLE);
                    return p.project(
                            Assignments.of(p.variable("x"), totalprice.toSymbolReference()),
                            p.tableScan(
                                    new TableHandle(
                                            new ConnectorId("local"),
                                            new TpchTableHandle("orders", TINY_SCALE_FACTOR),
                                            TestingTransactionHandle.create(),
                                            Optional.empty()),
                                    ImmutableList.of(orderdateVariable, totalpriceVariable),
                                    ImmutableMap.of(
                                            orderdateVariable, new TpchColumnHandle(orderdate.getName(), DATE),
                                            totalpriceVariable, new TpchColumnHandle(totalprice.getName(), DOUBLE))));
                })
                .matches(
                        strictProject(
                                ImmutableMap.of("x_", PlanMatchPattern.expression("totalprice_")),
                                strictTableScan("orders", ImmutableMap.of("totalprice_", "totalprice"))));
    }

    @Test
    public void testAllOutputsReferenced()
    {
        tester().assertThat(new PruneTableScanColumns())
                .on(p -> {
                    Symbol x = p.symbol("x");
                    VariableReferenceExpression xv = p.variable(x);
                    return p.project(
                            Assignments.of(p.variable("y"), expression("x")),
                            p.tableScan(
                                    ImmutableList.of(xv),
                                    ImmutableMap.of(p.variable(p.symbol("x")), new TestingColumnHandle("x"))));
                })
                .doesNotFire();
    }
}
