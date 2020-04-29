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
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.assignment;
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
                    VariableReferenceExpression orderdate = p.variable("orderdate", DATE);
                    VariableReferenceExpression totalprice = p.variable("totalprice", DOUBLE);
                    return p.project(
                            assignment(p.variable("x"), new SymbolReference(totalprice.getName())),
                            p.tableScan(
                                    new TableHandle(
                                            new ConnectorId("local"),
                                            new TpchTableHandle("orders", TINY_SCALE_FACTOR),
                                            TestingTransactionHandle.create(),
                                            Optional.empty()),
                                    ImmutableList.of(orderdate, totalprice),
                                    ImmutableMap.of(
                                            orderdate, new TpchColumnHandle(orderdate.getName(), DATE),
                                            totalprice, new TpchColumnHandle(totalprice.getName(), DOUBLE))));
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
                    VariableReferenceExpression xv = p.variable("x");
                    return p.project(
                            assignment(p.variable("y"), expression("x")),
                            p.tableScan(
                                    ImmutableList.of(xv),
                                    ImmutableMap.of(p.variable("x"), new TestingColumnHandle("x"))));
                })
                .doesNotFire();
    }
}
