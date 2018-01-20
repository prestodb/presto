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

import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.predicate.Domain.singleValue;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.airlift.slice.Slices.utf8Slice;

public class TestPickTableLayoutWithPredicatePushDown
        extends BasePickTableLayoutTest
{
    public TestPickTableLayoutWithPredicatePushDown()
    {
        super(true);
    }

    @Test
    public void ruleWithPushdownableToTableLayoutPredicate()
    {
        TableHandle ordersTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle(connectorId.toString(), "orders", 1.0));
        Type orderStatusType = createVarcharType(1);
        tester().assertThat(pickTableLayout.pickTableLayoutForPredicate())
                .on(p -> p.filter(expression("orderstatus = 'O'"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", orderStatusType)),
                                ImmutableMap.of(p.symbol("orderstatus", orderStatusType), new TpchColumnHandle("orderstatus", orderStatusType)))))
                .matches(constrainedTableScanWithTableLayout(
                        "orders",
                        ImmutableMap.of("orderstatus", singleValue(orderStatusType, utf8Slice("O"))),
                        ImmutableMap.of("orderstatus", "orderstatus")));
    }

    @Test
    public void nonDeterministicPredicate()
    {
        TableHandle ordersTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle(connectorId.toString(), "orders", 1.0));
        Type orderStatusType = createVarcharType(1);
        tester().assertThat(pickTableLayout.pickTableLayoutForPredicate())
                .on(p -> p.filter(expression("orderstatus = 'O' AND rand() = 0"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", orderStatusType)),
                                ImmutableMap.of(p.symbol("orderstatus", orderStatusType), new TpchColumnHandle("orderstatus", orderStatusType)))))
                .matches(
                        filter("rand() = 0",
                                constrainedTableScanWithTableLayout(
                                        "orders",
                                        ImmutableMap.of("orderstatus", singleValue(orderStatusType, utf8Slice("O"))),
                                        ImmutableMap.of("orderstatus", "orderstatus"))));
    }
}
