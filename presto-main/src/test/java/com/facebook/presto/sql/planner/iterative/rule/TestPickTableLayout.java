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
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.predicate.Domain.singleValue;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.airlift.slice.Slices.utf8Slice;

public class TestPickTableLayout
        extends BaseRuleTest
{
    private PickTableLayout pickTableLayout;
    private TableHandle nationTableHandle;
    private TableHandle ordersTableHandle;
    private TableLayoutHandle nationTableLayoutHandle;
    private TableLayoutHandle ordersTableLayoutHandle;
    private ConnectorId connectorId;

    @BeforeClass
    public void setUpBeforeClass()
    {
        pickTableLayout = new PickTableLayout(tester().getMetadata(), new SqlParser());

        connectorId = tester().getCurrentConnectorId();
        nationTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle(connectorId.toString(), "nation", 1.0));
        ordersTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle(connectorId.toString(), "orders", 1.0));

        nationTableLayoutHandle = new TableLayoutHandle(
                connectorId,
                TestingTransactionHandle.create(),
                new TpchTableLayoutHandle((TpchTableHandle) nationTableHandle.getConnectorHandle(), TupleDomain.all()));
        ordersTableLayoutHandle = new TableLayoutHandle(
                connectorId,
                TestingTransactionHandle.create(),
                new TpchTableLayoutHandle((TpchTableHandle) ordersTableHandle.getConnectorHandle(), TupleDomain.all()));
    }

    @Test
    public void doesNotFireIfNoTableScan()
    {
        for (Rule<?> rule : pickTableLayout.rules()) {
            tester().assertThat(rule)
                    .on(p -> p.values(p.symbol("a", BIGINT)))
                    .doesNotFire();
        }
    }

    @Test
    public void doesNotFireIfTableScanHasTableLayout()
    {
        tester().assertThat(pickTableLayout.pickTableLayoutWithoutPredicate())
                .on(p -> p.tableScan(
                        nationTableHandle,
                        ImmutableList.of(p.symbol("nationkey", BIGINT)),
                        ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                        Optional.of(nationTableLayoutHandle)))
                .doesNotFire();
    }

    @Test
    public void eliminateTableScanWhenNoLayoutExist()
    {
        tester().assertThat(pickTableLayout.pickTableLayoutForPredicate())
                .on(p -> p.filter(expression("orderstatus = 'G'"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1))),
                                Optional.of(ordersTableLayoutHandle))))
                .matches(values("A"));
    }

    @Test
    public void replaceWithExistsWhenNoLayoutExist()
    {
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester().assertThat(pickTableLayout.pickTableLayoutForPredicate())
                .on(p -> p.filter(expression("nationkey = BIGINT '44'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), columnHandle),
                                Optional.of(nationTableLayoutHandle),
                                TupleDomain.none(),
                                TupleDomain.none())))
                .matches(values("A"));
    }

    @Test
    public void doesNotFireIfRuleNotChangePlan()
    {
        tester().assertThat(pickTableLayout.pickTableLayoutForPredicate())
                .on(p -> p.filter(expression("nationkey % 17 =  BIGINT '44' AND nationkey % 15 =  BIGINT '43'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                                Optional.of(nationTableLayoutHandle),
                                TupleDomain.all(),
                                TupleDomain.all())))
                .doesNotFire();
    }

    @Test
    public void ruleAddedTableLayoutToTableScan()
    {
        tester().assertThat(pickTableLayout.pickTableLayoutWithoutPredicate())
                .on(p -> p.tableScan(
                        nationTableHandle,
                        ImmutableList.of(p.symbol("nationkey", BIGINT)),
                        ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT))))
                .matches(
                        constrainedTableScanWithTableLayout("nation", ImmutableMap.of(), ImmutableMap.of("nationkey", "nationkey")));
    }

    @Test
    public void ruleAddedTableLayoutToFilterTableScan()
    {
        Map<String, Domain> filterConstraint = ImmutableMap.<String, Domain>builder()
                .put("orderstatus", singleValue(createVarcharType(1), utf8Slice("F")))
                .build();
        tester().assertThat(pickTableLayout.pickTableLayoutForPredicate())
                .on(p -> p.filter(expression("orderstatus = CAST ('F' AS VARCHAR(1))"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1))))))
                .matches(
                        constrainedTableScanWithTableLayout("orders", filterConstraint, ImmutableMap.of("orderstatus", "orderstatus")));
    }

    @Test
    public void ruleAddedNewTableLayoutIfTableScanHasEmptyConstraint()
    {
        tester().assertThat(pickTableLayout.pickTableLayoutForPredicate())
                .on(p -> p.filter(expression("orderstatus = 'F'"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", createVarcharType(1))),
                                ImmutableMap.of(p.symbol("orderstatus", createVarcharType(1)), new TpchColumnHandle("orderstatus", createVarcharType(1))),
                                Optional.of(ordersTableLayoutHandle))))
                .matches(
                        constrainedTableScanWithTableLayout(
                                "orders",
                                ImmutableMap.of("orderstatus", singleValue(createVarcharType(1), utf8Slice("F"))),
                                ImmutableMap.of("orderstatus", "orderstatus")));
    }

    @Test
    public void ruleWithPushdownableToTableLayoutPredicate()
    {
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
