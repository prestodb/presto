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
import com.facebook.presto.sql.planner.iterative.RuleSet;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.predicate.Domain.singleValue;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createVarcharType;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.testing.Closeables.closeAllRuntimeException;

public class TestPickTableLayout
{
    private static final TableLayoutHandle DUMMY_TABLE_LAYOUT_HANDLE = new TableLayoutHandle(new ConnectorId("tpch"),
            TestingTransactionHandle.create(),
            new TpchTableLayoutHandle(new TpchTableHandle("local", "nation", 1.0), Optional.empty()));

    private RuleSet pickTableLayout;
    private TableHandle nationTableHandle;
    private TableHandle ordersTableHandle;
    private RuleTester tester;

    @BeforeClass
    public final void setUp()
    {
        tester = new RuleTester(true);
    }

    @AfterClass(alwaysRun = true)
    public final void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @BeforeMethod
    public void setUpPerMethod()
    {
        pickTableLayout = new PickTableLayout(tester.getMetadata(), new SqlParser());

        ConnectorId connectorId = tester.getCurrentConnectorId();
        nationTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle(connectorId.toString(), "nation", 1.0));
        ordersTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle(connectorId.toString(), "orders", 1.0));
    }

    @Test
    public void doesNotFireIfNoTableScan()
    {
        tester.assertThat(pickTableLayout)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();
    }

    @Test
    public void doesNotFireIfTableScanHasTableLayout()
    {
        tester.assertThat(pickTableLayout)
                .on(p -> p.tableScan(
                        nationTableHandle,
                        ImmutableList.of(p.symbol("nationkey", BIGINT)),
                        ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                        BooleanLiteral.TRUE_LITERAL,
                        Optional.of(DUMMY_TABLE_LAYOUT_HANDLE)))
                .doesNotFire();
    }

    @Test
    public void replacesExistingLayout()
    {
        // The TPCH connector returns a TableLayout, but that TableLayout doesn't handle any of the constraints.
        // However, we know that the rule fired because the constraints and TableLayout are included in the new plan.
        Map<String, Domain> filterConstraint = ImmutableMap.<String, Domain>builder()
                .put("nationkey", singleValue(BIGINT, 44L))
                .build();
        tester.assertThat(pickTableLayout)
                .on(p -> p.filter(expression("nationkey = BIGINT '44'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                                BooleanLiteral.TRUE_LITERAL,
                                Optional.of(DUMMY_TABLE_LAYOUT_HANDLE))))
                .matches(
                        filter("nationkey = BIGINT '44'",
                                constrainedTableScanWithTableLayout("nation", filterConstraint, ImmutableMap.of("nationkey", "nationkey"))));
    }

    @Test
    public void doesNotFireIfConstraintHasNotChanged()
    {
        // The TPCH connector returns a TableLayout, but that TableLayout doesn't handle any of the constraints.
        // However, we know that the rule fired because the constraints and TableLayout are included in the new plan.
        Map<String, Domain> filterConstraint = ImmutableMap.<String, Domain>builder()
                .put("nationkey", singleValue(BIGINT, 44L))
                .build();
        ColumnHandle columnHandle = new TpchColumnHandle("nationkey", BIGINT);
        tester.assertThat(pickTableLayout)
                .on(p -> p.filter(expression("nationkey = BIGINT '44'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                                TupleDomain.withColumnDomains(ImmutableMap.of(columnHandle, filterConstraint.get("nationkey"))),
                                expression("nationkey = BIGINT '44'"),
                                Optional.of(DUMMY_TABLE_LAYOUT_HANDLE))))
                .doesNotFire();
    }

    @Test
    public void ruleAddedTableLayoutToTableScan()
    {
        // The TPCH connector returns a TableLayout, but that TableLayout doesn't handle any of the constraints.
        // However, we know that the rule fired because the constraints and TableLayout are included in the new plan.
        Map<String, Domain> emptyConstraint = ImmutableMap.<String, Domain>builder().build();
        tester.assertThat(pickTableLayout)
                .on(p -> p.tableScan(
                        nationTableHandle,
                        ImmutableList.of(p.symbol("nationkey", BIGINT)),
                        ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT))))
                .matches(
                        constrainedTableScanWithTableLayout("nation", emptyConstraint, ImmutableMap.of("nationkey", "nationkey")));
    }

    @Test
    public void ruleAddedTableLayoutToFilterTableScan()
    {
        // The TPCH connector returns a TableLayout, but that TableLayout doesn't handle any of the constraints.
        // However, we know that the rule fired because the constraints and TableLayout are included in the new plan.
        Map<String, Domain> filterConstraint = ImmutableMap.<String, Domain>builder()
                .put("nationkey", singleValue(BIGINT, 44L))
                .build();
        tester.assertThat(pickTableLayout)
                .on(p -> p.filter(expression("nationkey = BIGINT '44'"),
                        p.tableScan(
                                nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)))))
                .matches(
                        filter("nationkey = BIGINT '44'",
                                constrainedTableScanWithTableLayout("nation", filterConstraint, ImmutableMap.of("nationkey", "nationkey"))));
    }

    @Test
    public void ruleAddsFullyEnforcedTableLayout()
    {
        Type orderStatusType = createVarcharType(1);
        Map<String, Domain> filterConstraint = ImmutableMap.<String, Domain>builder()
                .put("orderstatus", singleValue(orderStatusType, utf8Slice("O")))
                .build();
        tester.assertThat(pickTableLayout)
                .on(p -> p.filter(expression("orderstatus = 'O'"),
                        p.tableScan(
                                ordersTableHandle,
                                ImmutableList.of(p.symbol("orderstatus", orderStatusType)),
                                ImmutableMap.of(p.symbol("orderstatus", orderStatusType), new TpchColumnHandle("orderstatus", orderStatusType)))))
                .matches(constrainedTableScanWithTableLayout("orders", filterConstraint, ImmutableMap.of("orderstatus", "orderstatus")));
    }
}
