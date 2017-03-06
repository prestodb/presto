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
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.testing.TestingHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.spi.predicate.Domain.singleValue;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.constrainedTableScanWithTableLayout;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;

public class TestAddTableLayout
{
    public static final TableLayoutHandle DUMMY_TABLE_LAYOUT_HANDLE = new TableLayoutHandle(new ConnectorId("tpch"),
            TestingTransactionHandle.create(),
            TestingHandle.INSTANCE);

    private RuleTester tester;
    private Rule addTableLayout;
    private TableHandle nationTableHandle;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester();
        addTableLayout = new AddTableLayout(tester.getMetadata());

        ConnectorId connectorId = tester.getCurrentConnectorId();
        nationTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle(connectorId.toString(), "nation", 1.0));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester = null;
        addTableLayout = null;
    }

    @Test
    public void testDoesNotFireIfNoTableScan()
    {
        tester.assertThat(addTableLayout)
                .on(p -> p.values(p.symbol("a", BIGINT)))
                .doesNotFire();

        tester.assertThat(addTableLayout)
                .on(p -> p.filter(expression("b = 44"),
                        p.values(p.symbol("b", BIGINT))))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireIfTableScanHasTableLayout()
    {
        tester.assertThat(addTableLayout)
                .on(p -> p.tableScanWithTableLayout(
                        ImmutableList.of(p.symbol("nationkey", BIGINT)),
                        ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                        nationTableHandle,
                        DUMMY_TABLE_LAYOUT_HANDLE))
                .doesNotFire();

        tester.assertThat(addTableLayout)
                .on(p -> p.filter(expression("nationkey = CAST (44 AS BIGINT)"),
                        p.tableScanWithTableLayout(
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                                nationTableHandle,
                                DUMMY_TABLE_LAYOUT_HANDLE)))
                .doesNotFire();
    }

    @Test
    public void ruleAddedTableLayoutToTableScan()
    {
        // The TPCH connector returns a TableLayout, but that TableLayout doesn't handle any of the constraints.
        // However, we know that the rule fired because the constraints and TableLayout are included in the new plan.
        Map<String, Domain> emptyConstraint = ImmutableMap.<String, Domain>builder().build();
        tester.assertThat(addTableLayout)
                .on(p -> p.tableScan(
                        nationTableHandle,
                        ImmutableList.of(p.symbol("nationkey", BIGINT)),
                        ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT))))
                .matches(
                        constrainedTableScanWithTableLayout("nation", emptyConstraint, ImmutableMap.of("nationkey", "nationkey")));

        Map<String, Domain> filterConstraint = ImmutableMap.<String, Domain>builder()
                .put("nationkey", singleValue(BIGINT, 44L))
                .build();
        tester.assertThat(addTableLayout)
            .on(p -> p.filter(expression("nationkey = BIGINT '44'"),
                        p.tableScan(nationTableHandle,
                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)))))
            .matches(
                    PlanMatchPattern.filter("nationkey = BIGINT '44'",
                            constrainedTableScanWithTableLayout("nation", filterConstraint, ImmutableMap.of("nationkey", "nationkey"))));
    }
}
