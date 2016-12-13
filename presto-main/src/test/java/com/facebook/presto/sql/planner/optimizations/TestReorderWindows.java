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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.ExpectedValueProvider;
import com.facebook.presto.sql.planner.assertions.PlanAssert;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.specification;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.window;

public class TestReorderWindows
        extends BasePlanTest
{
    private static final String DISCOUNT_ALIAS = "DISCOUNT";
    private static final String ORDERKEY_ALIAS = "ORDERKEY";
    private static final String QUANTITY_ALIAS = "QUANTITY";
    private static final String PARTKEY_ALIAS = "PARTKEY";
    private static final String RECEIPTDATE_ALIAS = "RECEIPTDATE";
    private static final String SHIPDATE_ALIAS = "SHIPDATE";
    private static final String SUPPKEY_ALIAS = "SUPPKEY";
    private static final String TAX_ALIAS = "TAX";

    private static final PlanMatchPattern LINEITEM_TABLESCAN_DOQPRSST;
    private static final PlanMatchPattern LINEITEM_TABLESCAN_DOQRST;

    private static final Optional<WindowFrame> commonFrame;

    private static final ExpectedValueProvider<WindowNode.Specification> windowA;
    private static final ExpectedValueProvider<WindowNode.Specification> windowAp;
    private static final ExpectedValueProvider<WindowNode.Specification> windowApp;
    private static final ExpectedValueProvider<WindowNode.Specification> windowB;

    static {
        ImmutableMap.Builder<String, String> columns = ImmutableMap.builder();
        columns.put(DISCOUNT_ALIAS, "discount");
        columns.put(ORDERKEY_ALIAS, "orderkey");
        columns.put(QUANTITY_ALIAS, "quantity");
        columns.put(PARTKEY_ALIAS, "partkey");
        columns.put(RECEIPTDATE_ALIAS, "receiptdate");
        columns.put(SHIPDATE_ALIAS, "shipdate");
        columns.put(SUPPKEY_ALIAS, "suppkey");
        columns.put(TAX_ALIAS, "tax");
        LINEITEM_TABLESCAN_DOQPRSST = tableScan("lineitem", columns.build());

        columns = ImmutableMap.builder();
        columns.put(DISCOUNT_ALIAS, "discount");
        columns.put(ORDERKEY_ALIAS, "orderkey");
        columns.put(QUANTITY_ALIAS, "quantity");
        columns.put(RECEIPTDATE_ALIAS, "receiptdate");
        columns.put(SUPPKEY_ALIAS, "suppkey");
        columns.put(TAX_ALIAS, "tax");
        LINEITEM_TABLESCAN_DOQRST = tableScan("lineitem", columns.build());

        commonFrame = Optional.empty();

        windowA = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(ORDERKEY_ALIAS),
                ImmutableMap.of(ORDERKEY_ALIAS, SortOrder.ASC_NULLS_LAST));

        windowAp = specification(
                ImmutableList.of(SUPPKEY_ALIAS),
                ImmutableList.of(SHIPDATE_ALIAS),
                ImmutableMap.of(SHIPDATE_ALIAS, SortOrder.ASC_NULLS_LAST));

        windowApp = specification(
                ImmutableList.of(SUPPKEY_ALIAS, TAX_ALIAS),
                ImmutableList.of(RECEIPTDATE_ALIAS),
                ImmutableMap.of(RECEIPTDATE_ALIAS, SortOrder.ASC_NULLS_LAST));

        windowB = specification(
                ImmutableList.of(PARTKEY_ALIAS),
                ImmutableList.of(RECEIPTDATE_ALIAS),
                ImmutableMap.of(RECEIPTDATE_ALIAS, SortOrder.ASC_NULLS_LAST));
    }

    @Test
    public void testNonMergeableABAReordersToAABAllOptimizers()
    {
        @Language("SQL") String sql = "select " +
                "sum(quantity) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) sum_quantity_A, " +
                "avg(discount) over(PARTITION BY partkey ORDER BY receiptdate ASC NULLS LAST) avg_discount_B, " +
                "min(tax) over(PARTITION BY suppkey ORDER BY shipdate ASC NULLS LAST) min_tax_A " +
                "from lineitem";

        PlanMatchPattern pattern =
                anyTree(
                        window(windowAp,
                                ImmutableList.of(
                                        functionCall("min", commonFrame, ImmutableList.of(TAX_ALIAS))),
                                window(windowA,
                                        ImmutableList.of(
                                                functionCall("sum", commonFrame, ImmutableList.of(QUANTITY_ALIAS))),
                                        anyTree(
                                        window(windowB,
                                                ImmutableList.of(
                                                        functionCall("avg", commonFrame, ImmutableList.of(DISCOUNT_ALIAS))),
                                                anyTree(LINEITEM_TABLESCAN_DOQPRSST))))));

        assertPlan(sql, pattern);
    }

    @Test
    public void testNonMergeableABAReordersToAAB()
    {
        @Language("SQL") String sql = "select " +
                "sum(quantity) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) sum_quantity_A, " +
                "avg(discount) over(PARTITION BY partkey ORDER BY receiptdate ASC NULLS LAST) avg_discount_B, " +
                "min(tax) over(PARTITION BY suppkey ORDER BY shipdate ASC NULLS LAST) min_tax_A " +
                "from lineitem";

        assertUnitPlan(sql,
                anyTree(
                        window(windowAp,
                                ImmutableList.of(
                                        functionCall("min", commonFrame, ImmutableList.of(TAX_ALIAS))),
                                window(windowA,
                                        ImmutableList.of(
                                                functionCall("sum", commonFrame, ImmutableList.of(QUANTITY_ALIAS))),
                                        window(windowB,
                                                ImmutableList.of(
                                                        functionCall("avg", commonFrame, ImmutableList.of(DISCOUNT_ALIAS))),
                                                LINEITEM_TABLESCAN_DOQPRSST))))); // should be anyTree(LINEITEM_TABLESCANE_DOQPRSST) but anyTree does not handle zero nodes case correctly
    }

    @Test
    public void testPrefixOfPartitionComesFirstRegardlessOfTheirOrderInSQL()
    {
        {
            @Language("SQL") String sql = "select " +
                    "avg(discount) over(PARTITION BY suppkey, tax ORDER BY receiptdate ASC NULLS LAST) avg_discount_A, " +
                    "sum(quantity) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) sum_quantity_A " +
                    "from lineitem";

            assertUnitPlan(sql,
                    anyTree(window(windowApp,
                            ImmutableList.of(
                                    functionCall("avg", commonFrame, ImmutableList.of(DISCOUNT_ALIAS))),
                            window(windowA,
                                    ImmutableList.of(
                                            functionCall("sum", commonFrame, ImmutableList.of(QUANTITY_ALIAS))),
                                    LINEITEM_TABLESCAN_DOQRST)))); // should be anyTree(LINEITEM_TABLESCAN_DOQRST) but anyTree does not handle zero nodes case correctly
        }

        {
            @Language("SQL") String sql = "select " +
                    "sum(quantity) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) sum_quantity_A, " +
                    "avg(discount) over(PARTITION BY suppkey, tax ORDER BY receiptdate ASC NULLS LAST) avg_discount_A " +
                    "from lineitem";

            assertUnitPlan(sql,
                    anyTree(window(windowApp,
                            ImmutableList.of(
                                    functionCall("avg", commonFrame, ImmutableList.of(DISCOUNT_ALIAS))),
                            window(windowA,
                                    ImmutableList.of(
                                            functionCall("sum", commonFrame, ImmutableList.of(QUANTITY_ALIAS))),
                                    LINEITEM_TABLESCAN_DOQRST)))); // should be anyTree(LINEITEM_TABLESCAN_DOQRST) but anyTree does not handle zero nodes case correctly
        }
    }

    @Test
    public void testNotReorderAcrossNonWindowNodes()
    {
        @Language("SQL") String sql = "select " +
                "avg(discount) over(PARTITION BY suppkey, tax ORDER BY receiptdate ASC NULLS LAST) avg_discount_A, " +
                "lag(quantity, 1) over(PARTITION BY suppkey ORDER BY orderkey ASC NULLS LAST) lag_quantity_A " + // produces ProjectNode because of constant 1
                "from lineitem";

        assertUnitPlan(sql,
                anyTree(window(windowA,
                        ImmutableList.of(
                                functionCall("lag", commonFrame, ImmutableList.of(QUANTITY_ALIAS, "ONE"))),
                        project(ImmutableMap.of("ONE", expression("CAST(1 AS bigint)")),
                        window(windowApp,
                                ImmutableList.of(
                                        functionCall("avg", commonFrame, ImmutableList.of(DISCOUNT_ALIAS))),
                                LINEITEM_TABLESCAN_DOQRST))))); // should be anyTree(LINEITEM_TABLESCAN_DOQRST) but anyTree does not handle zero nodes case correctly
    }

    private void assertUnitPlan(@Language("SQL") String sql, PlanMatchPattern pattern)
    {
        LocalQueryRunner queryRunner = getQueryRunner();
        List<PlanOptimizer> optimizers = ImmutableList.of(
                new UnaliasSymbolReferences(),
                new PruneIdentityProjections(),
                new ReorderWindows(),
                new PruneUnreferencedOutputs());
        queryRunner.inTransaction(transactionSession -> {
            Plan actualPlan = queryRunner.createPlan(transactionSession, sql, optimizers);
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan, pattern);
            return null;
        });
    }
}
