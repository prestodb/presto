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

import com.facebook.presto.Session;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SystemSessionProperties.REWRITE_SEMI_JOIN_AGAINST_VALUES_TO_FILTER_MAX_SIZE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestRewriteSemiJoinAgainstValuesToFilterEndToEnd
{
    private static final String CATALOG = "local";

    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = new LocalQueryRunner(testSessionBuilder().setCatalog(CATALOG).setSchema("tiny").build());
        queryRunner.createCatalog(CATALOG, new TpchConnectorFactory(1), ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
    }

    private Session session(boolean enabled)
    {
        return testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema("tiny")
                .setSystemProperty(REWRITE_SEMI_JOIN_AGAINST_VALUES_TO_FILTER_MAX_SIZE, enabled ? "10000" : "0")
                .build();
    }

    private String planText(boolean enabled, String sql)
    {
        return queryRunner.inTransaction(session(enabled), transactionSession -> {
            Plan plan = queryRunner.createPlan(transactionSession, sql, WarningCollector.NOOP);
            return PlanPrinter.textLogicalPlan(
                    plan.getRoot(),
                    plan.getTypes(),
                    plan.getStatsAndCosts(),
                    queryRunner.getMetadata().getFunctionAndTypeManager(),
                    transactionSession,
                    0);
        });
    }

    @Test
    public void testAntiSemiJoinCollapsesToScanFilter()
    {
        String sql = "SELECT o.orderkey FROM orders o " +
                "LEFT JOIN (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT))) AS t(k) ON o.orderkey = t.k " +
                "WHERE t.k IS NULL";

        String enabledPlan = planText(true, sql);
        assertFalse(enabledPlan.contains("SemiJoin"), "Rule should remove the SemiJoin. Plan:\n" + enabledPlan);
        assertTrue(enabledPlan.matches("(?s).*ScanFilter\\[.*orders.*orderkey.*"), "Rule should push a filter to the orders scan. Plan:\n" + enabledPlan);

        String disabledPlan = planText(false, sql);
        assertTrue(disabledPlan.contains("SemiJoin"), "With the rule disabled the SemiJoin should survive. Plan:\n" + disabledPlan);
    }

    @Test
    public void testNotInSubqueryCollapsesToScanFilter()
    {
        String sql = "SELECT orderkey FROM orders WHERE orderkey NOT IN " +
                "(SELECT k FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT))) u(k))";

        String enabledPlan = planText(true, sql);
        assertFalse(enabledPlan.contains("SemiJoin"), "Rule should remove the SemiJoin. Plan:\n" + enabledPlan);
        assertTrue(enabledPlan.matches("(?s).*ScanFilter\\[.*orders.*orderkey.*"), "Rule should push a filter to the orders scan. Plan:\n" + enabledPlan);

        assertTrue(planText(false, sql).contains("SemiJoin"), "With the rule disabled the SemiJoin should survive.");
    }

    @Test
    public void testInSubqueryCollapsesToScanFilter()
    {
        String sql = "SELECT orderkey FROM orders WHERE orderkey IN " +
                "(SELECT k FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(3 AS BIGINT))) u(k))";

        String enabledPlan = planText(true, sql);
        assertFalse(enabledPlan.contains("SemiJoin"), "Rule should remove the SemiJoin. Plan:\n" + enabledPlan);
        assertTrue(enabledPlan.matches("(?s).*ScanFilter\\[.*orders.*orderkey.*"), "Rule should push a filter to the orders scan. Plan:\n" + enabledPlan);
    }

    @Test
    public void testCorrectnessNotInSubqueryNullInSource()
    {
        assertSameResults("SELECT x FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT)), (CAST(5 AS BIGINT))) s(x) " +
                "WHERE x NOT IN (SELECT k FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT))) f(k))");
    }

    @Test
    public void testCorrectnessNotInSubqueryNullInValues()
    {
        // SQL NOT IN with a NULL in the list returns no rows; the rewrite must agree.
        assertSameResults("SELECT x FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT)), (CAST(5 AS BIGINT))) s(x) " +
                "WHERE x NOT IN (SELECT k FROM (VALUES (CAST(1 AS BIGINT)), (CAST(NULL AS BIGINT))) f(k))");
    }

    @Test
    public void testCorrectnessNotInSubqueryAllNullValues()
    {
        // x NOT IN (all-NULL set) returns no rows; the rewrite collapses to constant FALSE.
        assertSameResults("SELECT x FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT)), (CAST(5 AS BIGINT))) s(x) " +
                "WHERE x NOT IN (SELECT k FROM (VALUES (CAST(NULL AS BIGINT))) f(k))");
    }

    @Test
    public void testCorrectnessInSubqueryAllNullValues()
    {
        // x IN (all-NULL set) matches nothing.
        assertSameResults("SELECT x FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT)), (CAST(5 AS BIGINT))) s(x) " +
                "WHERE x IN (SELECT k FROM (VALUES (CAST(NULL AS BIGINT))) f(k))");
    }

    @Test
    public void testCorrectnessNullInSource()
    {
        // Source has a NULL; an anti-join against a literal list keeps the NULL row and non-matching rows.
        assertSameResults("SELECT x FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT)), (CAST(5 AS BIGINT))) s(x) " +
                "LEFT JOIN (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT))) f(k) ON s.x = f.k WHERE f.k IS NULL");
    }

    @Test
    public void testCorrectnessNullInValues()
    {
        // Filtering side has a NULL; stripping it must not change anti-join results.
        assertSameResults("SELECT x FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT)), (CAST(5 AS BIGINT))) s(x) " +
                "LEFT JOIN (VALUES (CAST(1 AS BIGINT)), (CAST(NULL AS BIGINT))) f(k) ON s.x = f.k WHERE f.k IS NULL");
    }

    @Test
    public void testCorrectnessPositiveSemiJoin()
    {
        assertSameResults("SELECT x FROM (VALUES (CAST(1 AS BIGINT)), (CAST(2 AS BIGINT)), (CAST(NULL AS BIGINT)), (CAST(5 AS BIGINT))) s(x) " +
                "WHERE x IN (SELECT k FROM (VALUES (CAST(1 AS BIGINT)), (CAST(3 AS BIGINT))) f(k))");
    }

    private void assertSameResults(String sql)
    {
        MaterializedResult enabled = queryRunner.execute(session(true), sql);
        MaterializedResult disabled = queryRunner.execute(session(false), sql);
        assertEqualsIgnoreOrder(enabled.getMaterializedRows(), disabled.getMaterializedRows());
    }
}
