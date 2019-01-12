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
package io.prestosql.sql.query;

import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.assertions.BasePlanTest;
import io.prestosql.sql.planner.plan.FilterNode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.util.MorePredicates.isInstanceOfAny;
import static org.testng.Assert.assertFalse;

public class TestFilteredAggregations
        extends BasePlanTest
{
    private QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testAddPredicateForFilterClauses()
    {
        assertions.assertQuery(
                "SELECT sum(x) FILTER(WHERE x > 0) FROM (VALUES 1, 1, 0, 2, 3, 3) t(x)",
                "VALUES (BIGINT '10')");

        assertions.assertQuery(
                "SELECT sum(x) FILTER(WHERE x > 0), sum(x) FILTER(WHERE x < 3) FROM (VALUES 1, 1, 0, 5, 3, 8) t(x)",
                "VALUES (BIGINT '18', BIGINT '2')");

        assertions.assertQuery(
                "SELECT sum(x) FILTER(WHERE x > 1), sum(x) FROM (VALUES 1, 1, 0, 2, 3, 3) t(x)",
                "VALUES (BIGINT '8', BIGINT '10')");
    }

    @Test
    public void testGroupAll()
    {
        assertions.assertQuery(
                "SELECT count(DISTINCT x) FILTER (WHERE x > 1) " +
                        "FROM (VALUES 1, 1, 1, 2, 3, 3) t(x)",
                "VALUES BIGINT '2'");

        assertions.assertQuery(
                "SELECT count(DISTINCT x) FILTER (WHERE x > 1), sum(DISTINCT x) " +
                        "FROM (VALUES 1, 1, 1, 2, 3, 3) t(x)",
                "VALUES (BIGINT '2', BIGINT '6')");

        assertions.assertQuery(
                "SELECT count(DISTINCT x) FILTER (WHERE x > 1), sum(DISTINCT y) FILTER (WHERE x < 3)" +
                        "FROM (VALUES " +
                        "(1, 10)," +
                        "(1, 20)," +
                        "(1, 20)," +
                        "(2, 20)," +
                        "(3, 30)) t(x, y)",
                "VALUES (BIGINT '2', BIGINT '30')");

        assertions.assertQuery(
                "SELECT count(x) FILTER (WHERE x > 1), sum(DISTINCT x) " +
                        "FROM (VALUES 1, 2, 3, 3) t(x)",
                "VALUES (BIGINT '3', BIGINT '6')");
    }

    @Test
    public void testGroupingSets()
    {
        assertions.assertQuery(
                "SELECT k, count(DISTINCT x) FILTER (WHERE y = 100), count(DISTINCT x) FILTER (WHERE y = 200) FROM " +
                        "(VALUES " +
                        "   (1, 1, 100)," +
                        "   (1, 1, 200)," +
                        "   (1, 2, 100)," +
                        "   (1, 3, 300)," +
                        "   (2, 1, 100)," +
                        "   (2, 10, 100)," +
                        "   (2, 20, 100)," +
                        "   (2, 20, 200)," +
                        "   (2, 30, 300)," +
                        "   (2, 40, 100)" +
                        ") t(k, x, y) " +
                        "GROUP BY GROUPING SETS ((), (k))",
                "VALUES " +
                        "(1, BIGINT '2', BIGINT '1'), " +
                        "(2, BIGINT '4', BIGINT '1'), " +
                        "(CAST(NULL AS INTEGER), BIGINT '5', BIGINT '2')");
    }

    @Test
    public void rewriteAddFilterWithMultipleFilters()
    {
        assertPlan(
                "SELECT sum(totalprice) FILTER(WHERE totalprice > 0), sum(custkey) FILTER(WHERE custkey > 0) FROM orders",
                anyTree(
                        filter(
                                "(\"totalprice\" > 0E0 OR \"custkey\" > BIGINT '0')",
                                tableScan(
                                        "orders", ImmutableMap.of("totalprice", "totalprice",
                                                "custkey", "custkey")))));
    }

    @Test
    public void testDoNotPushdownPredicateIfNonFilteredAggregateIsPresent()
    {
        assertPlanContainsNoFilter("SELECT sum(totalprice) FILTER(WHERE totalprice > 0), sum(custkey) FROM orders");
    }

    @Test
    public void testPushDownConstantFilterPredicate()
    {
        assertPlanContainsNoFilter("SELECT sum(totalprice) FILTER(WHERE FALSE) FROM orders");

        assertPlanContainsNoFilter("SELECT sum(totalprice) FILTER(WHERE TRUE) FROM orders");
    }

    @Test
    public void testNoFilterAddedForConstantValueFilters()
    {
        assertPlanContainsNoFilter("SELECT sum(x) FILTER(WHERE x > 0) FROM (VALUES 1, 1, 0, 2, 3, 3) t(x) GROUP BY x");

        assertPlanContainsNoFilter("SELECT sum(totalprice) FILTER(WHERE totalprice > 0) FROM orders GROUP BY totalprice");
    }

    private void assertPlanContainsNoFilter(String sql)
    {
        assertFalse(
                searchFrom(plan(sql, LogicalPlanner.Stage.OPTIMIZED).getRoot())
                        .where(isInstanceOfAny(FilterNode.class))
                        .matches(),
                "Unexpected node for query: " + sql);
    }
}
