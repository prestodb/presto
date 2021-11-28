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
package com.facebook.presto.sql.query;

import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.sql.planner.LogicalPlanner;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.util.MorePredicates.isInstanceOfAny;
import static org.testng.Assert.assertFalse;

public class TestFilteredAggregations
        extends BasePlanTest
{
    private QueryAssertions assertions;

    public TestFilteredAggregations()
    {
        super(ImmutableMap.of(AGGREGATION_IF_TO_FILTER_REWRITE_STRATEGY, "filter_with_if"));
    }

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
                "SELECT sum(IF(x > 0, x)) FROM (VALUES 1, 1, 0, 2, 3, 3) t(x)",
                "VALUES (BIGINT '10')");

        assertions.assertQuery(
                "SELECT sum(x) FILTER(WHERE x > 0), sum(x) FILTER(WHERE x < 3) FROM (VALUES 1, 1, 0, 5, 3, 8) t(x)",
                "VALUES (BIGINT '18', BIGINT '2')");
        assertions.assertQuery(
                "SELECT sum(IF(x > 0, x)), sum(IF(x < 3, x)) FROM (VALUES 1, 1, 0, 5, 3, 8) t(x)",
                "VALUES (BIGINT '18', BIGINT '2')");
        assertions.assertQuery(
                "SELECT sum(IF(x > 0, x)), sum(x) FILTER(WHERE x < 3) FROM (VALUES 1, 1, 0, 5, 3, 8) t(x)",
                "VALUES (BIGINT '18', BIGINT '2')");

        assertions.assertQuery(
                "SELECT sum(x) FILTER(WHERE x > 1), sum(x) FROM (VALUES 1, 1, 0, 2, 3, 3) t(x)",
                "VALUES (BIGINT '8', BIGINT '10')");
        assertions.assertQuery(
                "SELECT sum(IF(x > 1, x)), sum(x) FROM (VALUES 1, 1, 0, 2, 3, 3) t(x)",
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
                "SELECT count(DISTINCT IF(x > 1, x)) " +
                        "FROM (VALUES 1, 1, 1, 2, 3, 3) t(x)",
                "VALUES BIGINT '2'");

        assertions.assertQuery(
                "SELECT count(DISTINCT x) FILTER (WHERE x > 1), sum(DISTINCT x) " +
                        "FROM (VALUES 1, 1, 1, 2, 3, 3) t(x)",
                "VALUES (BIGINT '2', BIGINT '6')");
        assertions.assertQuery(
                "SELECT count(DISTINCT IF(x > 1, x)), sum(DISTINCT x) " +
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
                "SELECT count(DISTINCT IF(x > 1, x)), sum(DISTINCT IF(x < 3, y)) " +
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
        assertions.assertQuery(
                "SELECT count(IF(x > 1, x)),  sum(DISTINCT x) " +
                        "FROM (VALUES 1, 2, 3, 3) t(x)",
                "VALUES (BIGINT '3', BIGINT '6')");
    }

    @Test
    public void testSetAggWithNulls()
    {
        assertions.assertQuery(
                "SELECT y, set_agg(y) FILTER (WHERE x = 1) FROM (SELECT 1 x, 2 y UNION ALL SELECT NULL x, 20 y UNION ALL SELECT 1 x, NULL y) GROUP BY y ORDER BY y",
                "VALUES (INTEGER '2', ARRAY[INTEGER '2']), (INTEGER '20', CAST(NULL AS ARRAY<INTEGER>)), (CAST(NULL AS INTEGER), ARRAY[CAST(NULL AS INTEGER)])");
        assertions.assertQuery(
                "SELECT y, set_agg(IF(x = 1,y)) FROM (SELECT 1 x, 2 y UNION ALL SELECT NULL x, 20 y UNION ALL SELECT 1 x, NULL y) GROUP BY y ORDER BY y",
                "VALUES (INTEGER '2', ARRAY[INTEGER '2']), (INTEGER '20', ARRAY[CAST(NULL AS INTEGER)]), (CAST(NULL AS INTEGER), ARRAY[CAST(NULL AS INTEGER)])");
    }

    @Test
    public void testApproxSet()
    {
        assertions.assertQuery(
                "SELECT y, approx_set(y) FILTER (WHERE x = 1) FROM (SELECT NULL x, 20 y UNION ALL SELECT 1 x, NULL y) GROUP BY y ORDER BY y",
                "VALUES (INTEGER '20', CAST(NULL AS HyperLogLog)), (CAST(NULL AS INTEGER), CAST(NULL AS HyperLogLog))");
        assertions.assertQuery(
                "SELECT y, approx_set(IF(x = 1,y)) FROM (SELECT NULL x, 20 y UNION ALL SELECT 1 x, NULL y) GROUP BY y ORDER BY y",
                "VALUES (INTEGER '20', CAST(NULL AS HyperLogLog)), (CAST(NULL AS INTEGER), CAST(NULL AS HyperLogLog))");
    }

    @Test
    public void testSetUnion()
    {
        assertions.assertQuery(
                "SELECT set_union(x) FILTER (WHERE y > 1) FROM (SELECT ARRAY[1] x, 1 y UNION ALL SELECT NULL x, 1 y)",
                "VALUES (CAST (NULL AS ARRAY<INTEGER>))");
        assertions.assertQuery(
                "SELECT set_union(IF(y > 1, x)) FROM (SELECT ARRAY[1] x, 1 y UNION ALL SELECT NULL x, 1 y)",
                "VALUES (CAST (ARRAY[] AS ARRAY<INTEGER>))");
    }

    @Test
    public void testMapUnion()
    {
        assertions.assertQuery(
                "SELECT map_union(x) FILTER (WHERE y > 1) FROM (SELECT MAP(ARRAY[1], ARRAY[1]) x, 1 y UNION ALL SELECT NULL x, 1 y)",
                "VALUES (CAST (NULL AS MAP<INTEGER, INTEGER>))");
        assertions.assertQuery(
                "SELECT map_union(IF(y > 1, x)) FROM (SELECT MAP(ARRAY[1], ARRAY[1]) x, 1 y UNION ALL SELECT NULL x, 1 y)",
                "VALUES (CAST (NULL AS MAP<INTEGER, INTEGER>))");
    }

    @Test
    public void testMapUnionSum()
    {
        assertions.assertQuery(
                "SELECT map_union_sum(x) FILTER (WHERE y > 1) FROM (SELECT MAP(ARRAY[1], ARRAY[1]) x, 1 y UNION ALL SELECT NULL x, 1 y)",
                "VALUES (CAST (NULL AS MAP<INTEGER, INTEGER>))");
        assertions.assertQuery(
                "SELECT map_union_sum(IF(y > 1, x)) FROM (SELECT MAP(ARRAY[1], ARRAY[1]) x, 1 y UNION ALL SELECT NULL x, 1 y)",
                "VALUES (CAST (NULL AS MAP<INTEGER, INTEGER>))");
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

        assertions.assertQuery(
                "SELECT k, count(DISTINCT IF(y = 100, x)),  count(DISTINCT IF(y = 200, x)) FROM " +
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
                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey", "custkey")))));

        assertPlan(
                "SELECT sum(IF(totalprice > 0, totalprice)), sum(IF(custkey > 0, custkey)) FROM orders",
                anyTree(
                        filter(
                                "(\"totalprice\" > 0E0 OR \"custkey\" > BIGINT '0')",
                                tableScan("orders", ImmutableMap.of("totalprice", "totalprice", "custkey", "custkey")))));
    }

    @Test
    public void testDoNotPushdownPredicateIfNonFilteredAggregateIsPresent()
    {
        assertPlanContainsNoFilter("SELECT sum(totalprice) FILTER(WHERE totalprice > 0), sum(custkey) FROM orders");
        assertPlanContainsNoFilter("SELECT sum(IF(totalprice > 0, totalprice)), sum(custkey) FROM orders");
    }

    @Test
    public void testPushDownConstantFilterPredicate()
    {
        assertPlanContainsNoFilter("SELECT sum(totalprice) FILTER(WHERE FALSE) FROM orders");
        assertPlanContainsNoFilter("SELECT sum(IF(FALSE, totalprice)) FROM orders");

        assertPlanContainsNoFilter("SELECT sum(totalprice) FILTER(WHERE TRUE) FROM orders");
        assertPlanContainsNoFilter("SELECT sum(IF(TRUE, totalprice)) FROM orders");
    }

    @Test
    public void testNoFilterAddedForConstantValueFilters()
    {
        assertPlanContainsNoFilter("SELECT sum(x) FILTER(WHERE x > 0) FROM (VALUES 1, 1, 0, 2, 3, 3) t(x) GROUP BY x");
        assertPlanContainsNoFilter("SELECT sum(IF(x > 0, x)) FROM (VALUES 1, 1, 0, 2, 3, 3) t(x) GROUP BY x");

        assertPlanContainsNoFilter("SELECT sum(totalprice) FILTER(WHERE totalprice > 0) FROM orders GROUP BY totalprice");
        assertPlanContainsNoFilter("SELECT sum(IF(totalprice > 0, totalprice)) FROM orders GROUP BY totalprice");
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
