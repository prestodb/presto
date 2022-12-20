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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.CONNECTOR;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.REMOVE_SAFE_CONSTANTS;
import static com.facebook.presto.sql.planner.CanonicalPlanGenerator.generateCanonicalPlan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.graph.Traverser.forTree;
import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestCanonicalPlanHashes
        extends BasePlanTest
{
    @Test
    public void testScanFilterProject()
            throws Exception
    {
        assertSamePlanHash(
                "SELECT totalprice, coalesce(orderkey, custkey) from orders WHERE custkey > 100 AND custkey < 120",
                "SELECT totalprice, coalesce(orderkey, custkey) from orders WHERE custkey > 100 AND custkey < 120",
                CONNECTOR);
        assertSamePlanHash(
                "SELECT totalprice, orderkey / 2 from orders WHERE custkey > 100 AND custkey < 120",
                "SELECT totalprice, orderkey / 4 from orders WHERE custkey > 100 AND custkey < 120",
                REMOVE_SAFE_CONSTANTS);

        assertDifferentPlanHash(
                "SELECT totalprice from orders WHERE custkey > 100 AND custkey < 110",
                "SELECT totalprice from orders WHERE custkey > 100 AND custkey < 120",
                CONNECTOR);
        assertDifferentPlanHash(
                "SELECT totalprice from orders WHERE custkey > 100 AND custkey < 110",
                "SELECT totalprice from orders WHERE custkey > 100 AND custkey < 120",
                REMOVE_SAFE_CONSTANTS);
        assertSamePlanHash(
                "SELECT cast(totalprice as varchar), orderkey / 2.0 from orders WHERE custkey > 100 AND custkey < 120",
                "SELECT cast(totalprice as varchar), orderkey / 4.0 from orders WHERE custkey > 100 AND custkey < 120",
                REMOVE_SAFE_CONSTANTS);
    }

    @Test
    public void testGroupBy()
            throws Exception
    {
        assertSamePlanHash(
                "SELECT COUNT_IF(totalprice > 0) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey, orderstatus",
                "SELECT COUNT_IF(totalprice > 0) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey, orderstatus",
                CONNECTOR);
        assertSamePlanHash(
                "SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY GROUPING SETS ((shippriority), (shippriority, custkey))",
                "SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY GROUPING SETS ((shippriority), (shippriority, custkey))",
                CONNECTOR);
        assertDifferentPlanHash(
                "SELECT COUNT_IF(totalprice > 0) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey, orderstatus",
                "SELECT COUNT_IF(totalprice > 5) from orders WHERE custkey > 100 AND custkey < 250 GROUP BY orderkey, orderstatus",
                REMOVE_SAFE_CONSTANTS);
        assertSamePlanHash(
                "SELECT COUNT_IF(totalprice > 0), 1 from (select *, shippriority/2 as pri from orders) WHERE custkey > 100 AND custkey < 200 GROUP BY GROUPING SETS ((pri), (shippriority, custkey))",
                "SELECT COUNT_IF(totalprice > 0), 2 from (select *, shippriority/4 as pri from orders) WHERE custkey > 100 AND custkey < 200 GROUP BY GROUPING SETS ((pri), (shippriority, custkey))",
                REMOVE_SAFE_CONSTANTS);

        assertDifferentPlanHash(
                "SELECT COUNT(totalprice) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey",
                "SELECT SUM(totalprice) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey",
                CONNECTOR);
        assertDifferentPlanHash(
                "SELECT COUNT(totalprice) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey",
                "SELECT COUNT(DISTINCT totalprice) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey",
                CONNECTOR);
        assertDifferentPlanHash(
                "SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY GROUPING SETS ((shippriority), (shippriority, custkey))",
                "SELECT shippriority, custkey, sum(totalprice) FROM orders GROUP BY GROUPING SETS ((shippriority), (custkey))",
                CONNECTOR);

        assertDifferentPlanHash(
                "SELECT COUNT(totalprice) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey",
                "SELECT SUM(totalprice) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey",
                REMOVE_SAFE_CONSTANTS);
        assertDifferentPlanHash(
                "SELECT COUNT(totalprice) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey",
                "SELECT COUNT(DISTINCT totalprice) from orders WHERE custkey > 100 AND custkey < 200 GROUP BY orderkey",
                REMOVE_SAFE_CONSTANTS);
        assertDifferentPlanHash(
                "SELECT COUNT_IF(totalprice > 0) from (select *, shippriority/2 as pri from orders) WHERE custkey > 100 AND custkey < 200 GROUP BY GROUPING SETS ((pri), (shippriority, custkey))",
                "SELECT COUNT_IF(totalprice > 0) from (select *, shippriority/2 as pri from orders) WHERE custkey > 100 AND custkey < 250 GROUP BY GROUPING SETS ((pri), (custkey))",
                REMOVE_SAFE_CONSTANTS);
    }

    @Test
    public void testUnnest()
            throws Exception
    {
        assertSamePlanHash(
                "SELECT a.custkey, t.e FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders) a CROSS JOIN UNNEST(my_array) AS t(e)",
                "SELECT a.custkey, t.e FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders) a CROSS JOIN UNNEST(my_array) AS t(e)",
                CONNECTOR);

        assertDifferentPlanHash(
                "SELECT a.custkey, t.e FROM (SELECT custkey, ARRAY[1, 2, 3, 4] AS my_array FROM orders) a CROSS JOIN UNNEST(my_array) AS t(e)",
                "SELECT a.custkey, t.e FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders) a CROSS JOIN UNNEST(my_array) AS t(e)",
                CONNECTOR);

        assertSamePlanHash(
                "SELECT a.custkey, t.e FROM (SELECT custkey, ARRAY[1, 2, 3, 4] AS my_array FROM orders) a CROSS JOIN UNNEST(my_array) AS t(e)",
                "SELECT a.custkey, t.e FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders) a CROSS JOIN UNNEST(my_array) AS t(e)",
                REMOVE_SAFE_CONSTANTS);
    }

    @Test
    public void testStatsEquivalentPlanNodesMarking()
    {
        List<PlanNode> nodes =
                getStatsEquivalentPlanHashes("SELECT COUNT(totalprice) from orders WHERE custkey > 100 GROUP BY orderkey");
        assertTrue(nodes.stream().anyMatch(node -> node instanceof AggregationNode));
        assertTrue(nodes.stream().anyMatch(node -> node instanceof FilterNode));
        assertTrue(nodes.stream().anyMatch(node -> node instanceof ProjectNode));
        assertTrue(nodes.stream().anyMatch(node -> node instanceof TableScanNode));
        assertEquals(nodes.size(), 6);
    }

    @Test
    public void testUnion()
            throws Exception
    {
        assertSamePlanHash(
                "SELECT orderkey, custkey FROM orders where orderkey < 1000 UNION ALL SELECT orderkey, custkey FROM orders where orderkey >= 1000 and orderkey < 2000 UNION ALL SELECT orderkey, custkey FROM orders where orderkey >= 2000 and orderkey < 3000",
                "SELECT orderkey, custkey FROM orders where orderkey >= 2000 and orderkey < 3000 UNION ALL SELECT orderkey, custkey FROM orders where orderkey < 1000 UNION ALL SELECT orderkey, custkey FROM orders where orderkey >= 1000 and orderkey < 2000",
                CONNECTOR);
        assertDifferentPlanHash(
                "SELECT orderkey, custkey, 1 as x FROM orders where orderkey < 1000 UNION ALL SELECT orderkey, custkey, 1 as x FROM orders where orderkey >= 1000 and orderkey < 2000 UNION ALL SELECT orderkey, custkey, 1 as x FROM orders where orderkey >= 2000 and orderkey < 3000",
                "SELECT orderkey, custkey, 2 as x FROM orders where orderkey >= 2000 and orderkey < 3000 UNION ALL SELECT orderkey, custkey, 2 as x FROM orders where orderkey < 1000 UNION ALL SELECT orderkey, custkey, 2 as x FROM orders where orderkey >= 1000 and orderkey < 2000",
                CONNECTOR);
        assertSamePlanHash(
                "SELECT orderkey, custkey, 1 as x FROM orders where orderkey < 1000 UNION ALL SELECT orderkey, custkey, 1 as x FROM orders where orderkey >= 1000 and orderkey < 2000 UNION ALL SELECT orderkey, custkey, 1 as x FROM orders where orderkey >= 2000 and orderkey < 3000",
                "SELECT orderkey, custkey, 2 as x FROM orders where orderkey >= 2000 and orderkey < 3000 UNION ALL SELECT orderkey, custkey, 2 as x FROM orders where orderkey < 1000 UNION ALL SELECT orderkey, custkey, 2 as x FROM orders where orderkey >= 1000 and orderkey < 2000",
                REMOVE_SAFE_CONSTANTS);
    }

    @Test
    public void testWindow()
            throws Exception
    {
        String query1 = "SELECT orderkey, SUM(custkey) OVER (PARTITION BY orderstatus ORDER BY totalprice) FROM orders where orderkey < 1000";
        String query2 = "SELECT orderkey, SUM(custkey) OVER (PARTITION BY orderstatus ORDER BY totalprice) FROM orders where orderkey < 2000";

        assertSamePlanHash(query1 + " UNION ALL " + query2, query2 + " UNION ALL " + query1, CONNECTOR);

        assertDifferentPlanHash(query1,
                "SELECT orderkey, SUM(custkey) OVER (PARTITION BY totalprice ORDER BY totalprice) FROM orders where orderkey < 1000",
                CONNECTOR);
    }

    @Test
    public void testValues()
            throws Exception
    {
        String query1 = "SELECT * FROM ( VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)";
        String query2 = "SELECT * FROM ( VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(idd, name)";
        String query3 = "SELECT * FROM ( VALUES (1, 'a'), (3, 'b'), (2, 'c')) AS t(id, name)";

        assertSamePlanHash(query1, query2, CONNECTOR);
        assertDifferentPlanHash(query1, query3, CONNECTOR);
    }

    @Test
    public void testSort()
            throws Exception
    {
        String query1 = "SELECT * FROM nation where substr(name, 1, 1) = 'A' ORDER BY regionkey";
        String query2 = "SELECT * FROM nation where substr(name, 1, 1) = 'A' ORDER BY nationkey";

        assertSamePlanHash(query1, query1, CONNECTOR);
        assertDifferentPlanHash(query1, query2, CONNECTOR);
    }

    @Test
    public void testMarkDistinct()
            throws Exception
    {
        String query = "SELECT count(*), count(distinct orderstatus) FROM (SELECT * FROM orders WHERE orderstatus = 'F')";
        assertSamePlanHash(query, query, CONNECTOR);
    }

    @Test
    public void testAssignUniqueId()
            throws Exception
    {
        String query = "SELECT name, (SELECT name FROM region WHERE regionkey = nation.regionkey) FROM nation";
        assertSamePlanHash(query, query, CONNECTOR);
    }

    @Test
    public void testEnforceSingleRow()
            throws Exception
    {
        String query = "SELECT (SELECT regionkey FROM nation WHERE name = 'nosuchvalue') AS sub";
        assertSamePlanHash(query, query, CONNECTOR);
    }

    @Test
    public void testJoin()
            throws Exception
    {
        assertSamePlanHash(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995",
                "SELECT N.name, O.totalprice, C.name FROM nation N, orders O, customer C WHERE C.nationkey = N.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995",
                CONNECTOR);
        assertSamePlanHash(
                "SELECT O.totalprice, C.name FROM orders O JOIN customer C ON C.custkey = O.custkey WHERE year(O.orderdate) = 1995",
                "SELECT O.totalprice, C.name FROM customer C JOIN orders O ON C.custkey = O.custkey WHERE year(O.orderdate) = 1995",
                CONNECTOR);
        assertSamePlanHash(
                "SELECT O.totalprice, C.name FROM orders O FULL OUTER JOIN customer C ON C.custkey = O.custkey WHERE year(O.orderdate) = 1995",
                "SELECT O.totalprice, C.name FROM customer C FULL OUTER JOIN orders O ON C.custkey = O.custkey WHERE year(O.orderdate) = 1995",
                CONNECTOR);
        assertSamePlanHash(
                "SELECT O.totalprice, C.name FROM orders O LEFT JOIN customer C ON C.custkey = O.custkey and year(O.orderdate) = 1995",
                "SELECT O.totalprice, C.name FROM customer C RIGHT JOIN orders O ON C.custkey = O.custkey and year(O.orderdate) = 1995",
                CONNECTOR);

        assertDifferentPlanHash(
                "SELECT O.totalprice, C.name FROM orders O LEFT JOIN customer C ON C.custkey = O.custkey and year(O.orderdate) = 1995",
                "SELECT O.totalprice, C.name FROM orders O RIGHT JOIN customer C ON C.custkey = O.custkey and year(O.orderdate) = 1995",
                CONNECTOR);
        assertDifferentPlanHash(
                "SELECT * FROM orders O FULL OUTER JOIN customer C ON O.custkey = C.custkey FULL OUTER JOIN nation N ON C.nationkey = N.nationkey",
                "SELECT * FROM nation N FULL OUTER JOIN customer C ON C.nationkey = N.nationkey FULL OUTER JOIN orders O ON O.custkey = C.custkey",
                CONNECTOR);
        assertDifferentPlanHash(
                "SELECT * FROM orders O LEFT OUTER JOIN customer C ON O.custkey = C.custkey LEFT OUTER JOIN nation N ON C.nationkey = N.nationkey",
                "SELECT * FROM nation N LEFT OUTER JOIN customer C ON C.nationkey = N.nationkey LEFT OUTER JOIN orders O ON O.custkey = C.custkey",
                CONNECTOR);
    }

    @Test
    public void testSemiJoin()
            throws Exception
    {
        String query = "SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = 2))";
        assertSamePlanHash(query, query, CONNECTOR);
        assertDifferentPlanHash(query, "SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = 1))", CONNECTOR);
    }

    @Test
    public void testRowNumber()
            throws Exception
    {
        String query1 = "SELECT nationkey, ROW_NUMBER() OVER (PARTITION BY regionkey) from nation";
        String query2 = "SELECT nationkey, ROW_NUMBER() OVER (PARTITION BY name) from nation";
        assertSamePlanHash(query1, query1, CONNECTOR);
        assertDifferentPlanHash(query1, query2, CONNECTOR);
    }

    @Test
    public void testLimit()
            throws Exception
    {
        assertSamePlanHash("SELECT * from nation LIMIT 1000", "SELECT * from nation LIMIT 1000", CONNECTOR);
        assertDifferentPlanHash("SELECT * from nation LIMIT 1000", "SELECT * from nation", CONNECTOR);
        assertDifferentPlanHash("SELECT * from nation LIMIT 1000", "SELECT * from nation LIMIT 10000", CONNECTOR);
        assertDifferentPlanHash("SELECT * from nation LIMIT 1000", "SELECT * from nation LIMIT 10000", REMOVE_SAFE_CONSTANTS);
    }

    @Test
    public void testTopN()
            throws Exception
    {
        String query = "SELECT orderkey FROM orders GROUP BY 1 ORDER BY 1 DESC LIMIT 1";
        assertSamePlanHash(query, query, CONNECTOR);
        assertDifferentPlanHash(query, "SELECT orderkey FROM orders GROUP BY 1 ORDER BY 1 DESC LIMIT 2", CONNECTOR);
    }

    @Test
    public void testTopNRowNumber()
            throws Exception
    {
        String query1 = "SELECT orderstatus FROM (SELECT orderstatus, row_number() OVER (PARTITION BY orderstatus ORDER BY custkey) n FROM orders) WHERE n = 1";
        String query2 = "SELECT orderstatus FROM (SELECT orderstatus, row_number() OVER (PARTITION BY orderstatus ORDER BY custkey) n FROM orders) WHERE n <= 2";
        assertSamePlanHash(query1, query1, CONNECTOR);
        assertDifferentPlanHash(query1, query2, CONNECTOR);
    }

    @Test
    public void testDistinctLimit()
            throws Exception
    {
        String query1 = "SELECT distinct regionkey from nation limit 2";
        String query2 = "SELECT distinct regionkey from nation limit 3";
        assertSamePlanHash(query1, query1, CONNECTOR);
        assertDifferentPlanHash(query1, query2, CONNECTOR);
    }

    @Test
    public void testInsert()
            throws Exception
    {
        assertSamePlanHash("INSERT INTO nation SELECT * from nation",
                "INSERT INTO nation SELECT * from nation",
                CONNECTOR);
    }

    private Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty("task_concurrency", "1")
                .build();
    }

    private void assertSamePlanHash(String sql1, String sql2, PlanCanonicalizationStrategy strategy)
            throws Exception
    {
        String hashes1 = sha256().hashString(getPlanHash(sql1, strategy), UTF_8).toString();
        String hashes2 = sha256().hashString(getPlanHash(sql2, strategy), UTF_8).toString();
        assertEquals(hashes1, hashes2);
    }

    private void assertDifferentPlanHash(String sql1, String sql2, PlanCanonicalizationStrategy strategy)
            throws Exception
    {
        String hashes1 = sha256().hashString(getPlanHash(sql1, strategy), UTF_8).toString();
        String hashes2 = sha256().hashString(getPlanHash(sql2, strategy), UTF_8).toString();
        assertNotEquals(hashes1, hashes2);
    }

    private List<PlanNode> getStatsEquivalentPlanHashes(String sql)
    {
        Session session = createSession();
        PlanNode root = plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, session).getRoot();
        assertTrue(root.getStatsEquivalentPlanNode().isPresent());

        ImmutableList.Builder<PlanNode> result = ImmutableList.builder();
        forTree(PlanNode::getSources)
                .depthFirstPreOrder(root)
                .forEach(node -> node.getStatsEquivalentPlanNode().ifPresent(result::add));
        return result.build();
    }

    private String getPlanHash(String sql, PlanCanonicalizationStrategy strategy)
            throws Exception
    {
        Session session = createSession();
        PlanNode plan = plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, session).getRoot();
        assertTrue(plan.getStatsEquivalentPlanNode().isPresent());
        return getObjectMapper().writeValueAsString(generateCanonicalPlan(plan.getStatsEquivalentPlanNode().get(), strategy, getObjectMapper()).get());
    }
}
