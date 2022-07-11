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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.USE_EXTERNAL_PLAN_STATISTICS;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.CONNECTOR;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.REMOVE_SAFE_CONSTANTS;
import static com.facebook.presto.sql.planner.CanonicalPlanGenerator.generateCanonicalPlan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
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

    private Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty(USE_EXTERNAL_PLAN_STATISTICS, "true")
                .setSystemProperty("task_concurrency", "1")
                .build();
    }

    private void assertSamePlanHash(String sql1, String sql2, PlanCanonicalizationStrategy strategy)
            throws Exception
    {
        String hashes1 = getPlanHash(sql1, strategy);
        String hashes2 = getPlanHash(sql2, strategy);
        assertEquals(hashes1, hashes2);
    }

    private void assertDifferentPlanHash(String sql1, String sql2, PlanCanonicalizationStrategy strategy)
            throws Exception
    {
        String hashes1 = getPlanHash(sql1, strategy);
        String hashes2 = getPlanHash(sql2, strategy);
        assertNotEquals(hashes1, hashes2);
    }

    private String getPlanHash(String sql, PlanCanonicalizationStrategy strategy)
            throws Exception
    {
        Session session = createSession();
        PlanNode plan = plan(sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, session).getRoot();
        assertTrue(plan.getStatsEquivalentPlanNode().isPresent());
        return getObjectMapper().writeValueAsString(generateCanonicalPlan(plan.getStatsEquivalentPlanNode().get(), strategy).get());
    }
}
