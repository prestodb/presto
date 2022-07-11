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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.common.plan.PlanCanonicalizationStrategy;
import com.facebook.presto.cost.HistoryBasedPlanStatisticsCalculator;
import com.facebook.presto.sql.planner.CanonicalPlanHashes;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.USE_EXTERNAL_PLAN_STATISTICS;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.CONNECTOR;
import static com.facebook.presto.common.plan.PlanCanonicalizationStrategy.REMOVE_SAFE_CONSTANTS;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestHiveCanonicalPlanHashes
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(ImmutableList.of(ORDERS, LINE_ITEM));
    }

    @Test
    public void testCanonicalizationStrategies()
    {
        QueryRunner queryRunner = getQueryRunner();

        try {
            queryRunner.execute("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['ds', 'ts']) AS " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-01' as ds, '00:01' as ts FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-02' as ds, '00:02' as ts FROM orders WHERE orderkey < 1000");

            assertSamePlanHashes(
                    "SELECT orderkey from test_orders",
                    "SELECT orderkey from test_orders",
                    CONNECTOR);
            assertSamePlanHashes(
                    "SELECT orderkey from test_orders where ds > '2020-09-01'",
                    "SELECT orderkey from test_orders where ds = '2020-09-02'",
                    CONNECTOR);
            assertSamePlanHashes(
                    "SELECT orderkey from test_orders where ds = '2020-09-01' AND orderkey < 10 AND ts >= '00:01'",
                    "SELECT orderkey from test_orders where ds = '2020-09-02' AND orderkey < 10 AND ts >= '00:02'",
                    CONNECTOR);

            assertDifferentPlanHashes(
                    "SELECT orderkey from test_orders where ds = '2020-09-01' AND orderkey < 10",
                    "SELECT orderkey from test_orders where ds = '2020-09-02' AND orderkey < 20",
                    CONNECTOR);

            assertSamePlanHashes(
                    "SELECT orderkey, CAST(1 AS VARCHAR) from test_orders where ds = '2020-09-01' AND orderkey < 10",
                    "SELECT orderkey, CAST(2 AS VARCHAR) from test_orders where ds = '2020-09-02' AND orderkey < 10",
                    REMOVE_SAFE_CONSTANTS);
            assertDifferentPlanHashes(
                    "SELECT orderkey, CAST(1 AS VARCHAR) from test_orders where ds = '2020-09-01' AND orderkey < 10",
                    "SELECT orderkey, CAST(1 AS VARCHAR) from test_orders where ds = '2020-09-02' AND orderkey < 20",
                    REMOVE_SAFE_CONSTANTS);
        }
        finally {
            queryRunner.execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    private void assertSamePlanHashes(String sql1, String sql2, PlanCanonicalizationStrategy strategy)
    {
        List<String> hashes1 = getPlanHashes(sql1, strategy);
        List<String> hashes2 = getPlanHashes(sql2, strategy);
        assertEquals(hashes1, hashes2);
    }

    private void assertDifferentPlanHashes(String sql1, String sql2, PlanCanonicalizationStrategy strategy)
    {
        List<String> hashes1 = getPlanHashes(sql1, strategy);
        List<String> hashes2 = getPlanHashes(sql2, strategy);
        assertNotEquals(hashes1, hashes2);
    }

    private List<String> getPlanHashes(String sql, PlanCanonicalizationStrategy strategy)
    {
        Session session = pushdownFilterEnabled();
        plan(sql, session);

        assertTrue(getQueryRunner().getStatsCalculator() instanceof HistoryBasedPlanStatisticsCalculator);
        HistoryBasedPlanStatisticsCalculator statsCalculator = (HistoryBasedPlanStatisticsCalculator) getQueryRunner().getStatsCalculator();
        CanonicalPlanHashes canonicalPlanHashes = statsCalculator.getCanonicalPlanHashes(session.getQueryId()).get();

        return canonicalPlanHashes.getAllHashes(strategy);
    }

    private Session pushdownFilterEnabled()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_EXTERNAL_PLAN_STATISTICS, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
    }
}
