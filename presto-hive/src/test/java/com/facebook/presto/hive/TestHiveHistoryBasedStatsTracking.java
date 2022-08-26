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
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.statistics.InMemoryHistoryBasedPlanStatisticsProvider;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHiveHistoryBasedStatsTracking
        extends AbstractTestQueryFramework
{
    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = HiveQueryRunner.createQueryRunner(ImmutableList.of(ORDERS));

        queryRunner.installPlugin(new Plugin()
        {
            @Override
            public Iterable<HistoryBasedPlanStatisticsProvider> getHistoryBasedPlanStatisticsProviders()
            {
                return ImmutableList.of(new InMemoryHistoryBasedPlanStatisticsProvider());
            }
        });
        return queryRunner;
    }

    @Test
    public void testHistoryBasedStatsCalculator()
    {
        try {
            getQueryRunner().execute("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['ds', 'ts']) AS " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-01' as ds, '00:01' as ts FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-02' as ds, '00:02' as ts FROM orders WHERE orderkey >= 1000 AND orderkey < 2000");

            // CBO Statistics
            assertPlan(
                    "SELECT * FROM test_orders where ds = '2020-09-01' and substr(orderpriority, 1, 1) = '1'",
                    anyTree(node(TableScanNode.class)).withOutputRowCount(229.5));

            // HBO Statistics
            executeAndTrackHistory("SELECT * FROM test_orders where ds = '2020-09-01' and substr(orderpriority, 1, 1) = '1'");
            assertPlan(
                    "SELECT * FROM test_orders where ds = '2020-09-02' and substr(orderpriority, 1, 1) = '1'",
                    anyTree(node(TableScanNode.class).withOutputRowCount(48)));
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    @Test
    public void testBroadcastJoin()
    {
        try {
            getQueryRunner().execute("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['ds', 'ts']) AS " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-01' as ds, '00:01' as ts FROM orders where orderkey < 2000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-02' as ds, '00:02' as ts FROM orders WHERE orderkey >= 1000 AND orderkey < 2000");

            // CBO Statistics
            Plan plan = plan("SELECT * FROM " +
                                        "(SELECT * FROM test_orders where ds = '2020-09-01' and substr(CAST(custkey AS VARCHAR), 1, 3) <> '370') t1 JOIN " +
                                        "(SELECT * FROM test_orders where ds = '2020-09-02' and substr(CAST(custkey AS VARCHAR), 1, 3) = '370') t2 ON t1.orderkey = t2.orderkey", createSession());

            assertTrue(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof JoinNode && ((JoinNode) node).getDistributionType().get().equals(JoinNode.DistributionType.PARTITIONED))
                    .findFirst()
                    .isPresent());

            // HBO Statistics
            executeAndTrackHistory("SELECT * FROM " +
                    "(SELECT * FROM test_orders where ds = '2020-09-01' and substr(CAST(custkey AS VARCHAR), 1, 3) <> '370') t1 JOIN " +
                    "(SELECT * FROM test_orders where ds = '2020-09-02' and substr(CAST(custkey AS VARCHAR), 1, 3) = '370') t2 ON t1.orderkey = t2.orderkey");

            plan = plan("SELECT * FROM " +
                    "(SELECT * FROM test_orders where ds = '2020-09-01' and substr(CAST(custkey AS VARCHAR), 1, 3) <> '370') t1 JOIN " +
                    "(SELECT * FROM test_orders where ds = '2020-09-02' and substr(CAST(custkey AS VARCHAR), 1, 3) = '370') t2 ON t1.orderkey = t2.orderkey", createSession());

            assertTrue(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof JoinNode && ((JoinNode) node).getDistributionType().get().equals(JoinNode.DistributionType.REPLICATED))
                    .findFirst()
                    .isPresent());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    @Override
    protected void assertPlan(@Language("SQL") String query, PlanMatchPattern pattern)
    {
        assertPlan(createSession(), query, pattern);
    }

    private void executeAndTrackHistory(String sql)
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        SqlQueryManager sqlQueryManager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        InMemoryHistoryBasedPlanStatisticsProvider provider = (InMemoryHistoryBasedPlanStatisticsProvider) sqlQueryManager.getHistoryBasedPlanStatisticsTracker().getHistoryBasedPlanStatisticsProvider();

        queryRunner.execute(createSession(), sql);
        provider.waitProcessQueryEvents();
    }

    private Session createSession()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(TRACK_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "automatic")
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
    }
}
