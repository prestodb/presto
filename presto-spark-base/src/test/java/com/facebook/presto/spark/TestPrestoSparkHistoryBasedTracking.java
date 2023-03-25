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
package com.facebook.presto.spark;

import com.facebook.presto.Session;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.statistics.InMemoryHistoryBasedPlanStatisticsProvider;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.spark.PrestoSparkQueryRunner.createHivePrestoSparkQueryRunner;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static io.airlift.tpch.TpchTable.NATION;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPrestoSparkHistoryBasedTracking
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        PrestoSparkQueryRunner queryRunner = createHivePrestoSparkQueryRunner(ImmutableList.of(NATION, ORDERS));
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
    public void testTracking()
    {
        // CBO Statistics
        assertPlan(
                getSession(),
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(TableScanNode.class).withOutputRowCount(22.5)));

        // HBO Statistics
        executeAndTrackHistory("SELECT * FROM nation where substr(name, 1, 1) = 'A'");
        assertPlan(
                getSession(),
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(TableScanNode.class).withOutputRowCount(2)));
        assertPlan(
                getSession(),
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                node(OutputNode.class, any()).withOutputRowCount(2));
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
                    "(SELECT * FROM test_orders where ds = '2020-09-02' and substr(CAST(custkey AS VARCHAR), 1, 3) = '370') t2 ON t1.orderkey = t2.orderkey", getSession());

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
                    "(SELECT * FROM test_orders where ds = '2020-09-02' and substr(CAST(custkey AS VARCHAR), 1, 3) = '370') t2 ON t1.orderkey = t2.orderkey", getSession());

            assertTrue(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof JoinNode && ((JoinNode) node).getDistributionType().get().equals(JoinNode.DistributionType.REPLICATED))
                    .findFirst()
                    .isPresent());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    private void executeAndTrackHistory(String sql)
    {
        getQueryRunner().execute(getSession(), sql);
        getHistoryProvider().waitProcessQueryEvents();
    }

    private InMemoryHistoryBasedPlanStatisticsProvider getHistoryProvider()
    {
        PrestoSparkQueryRunner queryRunner = (PrestoSparkQueryRunner) getQueryRunner();
        return (InMemoryHistoryBasedPlanStatisticsProvider) queryRunner.getHistoryBasedPlanStatisticsManager().getHistoryBasedPlanStatisticsTracker().getHistoryBasedPlanStatisticsProvider();
    }

    @Override
    protected Session getSession()
    {
        return Session.builder(super.getSession())
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(TRACK_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setCatalogSessionProperty("hive", "pushdown_filter_enabled", "true")
                .setSystemProperty("task_concurrency", "1")
                .build();
    }
}
