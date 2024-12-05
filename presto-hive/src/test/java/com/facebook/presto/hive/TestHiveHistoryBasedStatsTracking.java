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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.testing.InMemoryHistoryBasedPlanStatisticsProvider;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.CTE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.CTE_PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.SystemSessionProperties.HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.HISTORY_INPUT_TABLE_STATISTICS_MATCHING_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.PARTIAL_AGGREGATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY;
import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_PARTIAL_AGGREGATION_HISTORY;
import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;
import static com.facebook.presto.hive.HiveSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static io.airlift.tpch.TpchTable.ORDERS;
import static org.testng.Assert.assertFalse;
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

    @BeforeMethod(alwaysRun = true)
    public void setUp()
    {
        getHistoryProvider().clearCache();
    }

    private InMemoryHistoryBasedPlanStatisticsProvider getHistoryProvider()
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        SqlQueryManager sqlQueryManager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        return (InMemoryHistoryBasedPlanStatisticsProvider) sqlQueryManager.getHistoryBasedPlanStatisticsTracker().getHistoryBasedPlanStatisticsProvider();
    }

    @Test
    public void testHistoryMatchingThreshold()
    {
        String tableName = "test_history_matching_threshold";
        try {
            assertUpdate("create table " + tableName + "(a int, b varchar)");
            String query = "select * from " + tableName + " where a <= 2 order by b desc";

            // insert 12 rows into table `test_history_matching_threshold`
            executeAndTrackHistory("insert into " + tableName + " values(1, '1001'), (2, '1002'), (2, '1003'), (2, '1002'), (2, '1003'), (2, '1002'), (2, '1003'), (2, '1002'), (2, '1003'), (2, '1002'), (2, '1003'), (3, '1004')", defaultSession());

            // cost based statistics
            assertPlan(query, anyTree(node(TableScanNode.class).withOutputRowCount(false, "CBO")));
            assertPlan(query, node(OutputNode.class, anyTree(any())).withOutputRowCount(false, "CBO"));

            executeAndTrackHistory(query, defaultSession());

            // historical based statistics
            assertPlan(query, node(OutputNode.class, anyTree(any())).withOutputRowCount(11, "HBO"));

            // insert 1 more row into table `test_history_matching_threshold`,
            //  so that the change of input table size is less than `0.1`
            executeAndTrackHistory("insert into " + tableName + " values(1, '1005')", defaultSession());

            // default history threshold is `0.1`, so the old historical statistics could still be used
            assertPlan(query, node(OutputNode.class, anyTree(any())).withOutputRowCount(11, "HBO"));

            // set history threshold more than the ratio of real data changes explicitly, the old historical statistics could still be used
            assertPlan(createSessionWithHistoryThreshold(0.1), query, node(OutputNode.class, anyTree(any())).withOutputRowCount(11, "HBO"));
            assertPlan(createSessionWithHistoryThreshold(0.2), query, node(OutputNode.class, anyTree(any())).withOutputRowCount(11, "HBO"));

            // set history threshold less than the ratio of real data changes explicitly,
            //  then the historical statistics couldn't be used anymore
            assertPlan(createSessionWithHistoryThreshold(0.01), query, node(OutputNode.class, anyTree(any())).withOutputRowCount(false, "CBO"));
            assertPlan(createSessionWithHistoryThreshold(0.0), query, node(OutputNode.class, anyTree(any())).withOutputRowCount(false, "CBO"));

            // insert another 1 more row into table `test_history_matching_threshold`,
            //  so that the change of input table size is bigger than `0.1` and less than `0.2`
            executeAndTrackHistory("insert into " + tableName + " values(2, '1006')", defaultSession());

            // default history threshold is `0.1`, so the historical statistics could not be used anymore
            assertPlan(query, node(OutputNode.class, anyTree(any())).withOutputRowCount(false, "CBO"));

            // set history threshold less than the ratio of real data changes explicitly,
            //  then the historical statistics couldn't be used anymore
            assertPlan(createSessionWithHistoryThreshold(0.12), query, node(OutputNode.class, anyTree(any())).withOutputRowCount(false, "CBO"));
            assertPlan(createSessionWithHistoryThreshold(0.01), query, node(OutputNode.class, anyTree(any())).withOutputRowCount(false, "CBO"));
            assertPlan(createSessionWithHistoryThreshold(0.0), query, node(OutputNode.class, anyTree(any())).withOutputRowCount(false, "CBO"));

            // set history threshold more than the ratio of real data changes explicitly, the old historical statistics could still be used
            assertPlan(createSessionWithHistoryThreshold(0.2), query, node(OutputNode.class, anyTree(any())).withOutputRowCount(11, "HBO"));

            executeAndTrackHistory(query, defaultSession());

            assertPlan(query, node(OutputNode.class, anyTree(any())).withOutputRowCount(13, "HBO"));
            assertPlan(createSessionWithHistoryThreshold(0.0), query, node(OutputNode.class, anyTree(any())).withOutputRowCount(13, "HBO"));
            assertPlan(createSessionWithHistoryThreshold(0.15), query, node(OutputNode.class, anyTree(any())).withOutputRowCount(13, "HBO"));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    private Session createSessionWithHistoryThreshold(double threshold)
    {
        return Session.builder(defaultSession())
                .setSystemProperty(HISTORY_INPUT_TABLE_STATISTICS_MATCHING_THRESHOLD, String.valueOf(threshold))
                .build();
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
                    "SELECT *, 1 FROM test_orders where ds = '2020-09-01' and substr(orderpriority, 1, 1) = '1'",
                    anyTree(node(ProjectNode.class, any())).withOutputRowCount(229.5));

            // HBO Statistics
            executeAndTrackHistory("SELECT *, 1 FROM test_orders where ds = '2020-09-01' and substr(orderpriority, 1, 1) = '1'", defaultSession());
            assertPlan(
                    "SELECT *, 2 FROM test_orders where ds = '2020-09-02' and substr(orderpriority, 1, 1) = '1'",
                    anyTree(node(ProjectNode.class, any()).withOutputRowCount(48)));
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    @Test
    public void testHistoryBasedStatsCalculatorMultipleStrategies()
    {
        try {
            getQueryRunner().execute("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['ds', 'ts']) AS " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-01' as ds, '00:01' as ts FROM orders WHERE orderkey < 1000 " +
                    "UNION ALL " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-02' as ds, '00:02' as ts FROM orders WHERE orderkey >= 1000 AND orderkey < 2000");

            // CBO Statistics
            assertPlan(
                    "SELECT *, 1 FROM test_orders where ds = '2020-09-01' and orderpriority = '1-URGENT'",
                    anyTree(node(ProjectNode.class, any())).withOutputRowCount(51.0));

            // HBO Statistics
            executeAndTrackHistory("SELECT *, 1 FROM test_orders where ds = '2020-09-01' and orderpriority = '1-URGENT'",
                    createSession(ImmutableMap.of(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, "IGNORE_SAFE_CONSTANTS,IGNORE_SCAN_CONSTANTS")));
            assertPlan(createSession(ImmutableMap.of(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, "IGNORE_SAFE_CONSTANTS,IGNORE_SCAN_CONSTANTS")),
                    "SELECT *, 2 FROM test_orders where ds = '2020-09-02' and orderpriority = '1-URGENT'",
                    anyTree(node(ProjectNode.class, any()).withOutputRowCount(48)));
            assertPlan(createSession(ImmutableMap.of(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, "IGNORE_SAFE_CONSTANTS,IGNORE_SCAN_CONSTANTS")),
                    "SELECT *, 2 FROM test_orders where ds = '2020-09-02' and orderpriority = '2-HIGH'",
                    anyTree(node(ProjectNode.class, any()).withOutputRowCount(48)));
            assertPlan(createSession(ImmutableMap.of(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, "IGNORE_SAFE_CONSTANTS")),
                    "SELECT *, 2 FROM test_orders where ds = '2020-09-02' and orderpriority = '2-HIGH'",
                    anyTree(node(ProjectNode.class, any()).withOutputRowCount(49.6)));
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    @Test
    public void testInsertTable()
    {
        try {
            getQueryRunner().execute("CREATE TABLE test_orders (orderkey integer, ds varchar) WITH (partitioned_by = ARRAY['ds'])");

            Plan plan = plan("insert into test_orders (values (1, '2023-09-20'), (2, '2023-09-21'))", defaultSession());

            assertTrue(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof TableWriterMergeNode && !node.getStatsEquivalentPlanNode().isPresent())
                    .findFirst()
                    .isPresent());

            assertFalse(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof TableWriterMergeNode && node.getStatsEquivalentPlanNode().isPresent())
                    .findFirst()
                    .isPresent());
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
                    "(SELECT * FROM test_orders where ds = '2020-09-02' and substr(CAST(custkey AS VARCHAR), 1, 3) = '370') t2 ON t1.orderkey = t2.orderkey", defaultSession());

            assertTrue(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof JoinNode && ((JoinNode) node).getDistributionType().get().equals(JoinDistributionType.PARTITIONED))
                    .findFirst()
                    .isPresent());

            // HBO Statistics
            executeAndTrackHistory("SELECT * FROM " +
                            "(SELECT * FROM test_orders where ds = '2020-09-01' and substr(CAST(custkey AS VARCHAR), 1, 3) <> '370') t1 JOIN " +
                            "(SELECT * FROM test_orders where ds = '2020-09-02' and substr(CAST(custkey AS VARCHAR), 1, 3) = '370') t2 ON t1.orderkey = t2.orderkey",
                    defaultSession());

            plan = plan("SELECT * FROM " +
                    "(SELECT * FROM test_orders where ds = '2020-09-01' and substr(CAST(custkey AS VARCHAR), 1, 3) <> '370') t1 JOIN " +
                    "(SELECT * FROM test_orders where ds = '2020-09-02' and substr(CAST(custkey AS VARCHAR), 1, 3) = '370') t2 ON t1.orderkey = t2.orderkey", defaultSession());

            assertTrue(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof JoinNode && ((JoinNode) node).getDistributionType().get().equals(JoinDistributionType.REPLICATED))
                    .findFirst()
                    .isPresent());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    @Test
    public void testPartialAggStatistics()
    {
        try {
            // CBO Statistics
            getQueryRunner().execute("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['ds', 'ts']) AS " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-01' as ds, '00:01' as ts FROM orders where orderkey < 2000 ");

            String query = "SELECT count(*) FROM test_orders group by custkey";
            Session session = createSession(ImmutableMap.of(PARTIAL_AGGREGATION_STRATEGY, "always"));
            Plan plan = plan(query, session);

            assertTrue(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof AggregationNode && ((AggregationNode) node).getStep() == AggregationNode.Step.PARTIAL)
                    .findFirst()
                    .isPresent());

            // collect HBO Statistics
            executeAndTrackHistory(query, createSession(ImmutableMap.of(PARTIAL_AGGREGATION_STRATEGY, "always")));

            plan = plan(query, createSession(ImmutableMap.of(PARTIAL_AGGREGATION_STRATEGY, "automatic")));

            assertTrue(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof AggregationNode && ((AggregationNode) node).getStep() == AggregationNode.Step.PARTIAL).findAll().isEmpty());
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    @Test
    public void testPartialAggStatisticsGroupByPartKey()
    {
        try {
            // CBO Statistics
            getQueryRunner().execute("CREATE TABLE test_orders WITH (partitioned_by = ARRAY['ds']) AS " +
                    "SELECT orderkey, orderpriority, comment, custkey, '2020-09-01' as ds FROM orders where orderkey < 2000 ");

            // collect HBO Statistics
            String queryGBPartitionKey = "SELECT ds FROM test_orders group by ds";

            Plan plan = plan(queryGBPartitionKey, createSession(ImmutableMap.of(PARTIAL_AGGREGATION_STRATEGY, "always")));

            assertTrue(PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(node -> node instanceof AggregationNode && ((AggregationNode) node).getStep() == AggregationNode.Step.PARTIAL).findFirst().isPresent());
            executeAndTrackHistory(queryGBPartitionKey, createSession(ImmutableMap.of(PARTIAL_AGGREGATION_STRATEGY, "always")));
        }
        finally {
            getQueryRunner().execute("DROP TABLE IF EXISTS test_orders");
        }
    }

    @Test
    public void testHistoryBasedStatsCalculatorCTE()
    {
        String sql = "with t1 as (select orderkey, orderstatus from orders where totalprice > 100), t2 as (select orderkey, totalprice from orders where custkey > 100) " +
                "select orderstatus, sum(totalprice) from t1 join t2 on t1.orderkey=t2.orderkey group by orderstatus";
        Session cteMaterialization = Session.builder(defaultSession())
                .setSystemProperty(CTE_MATERIALIZATION_STRATEGY, "ALL")
                .setSystemProperty(CTE_PARTITIONING_PROVIDER_CATALOG, "hive")
                .build();
        // CBO Statistics
        assertPlan(cteMaterialization, sql, anyTree(node(ProjectNode.class, anyTree(any())).withOutputRowCount(Double.NaN)));

        // HBO Statistics
        executeAndTrackHistory(sql, cteMaterialization);
        assertPlan(cteMaterialization, sql, anyTree(node(ProjectNode.class, anyTree(any())).withOutputRowCount(3)));
    }

    @Test
    public void testHistoryBasedStatsWithSpecifiedCanonicalizationStrategy()
    {
        getQueryRunner().execute("CREATE TABLE test_myt(a int, b varchar)");
        getQueryRunner().execute("INSERT INTO test_myt values(1, '1001'), (2, '1002'), (3, '1003'), (4, '1004')");

        String query = "SELECT * FROM test_myt where a-1 < 3 ORDER BY b";
        Session session = Session.builder(defaultSession())
                .setSystemProperty(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, "IGNORE_SAFE_CONSTANTS")
                .build();

        // get cost base stats before completing any query
        assertPlan(session, query, node(OutputNode.class, anyTree(any())).withOutputRowCount(false, "CBO"));
        executeAndTrackHistory(query, session);

        // get history base stats after completing a query with the same canonicalization strategy
        assertPlan(session, query, node(OutputNode.class, anyTree(any())).withOutputRowCount(3, "HBO"));

        Session sessionWithAnotherStrategy = Session.builder(defaultSession())
                .setSystemProperty(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, "IGNORE_SCAN_CONSTANTS")
                .build();

        // could not get history base stats when using a different canonicalization strategy from the one that used to collect the stats
        assertPlan(sessionWithAnotherStrategy, query, node(OutputNode.class, anyTree(any())).withOutputRowCount(false, "CBO"));

        Session sessionWithMultiStrategy = Session.builder(defaultSession())
                .setSystemProperty(HISTORY_BASED_OPTIMIZATION_PLAN_CANONICALIZATION_STRATEGY, "DEFAULT,CONNECTOR,IGNORE_SCAN_CONSTANTS,IGNORE_SAFE_CONSTANTS")
                .build();

        // get history base stats when using multiple canonicalization strategies that contains the one used to collect the stats
        assertPlan(sessionWithMultiStrategy, query, node(OutputNode.class, anyTree(any())).withOutputRowCount(3, "HBO"));
    }

    @Override
    protected void assertPlan(@Language("SQL") String query, PlanMatchPattern pattern)
    {
        assertPlan(defaultSession(), query, pattern);
    }

    private void executeAndTrackHistory(String sql, Session session)
    {
        getQueryRunner().execute(session, sql);
        getHistoryProvider().waitProcessQueryEvents();
    }

    private Session defaultSession()
    {
        return createSession(ImmutableMap.of(PARTIAL_AGGREGATION_STRATEGY, "automatic"));
    }

    private Session createSession(Map<String, String> properties)
    {
        Session.SessionBuilder builder = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(TRACK_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "automatic")
                .setSystemProperty(USE_PARTIAL_AGGREGATION_HISTORY, "true")
                .setCatalogSessionProperty(HIVE_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .setSystemProperty(RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY, "false");
        properties.forEach((property, value) -> builder.setSystemProperty(property, value));
        return builder.build();
    }
}
