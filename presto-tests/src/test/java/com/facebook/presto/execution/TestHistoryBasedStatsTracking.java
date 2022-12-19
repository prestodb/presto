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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.statistics.InMemoryHistoryBasedPlanStatisticsProvider;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.isNaN;

@Test(singleThreaded = true)
public class TestHistoryBasedStatsTracking
        extends AbstractTestQueryFramework
{
    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = new DistributedQueryRunner(createSession(), 1);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of("tpch.splits-per-node", "3"));

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

    @Test
    public void testHistoryBasedStatsCalculator()
    {
        // CBO Statistics
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(FilterNode.class, any()).withOutputRowCount(Double.NaN)));

        // HBO Statistics
        executeAndTrackHistory("SELECT * FROM nation where substr(name, 1, 1) = 'A'");
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(FilterNode.class, any()).withOutputRowCount(2).withOutputSize(199)));

        // CBO Statistics
        assertPlan(
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(ProjectNode.class, node(FilterNode.class, any())).withOutputRowCount(12.5)));
        assertPlan(
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(AggregationNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(Double.NaN)));

        // HBO Statistics
        executeAndTrackHistory("SELECT max(nationkey) FROM nation where name < 'D' group by regionkey");
        assertPlan(
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(ProjectNode.class, node(FilterNode.class, any())).withOutputRowCount(5).withOutputSize(90)));

        assertPlan(
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(AggregationNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(3).withOutputSize(54)));
    }

    @Test
    public void testUnion()
    {
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) = 'A' UNION ALL SELECT * FROM nation where substr(name, 1, 1) = 'B'",
                anyTree(node(ExchangeNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(Double.NaN)));

        executeAndTrackHistory("SELECT * FROM nation where substr(name, 1, 1) = 'A' UNION ALL SELECT * FROM nation where substr(name, 1, 1) = 'B'");
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) = 'B' UNION ALL SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(ExchangeNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(3)));

        // INTERSECT, EXCEPT are implemented as UNION
        executeAndTrackHistory("SELECT * FROM nation where substr(name, 1, 1) >= 'B' and substr(name, 1, 1) <= 'E' INTERSECT SELECT * FROM nation where substr(name, 1, 1) <= 'B'");
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) >= 'B' and substr(name, 1, 1) <= 'E' INTERSECT SELECT * FROM nation where substr(name, 1, 1) <= 'B'",
                anyTree(node(ProjectNode.class, anyTree(anyTree(any()), anyTree(any()))).withOutputRowCount(1)));

        executeAndTrackHistory("SELECT * FROM nation where substr(name, 1, 1) >= 'B' and substr(name, 1, 1) <= 'E' EXCEPT SELECT * FROM nation where substr(name, 1, 1) <= 'B'");
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) >= 'B' and substr(name, 1, 1) <= 'E' EXCEPT SELECT * FROM nation where substr(name, 1, 1) <= 'B'",
                anyTree(node(ProjectNode.class, node(FilterNode.class, anyTree(anyTree(any()), anyTree(any())))).withOutputRowCount(4)));
    }

    @Test
    public void testWindow()
    {
        String query1 = "SELECT orderkey, SUM(custkey) OVER (PARTITION BY orderstatus ORDER BY totalprice) FROM orders where orderkey < 1000";
        String query2 = "SELECT orderkey, SUM(custkey) OVER (PARTITION BY orderstatus ORDER BY totalprice) FROM orders where orderkey < 2000";

        assertPlan(query1 + " UNION ALL " + query2, anyTree(node(WindowNode.class, anyTree(any())).withOutputRowCount(Double.NaN)));
        executeAndTrackHistory(query1 + " UNION ALL " + query2);
        assertPlan(
                query2 + " UNION ALL " + query1,
                anyTree(node(ExchangeNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(758)));
    }

    @Test
    public void testValues()
    {
        String query = "SELECT * FROM (SELECT * FROM ( VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, name)) T1 JOIN (SELECT nationkey FROM nation WHERE substr(name, 1, 1) = 'A') T2 ON T2.nationkey = T1.id";

        assertPlan(query, anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(Double.NaN)));
        executeAndTrackHistory(query);
        assertPlan(query, anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(1)));
    }

    @Test
    public void testSort()
    {
        String query = "SELECT * FROM nation where substr(name, 1, 1) = 'A' ORDER BY regionkey";

        assertPlan(query, anyTree(node(SortNode.class, anyTree(any())).withOutputRowCount(Double.NaN)));
        executeAndTrackHistory(query);
        assertPlan(query, anyTree(node(SortNode.class, anyTree(any())).withOutputRowCount(2)));
    }

    @Test
    public void testMarkDistinct()
    {
        String query = "SELECT count(*), count(distinct orderstatus) FROM (SELECT * FROM orders WHERE orderstatus = 'F')";

        assertPlan(query, anyTree(node(MarkDistinctNode.class, anyTree(any())).withOutputRowCount(Double.NaN)));
        executeAndTrackHistory(query);
        assertPlan(query, anyTree(node(MarkDistinctNode.class, anyTree(any())).withOutputRowCount(7304)));
    }

    @Test
    public void testAssignUniqueId()
    {
        String query = "SELECT name, (SELECT name FROM region WHERE regionkey = nation.regionkey) FROM nation";

        // Stats of AssignUniqueId can be generated from CBO, so test for OutputNode stats which include the whole plan hash
        assertPlan(query, node(OutputNode.class, anyTree(anyTree(any()), anyTree(any()))).withOutputRowCount(Double.NaN));
        executeAndTrackHistory(query);
        assertPlan(query, node(OutputNode.class, anyTree(anyTree(any()), anyTree(any()))).withOutputRowCount(25));
    }

    @Test
    public void testSemiJoin()
    {
        String query = "SELECT quantity FROM (SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderkey = 2))";

        assertPlan(query, anyTree(node(SemiJoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(Double.longBitsToDouble(4616202753553671564L))));
        executeAndTrackHistory(query);
        assertPlan(query, anyTree(node(SemiJoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(1)));
    }

    @Test
    public void testRowNumber()
    {
        String query = "SELECT nationkey, ROW_NUMBER() OVER (PARTITION BY regionkey) from nation where substr(name, 1, 1) = 'A'";

        assertPlan(query, anyTree(node(RowNumberNode.class, anyTree(any())).withOutputRowCount(Double.NaN)));
        executeAndTrackHistory(query);
        assertPlan(query, anyTree(node(RowNumberNode.class, anyTree(any())).withOutputRowCount(2)));
    }

    @Test
    public void testUnionMultiple()
    {
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) = 'A' UNION ALL " +
                        "SELECT * FROM nation where substr(name, 1, 1) = 'B' UNION ALL " +
                        "SELECT * FROM nation where substr(name, 1, 1) = 'C'",
                anyTree(node(ExchangeNode.class, anyTree(any()), anyTree(any()), anyTree(any())).withOutputRowCount(Double.NaN)));

        executeAndTrackHistory("SELECT * FROM nation where substr(name, 1, 1) = 'A' UNION ALL " +
                "SELECT * FROM nation where substr(name, 1, 1) = 'B' UNION ALL " +
                "SELECT * FROM nation where substr(name, 1, 1) = 'C'");
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) = 'B' UNION ALL " +
                        "SELECT * FROM nation where substr(name, 1, 1) = 'C' UNION ALL " +
                        "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(ExchangeNode.class, anyTree(any()), anyTree(any()), anyTree(any())).withOutputRowCount(5)));

        assertPlan(
                "SELECT nationkey FROM nation where substr(name, 1, 1) = 'A' UNION ALL SELECT nationkey FROM customer where nationkey < 10",
                anyTree(node(ExchangeNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(Double.NaN)));

        executeAndTrackHistory("SELECT nationkey FROM nation where substr(name, 1, 1) = 'A' UNION ALL SELECT nationkey FROM customer  where nationkey < 10");
        assertPlan(
                " SELECT nationkey FROM customer where nationkey < 10 UNION ALL SELECT nationkey FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(ExchangeNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(601)));
    }

    @Test
    public void testJoin()
    {
        assertPlan(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'",
                anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(Double.NaN)));
        assertPlan(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'",
                anyTree(node(JoinNode.class, anyTree(anyTree(any()), anyTree(any())), anyTree(any())).withOutputRowCount(Double.NaN)));

        executeAndTrackHistory("SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'");
        assertPlan(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'",
                anyTree(node(JoinNode.class, anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(2204)), anyTree(any()))));
        assertPlan(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'",
                anyTree(node(JoinNode.class, anyTree(anyTree(any()), anyTree(any())), anyTree(any())).withOutputRowCount(1915)));
        // Check that output size doesn't include hash variables
        assertPlan(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'",
                anyTree(anyTree(anyTree(any()), anyTree(any())), anyTree(node(ProjectNode.class, anyTree(any())).withOutputRowCount(22).withOutputSize(661 - 22 * 8))));
    }

    @Test
    public void testLimit()
    {
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(FilterNode.class, any()).withOutputRowCount(Double.NaN)));

        // Limit nodes may cause workers to break early and not log statistics.
        // This happens in some runs, so running it a few times should make it likely that some run succeeded in storing
        // stats.
        for (int i = 0; i < 10; ++i) {
            executeAndTrackHistory("SELECT * FROM nation where substr(name, 1, 1) = 'A' LIMIT 1");
        }
        // Don't track stats of filter node when limit is not present
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(FilterNode.class, any()).withOutputRowCount(Double.NaN)));
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) = 'A' LIMIT 1",
                anyTree(node(FilterNode.class, any()).with(validOutputRowCountMatcher())));
        assertPlan(
                "SELECT * FROM nation where substr(name, 1, 1) = 'A' LIMIT 1",
                anyTree(node(LimitNode.class, anyTree(any())).withOutputRowCount(1)));
    }

    private void executeAndTrackHistory(String sql)
    {
        getQueryRunner().execute(sql);
        getHistoryProvider().waitProcessQueryEvents();
    }

    private InMemoryHistoryBasedPlanStatisticsProvider getHistoryProvider()
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        SqlQueryManager sqlQueryManager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        return (InMemoryHistoryBasedPlanStatisticsProvider) sqlQueryManager.getHistoryBasedPlanStatisticsTracker().getHistoryBasedPlanStatisticsProvider();
    }

    private static Matcher validOutputRowCountMatcher()
    {
        return new Matcher()
        {
            @Override
            public boolean shapeMatches(PlanNode node)
            {
                return true;
            }

            @Override
            public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                return new MatchResult(!isNaN(stats.getStats(node).getOutputRowCount()));
            }
        };
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(TRACK_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty("task_concurrency", "1")
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
    }
}
