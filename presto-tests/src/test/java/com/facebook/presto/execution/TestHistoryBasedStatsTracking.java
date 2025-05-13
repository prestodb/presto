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
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.statistics.CostBasedSourceInfo;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.testing.InMemoryHistoryBasedPlanStatisticsProvider;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.CONFIDENCE_BASED_BROADCAST_ENABLED;
import static com.facebook.presto.SystemSessionProperties.ENFORCE_HISTORY_BASED_OPTIMIZER_REGISTRATION_TIMEOUT;
import static com.facebook.presto.SystemSessionProperties.HISTORY_BASED_OPTIMIZER_TIMEOUT_LIMIT;
import static com.facebook.presto.SystemSessionProperties.HISTORY_CANONICAL_PLAN_NODE_LIMIT;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY;
import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_STATS_FROM_FAILED_QUERIES;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_PERFECTLY_CONSISTENT_HISTORIES;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.FACT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static org.testng.Assert.assertFalse;

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
    public void testHistoryBasedStatsCalculatorEnforceTimeOut()
    {
        Session sessionWithDefaultTimeoutLimit = Session.builder(createSession())
                .setSystemProperty(ENFORCE_HISTORY_BASED_OPTIMIZER_REGISTRATION_TIMEOUT, "true")
                .build();
        Session sessionWithZeroTimeoutLimit = Session.builder(createSession())
                .setSystemProperty(ENFORCE_HISTORY_BASED_OPTIMIZER_REGISTRATION_TIMEOUT, "true")
                .setSystemProperty(HISTORY_BASED_OPTIMIZER_TIMEOUT_LIMIT, "0ms")
                .build();
        // CBO Statistics
        assertPlan(
                sessionWithDefaultTimeoutLimit,
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(FilterNode.class, any()).withOutputRowCount(Double.NaN)));

        // Write HBO statistics failed as we set timeout limit to be 0
        executeAndNoHistoryWritten("SELECT * FROM nation where substr(name, 1, 1) = 'A'", sessionWithZeroTimeoutLimit);
        // No HBO statistics read
        assertPlan(
                sessionWithDefaultTimeoutLimit,
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(FilterNode.class, any()).withOutputRowCount(Double.NaN)));

        // Write HBO Statistics is successful, as we use the default 10 seconds timeout limit
        executeAndTrackHistory("SELECT * FROM nation where substr(name, 1, 1) = 'A'", sessionWithDefaultTimeoutLimit);
        // Read HBO statistics successfully with default timeout
        assertPlan(
                sessionWithDefaultTimeoutLimit,
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(FilterNode.class, any()).withOutputRowCount(2).withOutputSize(199)));
        // Read HBO statistics fail due to timeout
        assertPlan(
                sessionWithZeroTimeoutLimit,
                "SELECT * FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(FilterNode.class, any()).withOutputRowCount(Double.NaN)));

        // CBO Statistics
        assertPlan(
                sessionWithDefaultTimeoutLimit,
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(ProjectNode.class, node(FilterNode.class, any())).withOutputRowCount(12.5)));
        assertPlan(
                sessionWithDefaultTimeoutLimit,
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(AggregationNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(Double.NaN)));

        // Write HBO statistics failed as we set timeout limit to be 0
        executeAndNoHistoryWritten("SELECT max(nationkey) FROM nation where name < 'D' group by regionkey", sessionWithZeroTimeoutLimit);
        // No HBO statistics read
        assertPlan(
                sessionWithDefaultTimeoutLimit,
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(ProjectNode.class, node(FilterNode.class, any())).withOutputRowCount(12.5)));
        assertPlan(
                sessionWithDefaultTimeoutLimit,
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(AggregationNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(Double.NaN)));

        // Write HBO Statistics is successful, as we use the default 10 seconds timeout limit
        executeAndTrackHistory("SELECT max(nationkey) FROM nation where name < 'D' group by regionkey", sessionWithDefaultTimeoutLimit);
        // Read HBO statistics successfully with default timeout
        assertPlan(
                sessionWithDefaultTimeoutLimit,
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(ProjectNode.class, node(FilterNode.class, any())).withOutputRowCount(5).withOutputSize(90)));
        assertPlan(
                sessionWithDefaultTimeoutLimit,
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(AggregationNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(3).withOutputSize(54)));

        // Read HBO statistics fail due to timeout
        assertPlan(
                sessionWithZeroTimeoutLimit,
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(ProjectNode.class, node(FilterNode.class, any())).withOutputRowCount(12.5)));
        assertPlan(
                sessionWithZeroTimeoutLimit,
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(AggregationNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(Double.NaN)));
    }

    @Test
    public void testFailedQuery()
    {
        String sql = "select o.orderkey, l.partkey, l.mapcol[o.orderkey] from (select orderkey, partkey, mapcol from (select *, map(array[1], array[2]) mapcol from lineitem)) l " +
                "join orders o on l.partkey=o.custkey where length(comment)>10";
        Session session = Session.builder(createSession())
                .setSystemProperty(TRACK_HISTORY_STATS_FROM_FAILED_QUERIES, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .build();
        // CBO Statistics
        assertPlan(session, sql, anyTree(anyTree(any()), anyTree(node(ProjectNode.class, node(FilterNode.class, any())).withOutputRowCount(Double.NaN))));

        // HBO Statistics
        assertQueryFails(session, sql, ".*Key not present in map.*");
        getHistoryProvider().waitProcessQueryEvents();

        assertPlan(session, sql, anyTree(anyTree(any()), anyTree(node(ProjectNode.class, node(FilterNode.class, any())).withOutputRowCount(15000))));
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
                "SELECT nationkey FROM customer where nationkey < 10 UNION ALL SELECT nationkey FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(ExchangeNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(601)));
    }

    @Test
    public void testJoin()
    {
        assertPlan(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'",
                anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(Double.NaN).withJoinStatistics(Double.NaN, Double.NaN, Double.NaN, Double.NaN)));
        assertPlan(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'",
                anyTree(node(JoinNode.class, anyTree(anyTree(any()), anyTree(any())), anyTree(any())).withOutputRowCount(Double.NaN).withJoinStatistics(Double.NaN, Double.NaN, Double.NaN, Double.NaN)));

        executeAndTrackHistory("SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'");
        assertPlan(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'",
                anyTree(node(JoinNode.class, anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(2204).withJoinStatistics(1500, 0, 2204, 0)), anyTree(any()))));
        assertPlan(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'",
                anyTree(node(JoinNode.class, anyTree(anyTree(any()), anyTree(any())), anyTree(any())).withOutputRowCount(1915).withJoinStatistics(22, 0, 2204, 0)));
        // Check that output size doesn't include hash variables
        assertPlan(
                "SELECT N.name, O.totalprice, C.name FROM orders O, customer C, nation N WHERE N.nationkey = C.nationkey and C.custkey = O.custkey and year(O.orderdate) = 1995 AND substr(N.name, 1, 1) >= 'C'",
                anyTree(anyTree(anyTree(any()), anyTree(any())), anyTree(node(ProjectNode.class, anyTree(any())).withOutputRowCount(22).withOutputSize(661 - 22 * 8))));
    }

    @Test
    public void testJoinWithBuildNull()
    {
        assertPlan(
                "SELECT O.totalprice, C.name FROM orders O JOIN (SELECT name, custkey FROM customer UNION ALL SELECT * FROM (VALUES ('unknown', NULL)) t(name, custkey)) C ON C.custkey = O.custkey AND YEAR(O.orderdate) = 1995",
                anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(Double.NaN).withJoinStatistics(Double.NaN, Double.NaN, Double.NaN, Double.NaN)));

        executeAndTrackHistory("SELECT O.totalprice, C.name FROM orders O JOIN (SELECT name, custkey FROM customer UNION ALL SELECT * FROM (VALUES ('unknown', NULL)) t(name, custkey)) C ON C.custkey = O.custkey AND YEAR(O.orderdate) = 1995");
        assertPlan(
                "SELECT O.totalprice, C.name FROM orders O JOIN (SELECT name, custkey FROM customer UNION ALL SELECT * FROM (VALUES ('unknown', NULL)) t(name, custkey)) C ON C.custkey = O.custkey AND YEAR(O.orderdate) = 1995",
                anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(2204).withJoinStatistics(1501, 1, 2204, 0)));
    }

    @Test
    public void testJoinWithProbeNull()
    {
        String sql = "SELECT\n" +
                "    O.totalprice,\n" +
                "    C.name\n" +
                "FROM (\n" +
                "    SELECT\n" +
                "        totalprice,\n" +
                "        custkey,\n" +
                "        orderdate\n" +
                "    FROM orders\n" +
                "\n" +
                "    UNION ALL\n" +
                "\n" +
                "    SELECT\n" +
                "        *\n" +
                "    FROM (\n" +
                "        VALUES\n" +
                "            (10.0, NULL, date '1995-09-20')\n" +
                "    )\n" +
                ") O\n" +
                "JOIN customer C\n" +
                "    ON C.custkey = O.custkey\n" +
                "    AND YEAR(O.orderdate) = 1995";
        assertPlan(sql, anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(Double.NaN).withJoinStatistics(Double.NaN, Double.NaN, Double.NaN, Double.NaN)));

        executeAndTrackHistory(sql);
        assertPlan(sql, anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(2204).withJoinStatistics(1500, 0, 2205, 1)));
    }

    @Test
    public void testJoinNull()
    {
        String sql = "SELECT\n" +
                "    O.totalprice,\n" +
                "    C.name\n" +
                "FROM (\n" +
                "    SELECT\n" +
                "        totalprice,\n" +
                "        custkey,\n" +
                "        orderdate\n" +
                "    FROM orders\n" +
                "\n" +
                "    UNION ALL\n" +
                "\n" +
                "    SELECT\n" +
                "        *\n" +
                "    FROM (\n" +
                "        VALUES\n" +
                "            (10.0, NULL, DATE '1995-09-20')\n" +
                "    )\n" +
                ") O\n" +
                "JOIN (\n" +
                "    SELECT\n" +
                "        name,\n" +
                "        custkey\n" +
                "    FROM customer\n" +
                "\n" +
                "    UNION ALL\n" +
                "\n" +
                "    SELECT\n" +
                "        *\n" +
                "    FROM (\n" +
                "        VALUES\n" +
                "            ('unknown', NULL)\n" +
                "    ) t(name, custkey)\n" +
                ") C\n" +
                "    ON C.custkey = O.custkey\n" +
                "    AND YEAR(O.orderdate) = 1995";
        assertPlan(sql, anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(Double.NaN).withJoinStatistics(Double.NaN, Double.NaN, Double.NaN, Double.NaN)));

        executeAndTrackHistory(sql);
        assertPlan(sql, anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(2204).withJoinStatistics(1501, 1, 2205, 1)));
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

    @Test
    public void testTopN()
    {
        String query = "SELECT orderkey FROM orders where orderkey < 1000 GROUP BY 1 ORDER BY 1 DESC LIMIT 10000";
        assertPlan(query, anyTree(node(TopNNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(Double.NaN)));
        executeAndTrackHistory(query);
        assertPlan(query, anyTree(node(TopNNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(255)));
    }

    @Test
    public void testUseHistoriesWithLimitingPlanNode()
    {
        String query = "SELECT orderkey FROM orders where orderkey < 1000 GROUP BY 1 ORDER BY 1 DESC LIMIT 10000";
        Session session = Session.builder(createSession())
                .setSystemProperty(HISTORY_CANONICAL_PLAN_NODE_LIMIT, "2")
                .build();
        assertPlan(session, query, anyTree(node(TopNNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(Double.NaN)));
        getQueryRunner().execute(session, query);
        assertFalse(getHistoryProvider().waitProcessQueryEventsIfAvailable());
        assertPlan(session, query, anyTree(node(TopNNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(Double.NaN)));
    }

    @Test
    public void testTopNRowNumber()
    {
        String query = "SELECT orderstatus FROM (SELECT orderstatus, row_number() OVER (PARTITION BY orderstatus ORDER BY custkey) n FROM orders) WHERE n = 1";
        assertPlan(query, anyTree(node(TopNRowNumberNode.class, anyTree(any())).withOutputRowCount(Double.NaN)));
        executeAndTrackHistory(query);
        assertPlan(query, anyTree(node(TopNRowNumberNode.class, anyTree(any())).withOutputRowCount(3)));
    }

    @Test
    public void testDistinctLimit()
    {
        String query = "SELECT distinct regionkey from nation limit 2";
        assertPlan(query, anyTree(node(DistinctLimitNode.class, anyTree(any())).withOutputRowCount(Double.NaN)));
        executeAndTrackHistory(query);
        assertPlan(query, anyTree(node(DistinctLimitNode.class, anyTree(any())).withOutputRowCount(2)));
    }

    @Test
    public void testBroadcastJoin()
    {
        Session broadcastSession = Session.builder(getQueryRunner().getDefaultSession()).setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST").build();
        String sql = "select s.name, s.acctbal, sum(l.quantity) from lineitem l join supplier s on l.suppkey=s.suppkey where acctbal < 0 and length(l.comment) > 2 group by 1, 2";
        // CBO Statistics
        assertPlan(
                broadcastSession,
                sql,
                anyTree(
                        node(ProjectNode.class, anyTree(any())).withOutputRowCount(NaN),
                        anyTree(any())));

        // HBO Statistics
        executeAndTrackHistory(sql, broadcastSession);
        assertPlan(
                broadcastSession,
                sql,
                anyTree(
                        node(ProjectNode.class, anyTree(any())).withOutputRowCount(60175.0),
                        anyTree(any())));
    }

    @Test
    public void testFactPrioritization()
    {
        String query1 = "SELECT (SELECT nationkey FROM nation WHERE name = 'UNITED STATES') AS us_nationkey";
        executeAndTrackHistory(query1);
        assertPlan(query1, anyTree(node(EnforceSingleRowNode.class, anyTree(any()))
                .withOutputRowCount(1)
                .withSourceInfo(new CostBasedSourceInfo(FACT)))
                .withConfidenceLevel(FACT));

        Session session = Session.builder(createSession()).setSystemProperty("prefer_partial_aggregation", "false").build();
        String query2 = "SELECT COUNT(*) FROM orders";
        executeAndTrackHistory(query2);
        assertPlan(session, query2, anyTree(node(AggregationNode.class, anyTree(any())))
                .withOutputRowCount(1)
                .withSourceInfo(new CostBasedSourceInfo(FACT))
                .withConfidenceLevel(FACT));
    }

    @Test
    public void testBroadcastHighConfidence()
    {
        Session broadcastSession = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "AUTOMATIC")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(CONFIDENCE_BASED_BROADCAST_ENABLED, "TRUE")
                .setSystemProperty("prefer_partial_aggregation", "false")
                .build();
        String sql = "SELECT COUNT(*) FROM lineitem l JOIN supplier s ON l.suppkey = s.suppkey";

        executeAndTrackHistory(sql, broadcastSession);
        assertPlan(
                broadcastSession,
                sql,
                anyTree(
                        node(AggregationNode.class, anyTree(any())).withOutputRowCount(1).withConfidenceLevel(FACT)));
    }

    private void executeAndTrackHistory(String sql)
    {
        getQueryRunner().execute(sql);
        getHistoryProvider().waitProcessQueryEvents();
    }

    private void executeAndTrackHistory(String sql, Session session)
    {
        getQueryRunner().execute(session, sql);
        getHistoryProvider().waitProcessQueryEvents();
    }

    private void executeAndNoHistoryWritten(String sql, Session session)
    {
        getQueryRunner().execute(session, sql);
        getHistoryProvider().noProcessQueryEvents();
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
                .setSystemProperty(USE_PERFECTLY_CONSISTENT_HISTORIES, "true")
                .setSystemProperty("task_concurrency", "1")
                .setSystemProperty(RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY, "false")
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
    }
}
