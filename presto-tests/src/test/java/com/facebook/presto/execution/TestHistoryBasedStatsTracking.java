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
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.statistics.InMemoryHistoryBasedPlanStatisticsProvider;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

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
                anyTree(node(FilterNode.class, any()).withOutputRowCount(2)));

        executeAndTrackHistory("SELECT *, 1 FROM nation where substr(name, 1, 1) = 'A'");
        assertPlan(
                "SELECT *, 2 FROM nation where substr(name, 1, 1) = 'A'",
                anyTree(node(ProjectNode.class, anyTree(any())).withOutputRowCount(2)));

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
                anyTree(node(ProjectNode.class, node(FilterNode.class, any())).withOutputRowCount(5)));

        assertPlan(
                "SELECT max(nationkey) FROM nation where name < 'D' group by regionkey",
                anyTree(node(AggregationNode.class, node(ExchangeNode.class, anyTree(any()))).withOutputRowCount(3)));
    }

    private void executeAndTrackHistory(String sql)
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        SqlQueryManager sqlQueryManager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        InMemoryHistoryBasedPlanStatisticsProvider provider = (InMemoryHistoryBasedPlanStatisticsProvider) sqlQueryManager.getHistoryBasedPlanStatisticsTracker().getHistoryBasedPlanStatisticsProvider();

        queryRunner.execute(sql);
        provider.waitProcessQueryEvents();
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
