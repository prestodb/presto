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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.execution.SqlQueryManager;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.statistics.HistoryBasedPlanStatisticsProvider;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.InMemoryHistoryBasedPlanStatisticsProvider;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PARTIAL_AGGREGATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY;
import static com.facebook.presto.SystemSessionProperties.TRACK_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_HISTORY_BASED_PLAN_STATISTICS;
import static com.facebook.presto.SystemSessionProperties.USE_PERFECTLY_CONSISTENT_HISTORIES;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;

// InMemoryHistoryBasedPlanStatisticsProvider expects a single threaded environment.
@Test(singleThreaded = true)
public class TestPrestoNativeHistoryBasedStatistics
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createOrders(queryRunner);
        createLineitem(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner() throws Exception
    {
        QueryRunner queryRunner = PrestoNativeQueryRunnerUtils.createNativeQueryRunner(true);
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

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner() throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp()
    {
        getHistoryProvider().clearCache();
    }

    @Test
    public void testJoinNull()
    {
        String sql = "SELECT count(1) \n" +
                "FROM orders o LEFT JOIN lineitem l \n" +
                "   ON if(o.orderkey % 2 = 0, o.orderkey) = if(l.orderkey % 11 = 0, l.orderkey)";
        assertPlan(sql, anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(Double.NaN).withJoinStatistics(Double.NaN, Double.NaN, Double.NaN, Double.NaN)));

        assertQuery(createSession(), sql);
        getHistoryProvider().waitProcessQueryEvents();
        // Test if stats are recorded by HBO. RandomizeNullKeyInOuterJoin will not get triggered due to
        // high hardcoded thresholds - we can revamp this test once those thresholds are configurable.
        assertPlan(createSession(), sql, anyTree(node(JoinNode.class, anyTree(any()), anyTree(any())).withOutputRowCount(17117).withJoinStatistics(60175, 54599, 15000, 7500)));
        // Test correctness
        assertQuery(createSession(), sql);
    }

    private InMemoryHistoryBasedPlanStatisticsProvider getHistoryProvider()
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        SqlQueryManager sqlQueryManager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        return (InMemoryHistoryBasedPlanStatisticsProvider) sqlQueryManager.getHistoryBasedPlanStatisticsTracker().getHistoryBasedPlanStatisticsProvider();
    }

    private Session createSession()
    {
        return Session.builder(getSession())
                .setSystemProperty(USE_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(TRACK_HISTORY_BASED_PLAN_STATISTICS, "true")
                .setSystemProperty(USE_PERFECTLY_CONSISTENT_HISTORIES, "true")
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "never")
                .setSystemProperty("task_concurrency", "1")
                .setSystemProperty(RESTRICT_HISTORY_BASED_OPTIMIZATION_TO_COMPLEX_QUERY, "false")
                .build();
    }
}
