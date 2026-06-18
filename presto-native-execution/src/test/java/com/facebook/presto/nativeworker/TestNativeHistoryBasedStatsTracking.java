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
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.testing.InMemoryHistoryBasedPlanStatisticsProvider;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.any;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static java.lang.Double.NaN;

@Test(singleThreaded = true)
public class TestNativeHistoryBasedStatsTracking
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setUseThrift(true)
                .build();
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp()
    {
        getHistoryProvider().clearCache();
    }

    @Test
    public void testDynamicFilterEnabled()
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
                        node(ProjectNode.class, anyTree(any())).withOutputRowCount(NaN),
                        anyTree(any())));
    }

    private void executeAndTrackHistory(String sql, Session session)
    {
        getQueryRunner().execute(session, sql);
        getHistoryProvider().waitProcessQueryEvents();
    }

    private InMemoryHistoryBasedPlanStatisticsProvider getHistoryProvider()
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) getQueryRunner();
        SqlQueryManager sqlQueryManager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        return (InMemoryHistoryBasedPlanStatisticsProvider) sqlQueryManager.getHistoryBasedPlanStatisticsTracker().getHistoryBasedPlanStatisticsProvider();
    }
}
