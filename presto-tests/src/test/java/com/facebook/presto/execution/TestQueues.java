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
import com.facebook.presto.resourceGroups.ResourceGroupManagerPlugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_REJECTED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class TestQueues
{
    private static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM lineitem";

    @Test(timeOut = 240_000)
    public void testSqlQueryQueueManager()
            throws Exception
    {
        testBasic(false);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManager()
            throws Exception
    {
        testBasic(true);
    }

    private void testBasic(boolean resourceGroups)
            throws Exception
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (resourceGroups) {
            builder.put("experimental.resource-groups-enabled", "true");
        }
        else {
            builder.put("query.queue-config-file", getResourceFilePath("queue_config_dashboard.json"));
        }
        Map<String, String> properties = builder.build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();

            // submit first "dashboard" query
            QueryId firstDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);

            // wait for the first "dashboard" query to start
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

            assertEquals(queryManager.getStats().getRunningQueries(), 1);

            // submit second "dashboard" query
            QueryId secondDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);

            // wait for the second "dashboard" query to be queued ("dashboard.${USER}" queue strategy only allows one "dashboard" query to be accepted for execution)
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

            assertEquals(queryManager.getStats().getRunningQueries(), 1);

            // submit first non "dashboard" query
            QueryId firstNonDashboardQuery = createQuery(queryRunner, newSession(), LONG_LASTING_QUERY);

            // wait for the first non "dashboard" query to start
            waitForQueryState(queryRunner, firstNonDashboardQuery, RUNNING);

            assertEquals(queryManager.getStats().getRunningQueries(), 2);

            // submit second non "dashboard" query
            QueryId secondNonDashboardQuery = createQuery(queryRunner, newSession(), LONG_LASTING_QUERY);

            // wait for the second non "dashboard" query to start
            waitForQueryState(queryRunner, secondNonDashboardQuery, RUNNING);

            assertEquals(queryManager.getStats().getRunningQueries(), 3);

            // cancel first "dashboard" query, second "dashboard" query and second non "dashboard" query should start running
            cancelQuery(queryRunner, firstDashboardQuery);
            waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
            waitForQueryState(queryRunner, secondDashboardQuery, RUNNING);

            assertEquals(queryManager.getStats().getRunningQueries(), 3);
            assertEquals(queryManager.getStats().getCompletedQueries().getTotalCount(), 1);
        }
    }

    @Test(timeOut = 240_000)
    public void testSqlQueryQueueManagerWithTwoDashboardQueriesRequestedAtTheSameTime()
            throws Exception
    {
        testTwoQueriesAtSameTime(false);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManagerWithTwoDashboardQueriesRequestedAtTheSameTime()
            throws Exception
    {
        testTwoQueriesAtSameTime(true);
    }

    private void testTwoQueriesAtSameTime(boolean resourceGroups)
            throws Exception
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (resourceGroups) {
            builder.put("experimental.resource-groups-enabled", "true");
        }
        else {
            builder.put("query.queue-config-file", getResourceFilePath("queue_config_dashboard.json"));
        }
        Map<String, String> properties = builder.build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryId firstDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            QueryId secondDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);

            ImmutableSet<QueryState> queuedOrRunning = ImmutableSet.of(QUEUED, RUNNING);
            waitForQueryState(queryRunner, firstDashboardQuery, queuedOrRunning);
            waitForQueryState(queryRunner, secondDashboardQuery, queuedOrRunning);
        }
    }

    @Test(timeOut = 240_000)
    public void testSqlQueryQueueManagerWithTooManyQueriesScheduled()
            throws Exception
    {
        testTooManyQueries(false);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManagerWithTooManyQueriesScheduled()
            throws Exception
    {
        testTooManyQueries(true);
    }

    private void testTooManyQueries(boolean resourceGroups)
            throws Exception
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (resourceGroups) {
            builder.put("experimental.resource-groups-enabled", "true");
        }
        else {
            builder.put("query.queue-config-file", getResourceFilePath("queue_config_dashboard.json"));
        }
        Map<String, String> properties = builder.build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryId firstDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

            QueryId secondDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

            QueryId thirdDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, thirdDashboardQuery, FAILED);
        }
    }

    @Test(timeOut = 240_000)
    public void testSqlQueryQueueManagerRejection()
            throws Exception
    {
        testRejection(false);
    }

    @Test(timeOut = 240_000)
    public void testResourceGroupManagerRejection()
            throws Exception
    {
        testRejection(true);
    }

    private void testRejection(boolean resourceGroups)
            throws Exception
    {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        if (resourceGroups) {
            builder.put("experimental.resource-groups-enabled", "true");
        }
        else {
            builder.put("query.queue-config-file", getResourceFilePath("queue_config_dashboard.json"));
        }
        Map<String, String> properties = builder.build();

        try (DistributedQueryRunner queryRunner = createQueryRunner(properties)) {
            queryRunner.installPlugin(new ResourceGroupManagerPlugin());
            queryRunner.getCoordinator().getResourceGroupManager().get().setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_dashboard.json")));

            QueryId queryId = createQuery(queryRunner, newRejectionSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            assertEquals(queryManager.getQueryInfo(queryId).getErrorCode(), QUERY_REJECTED.toErrorCode());
        }
    }

    private static QueryId createQuery(DistributedQueryRunner queryRunner, Session session, String sql)
    {
        return queryRunner.getCoordinator().getQueryManager().createQuery(session, sql).getQueryId();
    }

    private static void cancelQuery(DistributedQueryRunner queryRunner, QueryId queryId)
    {
        queryRunner.getCoordinator().getQueryManager().cancelQuery(queryId);
    }

    private static void waitForQueryState(DistributedQueryRunner queryRunner, QueryId queryId, QueryState expectedQueryState)
            throws InterruptedException
    {
        waitForQueryState(queryRunner, queryId, ImmutableSet.of(expectedQueryState));
    }

    private static void waitForQueryState(DistributedQueryRunner queryRunner, QueryId queryId, Set<QueryState> expectedQueryStates)
            throws InterruptedException
    {
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        do {
            MILLISECONDS.sleep(500);
        }
        while (!expectedQueryStates.contains(queryManager.getQueryInfo(queryId).getState()));
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    private static DistributedQueryRunner createQueryRunner(Map<String, String> properties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(testSessionBuilder().build(), 2, ImmutableMap.of(), properties, new SqlParserOptions());

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    private static Session newSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("adhoc")
                .build();
    }

    private static Session newDashboardSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("dashboard")
                .build();
    }

    private static Session newRejectionSession()
    {
        return testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf100000")
                .setSource("reject")
                .build();
    }
}
