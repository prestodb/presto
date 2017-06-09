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
package com.facebook.presto.execution.resourceGroups.db;

import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.resourceGroups.db.H2ResourceGroupsDao;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupInfo;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.cancelQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.createQueryRunner;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getDao;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getDbConfigUrl;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getSelectors;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getSimpleQueryRunner;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.newDashboardSession;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.newRejectionSession;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.newSession;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.waitForCompleteQueryCount;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.waitForRunningQueryCount;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_REJECTED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

// run single threaded to avoid creating multiple query runners at once
@Test(singleThreaded = true)
public class TestQueues
{
    // Copy of TestQueues with tests for db reconfiguration of resource groups
    private static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM lineitem";

    @Test(timeOut = 60_000)
    public void testRunningQuery()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = getSimpleQueryRunner()) {
            queryRunner.execute("SELECT COUNT(*), clerk FROM orders GROUP BY clerk");
            while (true) {
                ResourceGroupInfo global = queryRunner.getCoordinator().getResourceGroupManager().get().getResourceGroupInfo(new ResourceGroupId(new ResourceGroupId("global"), "bi-user"));
                if (global.getSoftMemoryLimit().toBytes() > 0) {
                    break;
                }
                TimeUnit.SECONDS.sleep(2);
            }
        }
    }

    @Test(timeOut = 60_000)
    public void testBasic()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        try (DistributedQueryRunner queryRunner = createQueryRunner(dbConfigUrl, dao)) {
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            // submit first "dashboard" query
            QueryId firstDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);

            // wait for the first "dashboard" query to start
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);
            waitForRunningQueryCount(queryRunner, 1);
            // submit second "dashboard" query
            QueryId secondDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            MILLISECONDS.sleep(2000);
            // wait for the second "dashboard" query to be queued ("dashboard.${USER}" queue strategy only allows one "dashboard" query to be accepted for execution)
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);
            waitForRunningQueryCount(queryRunner, 1);
            // Update db to allow for 1 more running query in dashboard resource group
            dao.updateResourceGroup(3, "user-${USER}", "1MB", 3, 4, null, null, null, null, null, null, null, 1L);
            dao.updateResourceGroup(5, "dashboard-${USER}", "1MB", 1, 2, null, null, null, null, null, null, null, 3L);
            waitForQueryState(queryRunner, secondDashboardQuery, RUNNING);
            QueryId thirdDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, thirdDashboardQuery, QUEUED);
            waitForRunningQueryCount(queryRunner, 2);
            // submit first non "dashboard" query
            QueryId firstNonDashboardQuery = createQuery(queryRunner, newSession(), LONG_LASTING_QUERY);
            // wait for the first non "dashboard" query to start
            waitForQueryState(queryRunner, firstNonDashboardQuery, RUNNING);
            waitForRunningQueryCount(queryRunner, 3);
            // submit second non "dashboard" query
            QueryId secondNonDashboardQuery = createQuery(queryRunner, newSession(), LONG_LASTING_QUERY);
            // wait for the second non "dashboard" query to start
            waitForQueryState(queryRunner, secondNonDashboardQuery, RUNNING);
            waitForRunningQueryCount(queryRunner, 4);
            // cancel first "dashboard" query, the second "dashboard" query and second non "dashboard" query should start running
            cancelQuery(queryRunner, firstDashboardQuery);
            waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
            waitForQueryState(queryRunner, thirdDashboardQuery, RUNNING);
            waitForRunningQueryCount(queryRunner, 4);
            waitForCompleteQueryCount(queryRunner, 1);
        }
    }

    @Test(timeOut = 60_000)
    public void testTwoQueriesAtSameTime()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        try (DistributedQueryRunner queryRunner = createQueryRunner(dbConfigUrl, dao)) {
            QueryId firstDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            QueryId secondDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);

            ImmutableSet<QueryState> queuedOrRunning = ImmutableSet.of(QUEUED, RUNNING);
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);
        }
    }

    @Test(timeOut = 60_000)
    public void testTooManyQueries()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        try (DistributedQueryRunner queryRunner = createQueryRunner(dbConfigUrl, dao)) {
            QueryId firstDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);

            QueryId secondDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);

            QueryId thirdDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, thirdDashboardQuery, FAILED);

            // Allow one more query to run and resubmit third query
            dao.updateResourceGroup(3, "user-${USER}", "1MB", 3, 4, null, null, null, null, null, null, null, 1L);
            dao.updateResourceGroup(5, "dashboard-${USER}", "1MB", 1, 2, null, null, null, null, null, null, null, 3L);
            waitForQueryState(queryRunner, secondDashboardQuery, RUNNING);
            thirdDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, thirdDashboardQuery, QUEUED);

            // Lower running queries in dashboard resource groups and wait until groups are reconfigured
            dao.updateResourceGroup(5, "dashboard-${USER}", "1MB", 1, 1, null, null, null, null, null, null, null, 3L);
            ResourceGroupManager manager = queryRunner.getCoordinator().getResourceGroupManager().get();
            while (manager.getResourceGroupInfo(
                    new ResourceGroupId(new ResourceGroupId(new ResourceGroupId("global"), "user-user"), "dashboard-user")).getMaxRunningQueries() != 1) {
                MILLISECONDS.sleep(500);
            }
            // Cancel query and verify that third query is still queued
            cancelQuery(queryRunner, firstDashboardQuery);
            waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
            MILLISECONDS.sleep(2000);
            waitForQueryState(queryRunner, thirdDashboardQuery, QUEUED);
        }
    }

    @Test(timeOut = 60_000)
    public void testRejection()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        try (DistributedQueryRunner queryRunner = createQueryRunner(dbConfigUrl, dao)) {
            // Verify the query cannot be submitted
            QueryId queryId = createQuery(queryRunner, newRejectionSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            assertEquals(queryManager.getQueryInfo(queryId).getErrorCode(), QUERY_REJECTED.toErrorCode());
            int selectorCount = getSelectors(queryRunner).size();
            dao.insertSelector(4, "user.*", "(?i).*reject.*");
            assertEquals(dao.getSelectors().size(), selectorCount + 1);
            while (getSelectors(queryRunner).size() == selectorCount) {
                MILLISECONDS.sleep(500);
            }
            // Verify the query can be submitted
            queryId = createQuery(queryRunner, newRejectionSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, queryId, RUNNING);
            dao.deleteSelector(4, "user.*", "(?i).*reject.*");
            while (getSelectors(queryRunner).size() != selectorCount) {
                MILLISECONDS.sleep(500);
            }
            // Verify the query cannot be submitted
            queryId = createQuery(queryRunner, newRejectionSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, queryId, FAILED);
        }
    }

    @Test(timeOut = 60_000)
    public void testRunningTimeLimit()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        try (DistributedQueryRunner queryRunner = createQueryRunner(dbConfigUrl, dao)) {
            dao.updateResourceGroup(5, "dashboard-${USER}", "1MB", 1, 1, null, null, null, null, null, null, "3s", 3L);
            QueryId firstDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, firstDashboardQuery, FAILED);
        }
    }

    @Test(timeOut = 60_000)
    public void testQueuedTimeLimit()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        try (DistributedQueryRunner queryRunner = createQueryRunner(dbConfigUrl, dao)) {
            dao.updateResourceGroup(5, "dashboard-${USER}", "1MB", 1, 1, null, null, null, null, null, "5s", null, 3L);
            QueryId firstDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, firstDashboardQuery, RUNNING);
            QueryId secondDashboardQuery = createQuery(queryRunner, newDashboardSession(), LONG_LASTING_QUERY);
            waitForQueryState(queryRunner, secondDashboardQuery, QUEUED);
            waitForQueryState(queryRunner, secondDashboardQuery, FAILED);
        }
    }
}
