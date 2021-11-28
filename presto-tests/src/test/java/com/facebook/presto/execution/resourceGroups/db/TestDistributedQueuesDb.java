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

import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.resourceGroups.db.H2ResourceGroupsDao;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.cancelQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.adhocSession;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.createQueryRunner;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.dashboardSession;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getDao;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.getDbConfigUrl;
import static com.facebook.presto.execution.resourceGroups.db.H2TestUtil.testSession;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

// run single threaded to avoid creating multiple query runners at once
@Test(singleThreaded = true)
public class TestDistributedQueuesDb
{
    private static final String LONG_LASTING_QUERY = "SELECT COUNT(*) FROM lineitem";
    private DistributedQueryRunner queryRunner;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        String dbConfigUrl = getDbConfigUrl();
        H2ResourceGroupsDao dao = getDao(dbConfigUrl);
        ImmutableMap.Builder<String, String> coordinatorProperties = new ImmutableMap.Builder<>();
        coordinatorProperties.put("query-manager.experimental.required-coordinators", "2");
        coordinatorProperties.put("resource-manager.query-heartbeat-interval", "10ms");
        coordinatorProperties.put("resource-group-runtimeinfo-refresh-interval", "100ms");
        coordinatorProperties.put("concurrency-threshold-to-enable-resource-group-refresh", "0");

        queryRunner = createQueryRunner(dbConfigUrl, dao, coordinatorProperties.build(), 2);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        closeQuietly(queryRunner);
        queryRunner = null;
    }

    @Test(timeOut = 60_000)
    public void testResourceGroupConcurrencyThreshold()
            throws Exception
    {
        QueryId firstAdhocQuery = createQuery(queryRunner, 1, adhocSession(), LONG_LASTING_QUERY);

        QueryId secondAdhocQuery = createQuery(queryRunner, 1, adhocSession(), LONG_LASTING_QUERY);

        QueryId thirdAdhocQuery = createQuery(queryRunner, 1, adhocSession(), LONG_LASTING_QUERY);

        waitForQueryState(queryRunner, 1, firstAdhocQuery, RUNNING);
        waitForQueryState(queryRunner, 1, secondAdhocQuery, RUNNING);
        waitForQueryState(queryRunner, 1, thirdAdhocQuery, RUNNING);

        Map<ResourceGroupId, ResourceGroupRuntimeInfo> resourceGroupRuntimeInfoSnapshot;
        int globalRunningQueries = 0;
        do {
            MILLISECONDS.sleep(100);
            resourceGroupRuntimeInfoSnapshot = queryRunner.getCoordinator(0).getResourceGroupManager().get().getResourceGroupRuntimeInfosSnapshot();
            ResourceGroupRuntimeInfo resourceGroupRuntimeInfo = resourceGroupRuntimeInfoSnapshot.get(new ResourceGroupId("global"));
            if (resourceGroupRuntimeInfo != null) {
                globalRunningQueries = resourceGroupRuntimeInfo.getDescendantRunningQueries();
            }
        } while (globalRunningQueries != 3);

        QueryId fourthAdhocQuery = createQuery(queryRunner, 0, adhocSession(), LONG_LASTING_QUERY);

        waitForQueryState(queryRunner, 0, fourthAdhocQuery, QUEUED);

        cancelQuery(queryRunner, 1, firstAdhocQuery);
        waitForQueryState(queryRunner, 0, fourthAdhocQuery, RUNNING);
    }

    @Test(timeOut = 60_000)
    public void testMultiResourceGroupConcurrencyThreshold()
            throws Exception
    {
        QueryId firstAdhocQuery = createQuery(queryRunner, 1, adhocSession(), LONG_LASTING_QUERY);

        QueryId secondAdhocQuery = createQuery(queryRunner, 1, adhocSession(), LONG_LASTING_QUERY);

        QueryId thirdAdhocQuery = createQuery(queryRunner, 1, adhocSession(), LONG_LASTING_QUERY);

        waitForQueryState(queryRunner, 1, firstAdhocQuery, RUNNING);
        waitForQueryState(queryRunner, 1, secondAdhocQuery, RUNNING);
        waitForQueryState(queryRunner, 1, thirdAdhocQuery, RUNNING);

        Map<ResourceGroupId, ResourceGroupRuntimeInfo> resourceGroupRuntimeInfoSnapshot;
        int globalRunningQueries = 0;
        do {
            MILLISECONDS.sleep(100);
            resourceGroupRuntimeInfoSnapshot = queryRunner.getCoordinator(0).getResourceGroupManager().get().getResourceGroupRuntimeInfosSnapshot();
            ResourceGroupRuntimeInfo resourceGroupRuntimeInfo = resourceGroupRuntimeInfoSnapshot.get(new ResourceGroupId("global"));
            if (resourceGroupRuntimeInfo != null) {
                globalRunningQueries = resourceGroupRuntimeInfo.getDescendantRunningQueries();
            }
        } while (globalRunningQueries != 3);

        QueryId firstDashboardQuery = createQuery(queryRunner, 0, dashboardSession(), LONG_LASTING_QUERY);

        waitForQueryState(queryRunner, 0, firstDashboardQuery, QUEUED);
        cancelQuery(queryRunner, 1, firstAdhocQuery);
        waitForQueryState(queryRunner, 0, firstDashboardQuery, RUNNING);
    }

    @Test(timeOut = 60_000)
    public void testDistributedQueue()
            throws Exception
    {
        QueryId firstAdhocQuery = createQuery(queryRunner, 1, adhocSession(), LONG_LASTING_QUERY);

        QueryId secondAdhocQuery = createQuery(queryRunner, 1, adhocSession(), LONG_LASTING_QUERY);

        QueryId thirdAdhocQuery = createQuery(queryRunner, 0, adhocSession(), LONG_LASTING_QUERY);

        waitForQueryState(queryRunner, 1, firstAdhocQuery, RUNNING);
        waitForQueryState(queryRunner, 1, secondAdhocQuery, RUNNING);
        waitForQueryState(queryRunner, 0, thirdAdhocQuery, RUNNING);

        Map<ResourceGroupId, ResourceGroupRuntimeInfo> resourceGroupRuntimeInfoSnapshot;
        int globalRunningQueries = 0;
        do {
            MILLISECONDS.sleep(100);
            globalRunningQueries = 0;
            for (int coordinator = 0; coordinator < 2; coordinator++) {
                resourceGroupRuntimeInfoSnapshot = queryRunner.getCoordinator(coordinator).getResourceGroupManager().get().getResourceGroupRuntimeInfosSnapshot();
                ResourceGroupRuntimeInfo resourceGroupRuntimeInfo = resourceGroupRuntimeInfoSnapshot.get(new ResourceGroupId("global"));
                if (resourceGroupRuntimeInfo != null) {
                    globalRunningQueries += resourceGroupRuntimeInfo.getDescendantRunningQueries();
                }
            }
        } while (globalRunningQueries != 3);

        QueryId firstDashboardQuery = createQuery(queryRunner, 0, dashboardSession(), LONG_LASTING_QUERY);

        waitForQueryState(queryRunner, 0, firstDashboardQuery, QUEUED);
        cancelQuery(queryRunner, 0, thirdAdhocQuery);
        waitForQueryState(queryRunner, 0, firstDashboardQuery, RUNNING);
    }

    @Test(timeOut = 1_000)
    public void testDistributedQueue_burstTraffic()
            throws Exception
    {
        QueryId firstAdhocQuery = createQuery(queryRunner, 1, testSession(new Identity("user1", Optional.empty())), LONG_LASTING_QUERY);

        QueryId secondAdhocQuery = createQuery(queryRunner, 0, testSession(new Identity("user2", Optional.empty())), LONG_LASTING_QUERY);

        QueryId thirdAdhocQuery = createQuery(queryRunner, 1, testSession(new Identity("user3", Optional.empty())), LONG_LASTING_QUERY);

        QueryId fourthAdhocQuery = createQuery(queryRunner, 0, testSession(new Identity("user4", Optional.empty())), LONG_LASTING_QUERY);

        Map<ResourceGroupId, ResourceGroupRuntimeInfo> resourceGroupRuntimeInfoSnapshot;
        int globalRunningQueries = 0;
        int globalQueriedQueries = 0;
        do {
            MILLISECONDS.sleep(100);
            globalRunningQueries = 0;
            globalQueriedQueries = 0;
            for (int coordinator = 0; coordinator < 2; coordinator++) {
                resourceGroupRuntimeInfoSnapshot = queryRunner.getCoordinator(coordinator).getResourceGroupManager().get().getResourceGroupRuntimeInfosSnapshot();
                ResourceGroupRuntimeInfo resourceGroupRuntimeInfo = resourceGroupRuntimeInfoSnapshot.get(new ResourceGroupId("global"));
                if (resourceGroupRuntimeInfo != null) {
                    globalRunningQueries += resourceGroupRuntimeInfo.getDescendantRunningQueries();
                    globalQueriedQueries += resourceGroupRuntimeInfo.getDescendantQueuedQueries();
                }
            }
        } while (globalRunningQueries != 3 && globalQueriedQueries != 1);
    }
}
