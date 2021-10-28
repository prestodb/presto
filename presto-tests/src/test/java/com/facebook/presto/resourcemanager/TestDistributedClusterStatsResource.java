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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.ClusterStatsResource;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToFirstResult;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToQueued;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDistributedClusterStatsResource
{
    private static final String RESOURCE_GROUPS_CONFIG_FILE = "resource_groups_config_simple.json";
    private static final int COORDINATOR_COUNT = 2;
    private static final String RESOURCE_GROUP_GLOBAL = "global";
    private HttpClient client;
    private TestingPrestoServer coordinator1;
    private TestingPrestoServer resourceManager;
    private TestingPrestoServer coordinator2;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        DistributedQueryRunner runner = createQueryRunner(
                ImmutableMap.of(
                        "resource-manager.query-expiration-timeout", "4m",
                        "resource-manager.completed-query-expiration-timeout", "4m"),
                ImmutableMap.of(
                        "query.client.timeout", "20s",
                        "resource-manager.query-heartbeat-interval", "100ms",
                        "resource-group-runtimeinfo-refresh-interval", "100ms",
                        "concurrency-threshold-to-enable-resource-group-refresh", "0.1"),
                ImmutableMap.of(),
                COORDINATOR_COUNT);
        coordinator1 = runner.getCoordinator(0);
        coordinator2 = runner.getCoordinator(1);
        Optional<TestingPrestoServer> resourceManager = runner.getResourceManager();
        checkState(resourceManager.isPresent(), "resource manager not present");
        this.resourceManager = resourceManager.get();
        runner.getCoordinators().stream().forEach(coordinator -> {
            coordinator.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
            coordinator.getResourceGroupManager().get()
                    .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath(RESOURCE_GROUPS_CONFIG_FILE)));
        });
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(coordinator1);
        closeQuietly(coordinator2);
        closeQuietly(resourceManager);
        closeQuietly(client);
        coordinator1 = null;
        coordinator2 = null;
        resourceManager = null;
        client = null;
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }

    @Test(timeOut = 60_000, enabled = false)
    public void testClusterStatsRedirectToResourceManager()
            throws Exception
    {
        waitUntilCoordinatorsDiscoveredHealthyInRM(SECONDS.toMillis(15));
        runToFirstResult(client, coordinator2, "SELECT * from tpch.sf102.orders");
        runToFirstResult(client, coordinator2, "SELECT * from tpch.sf100.orders");
        runToFirstResult(client, coordinator2, "SELECT * from tpch.sf101.orders");
        waitForGlobalQueryViewInCoordinator(3, RUNNING, coordinator1, SECONDS.toMillis(20));
        runToQueued(client, coordinator2, "SELECT * from tpch.sf100.orders");
        waitForGlobalQueryViewInCoordinator(1, QUEUED, coordinator1, SECONDS.toMillis(20));
        ClusterStatsResource.ClusterStats clusterStats = getClusterStats(true, coordinator1);
        assertNotNull(clusterStats);
        assertTrue(clusterStats.getActiveWorkers() > 0);
        assertEquals(clusterStats.getRunningQueries(), 3);
        assertEquals(clusterStats.getBlockedQueries(), 0);
        assertEquals(clusterStats.getQueuedQueries(), 1);
        assertEquals(clusterStats.getAdjustedQueueSize(), 0);
        assertEquals(clusterStats.getRunningTasks(), 12);
    }

    private void waitForGlobalQueryViewInCoordinator(int numberOfRunningQueries, QueryState state, TestingPrestoServer coordinator, long timeoutInMillis)
            throws InterruptedException, TimeoutException
    {
        long deadline = System.currentTimeMillis() + timeoutInMillis;
        int globalQueryCount = 0;
        while (System.currentTimeMillis() < deadline) {
            Optional<Integer> globalQueryCountFromCoordinator = getGlobalQueryCountIfAvailable(state, coordinator);
            if (!globalQueryCountFromCoordinator.isPresent()) {
                continue;
            }
            globalQueryCount = globalQueryCountFromCoordinator.get();
            if (globalQueryCount == numberOfRunningQueries) {
                return;
            }
            sleep(100);
        }
        throw new TimeoutException(format("Global Query Count: %s after %s ms", globalQueryCount, timeoutInMillis));
    }

    private Optional<Integer> getGlobalQueryCountIfAvailable(QueryState state, TestingPrestoServer coordinator)
    {
        Map<ResourceGroupId, ResourceGroupRuntimeInfo> resourceGroupRuntimeInfoSnapshot = coordinator.getResourceGroupManager().get().getResourceGroupRuntimeInfosSnapshot();
        ResourceGroupRuntimeInfo resourceGroupRuntimeInfo = resourceGroupRuntimeInfoSnapshot.get(new ResourceGroupId(RESOURCE_GROUP_GLOBAL));
        if (resourceGroupRuntimeInfo == null) {
            return Optional.empty();
        }
        int queryCount = 0;
        switch (state) {
            case RUNNING:
                queryCount = resourceGroupRuntimeInfo.getDescendantRunningQueries();
                break;
            case QUEUED:
                queryCount = resourceGroupRuntimeInfo.getDescendantQueuedQueries();
                break;
            default:
                fail(format("Unexpected query state %s", state));
        }
        return Optional.of(queryCount);
    }

    private void waitUntilCoordinatorsDiscoveredHealthyInRM(long timeoutInMillis)
            throws TimeoutException, InterruptedException
    {
        long deadline = System.currentTimeMillis() + timeoutInMillis;
        while (System.currentTimeMillis() < deadline) {
            AllNodes allNodes = this.resourceManager.refreshNodes();
            if (allNodes.getActiveCoordinators().size() == COORDINATOR_COUNT) {
                return;
            }
            sleep(100);
        }
        throw new TimeoutException(format("one of the nodes is still missing after: %s ms", timeoutInMillis));
    }

    private ClusterStatsResource.ClusterStats getClusterStats(boolean followRedirects, TestingPrestoServer coordinator)
    {
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
                .setUri(uriBuilderFrom(coordinator.getBaseUrl().resolve("/v1/cluster")).build())
                .setFollowRedirects(followRedirects)
                .build();

        return client.execute(request, createJsonResponseHandler(jsonCodec(ClusterStatsResource.ClusterStats.class)));
    }
}
