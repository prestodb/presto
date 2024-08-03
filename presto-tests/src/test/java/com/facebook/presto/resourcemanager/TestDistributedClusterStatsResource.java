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
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.ClusterStatsResource;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.testing.TestingPeriodicTaskExecutorFactory;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToQueued;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToRunning;
import static com.facebook.presto.utils.ResourceUtils.getResourceFilePath;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.lang.Thread.sleep;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDistributedClusterStatsResource
{
    private static final String RESOURCE_GROUPS_CONFIG_FILE = "resource_groups_config_simple.json";
    private static final int COORDINATOR_COUNT = 2;
    private static final String RESOURCE_GROUP_GLOBAL = "global";
    private HttpClient client;
    private TestingPrestoServer coordinator1;
    private TestingPrestoServer resourceManager;
    private TestingPrestoServer coordinator2;
    private DistributedQueryRunner runner;
    private Closer closer;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        runner = createQueryRunner(
                ImmutableMap.of(
                        "resource-manager.query-expiration-timeout", "4m",
                        "resource-manager.completed-query-expiration-timeout", "4m"),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(
                        "query.client.timeout", "2m",
                        "resource-manager.query-heartbeat-interval", "100ms",
                        "resource-group-runtimeinfo-refresh-interval", "100ms",
                        "concurrency-threshold-to-enable-resource-group-refresh", "1"),
                ImmutableMap.of(),
                COORDINATOR_COUNT,
                false,
                1,
                true);
        coordinator1 = runner.getCoordinator(0);
        coordinator2 = runner.getCoordinator(1);
        Optional<TestingPrestoServer> resourceManager = runner.getResourceManager();
        checkState(resourceManager.isPresent(), "resource manager not present");
        this.resourceManager = resourceManager.get();
        runner.getCoordinators().stream().forEach(coordinator -> {
            coordinator.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
            coordinator.getResourceGroupManager().get()
                    .forceSetConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath(RESOURCE_GROUPS_CONFIG_FILE)));
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

    @BeforeMethod
    public void prepareToRunQueries() throws Exception
    {
        assertNull(closer);
        closer = Closer.create();

        waitUntilCoordinatorsDiscoveredHealthyInRM(SECONDS.toMillis(15));
        coordinator1.getPeriodicTaskExecutorFactory().get().tick();
        coordinator2.getPeriodicTaskExecutorFactory().get().tick();
    }

    @AfterMethod
    public void cancelRunningQueries() throws IOException
    {
        closer.close();
        closer = null;
    }

    @Test(timeOut = 120_000)
    public void testClusterStatsRedirectToResourceManager()
    {
        closer.register(runToRunning(client, coordinator2, "SELECT * from tpch.sf102.orders"));
        closer.register(runToRunning(client, coordinator2, "SELECT * from tpch.sf100.orders"));
        closer.register(runToRunning(client, coordinator2, "SELECT * from tpch.sf101.orders"));
        tick(coordinator1, coordinator2);
        runToQueued(client, coordinator2, "SELECT * from tpch.sf100.orders");
        tick(coordinator1, coordinator2);
        ClusterStatsResource.ClusterStats clusterStats = getClusterStats(true, coordinator1, false);
        assertNotNull(clusterStats);
        assertTrue(clusterStats.getActiveWorkers() > 0);
        assertEquals(clusterStats.getRunningQueries(), 3);
        assertEquals(clusterStats.getBlockedQueries(), 0);
        assertEquals(clusterStats.getQueuedQueries(), 1);
        assertEquals(clusterStats.getAdjustedQueueSize(), 0);
        assertEquals(clusterStats.getRunningTasks(), 12);
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

    private ClusterStatsResource.ClusterStats getClusterStats(boolean followRedirects, TestingPrestoServer coordinator, boolean includeLocalInfo)
    {
        String localInfo = "";
        if (includeLocalInfo) {
            localInfo = "?includeLocalInfoOnly=true";
        }
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
                .setUri(uriBuilderFrom(coordinator.getBaseUrl().resolve("/v1/cluster" + localInfo)).build())
                .setFollowRedirects(followRedirects)
                .build();

        return client.execute(request, createJsonResponseHandler(jsonCodec(ClusterStatsResource.ClusterStats.class)));
    }

    private void tick(TestingPrestoServer... coordinators)
    {
        requireNonNull(coordinators, "coordinators is null");
        for (TestingPrestoServer coordinator : coordinators) {
            coordinator.getPeriodicTaskExecutorFactory().ifPresent(TestingPeriodicTaskExecutorFactory::tick);
        }
    }

    @Test(timeOut = 120_000)
    public void testClusterStatsLocalInfoReturn()
            throws Exception
    {
        closer.register(runToRunning(client, coordinator2, "SELECT * from tpch.sf100.orders"));
        tick(coordinator1, coordinator2);
        closer.register(runToRunning(client, coordinator2, "SELECT * from tpch.sf101.orders"));
        tick(coordinator2, coordinator1);
        closer.register(runToRunning(client, coordinator1, "SELECT * from tpch.sf101.orders"));
        tick(coordinator1, coordinator2);
        closer.register(runToQueued(client, coordinator2, "SELECT * from tpch.sf101.orders"));
        tick(coordinator1, coordinator2);

        ClusterStatsResource.ClusterStats clusterLocalStatsCoord1 = getClusterStats(false, coordinator1, true);
        assertNotNull(clusterLocalStatsCoord1);
        assertTrue(clusterLocalStatsCoord1.getActiveWorkers() > 0);
        assertEquals(clusterLocalStatsCoord1.getRunningQueries(), 1);
        assertEquals(clusterLocalStatsCoord1.getQueuedQueries(), 0);
        assertEquals(clusterLocalStatsCoord1.getBlockedQueries(), 0);

        ClusterStatsResource.ClusterStats clusterLocalStatsCoord2 = getClusterStats(false, coordinator2, true);
        assertNotNull(clusterLocalStatsCoord2);
        assertTrue(clusterLocalStatsCoord2.getActiveWorkers() > 0);
        assertEquals(clusterLocalStatsCoord2.getRunningQueries(), 2);
        assertEquals(clusterLocalStatsCoord2.getQueuedQueries(), 1);
        assertEquals(clusterLocalStatsCoord2.getBlockedQueries(), 0);

        ClusterStatsResource.ClusterStats clusterStats = getClusterStats(false, coordinator1, false);
        assertNotNull(clusterStats);
        assertTrue(clusterStats.getActiveWorkers() > 0);
        assertEquals(clusterStats.getRunningQueries(), 3);
        assertEquals(clusterStats.getQueuedQueries(), 1);
        assertEquals(clusterStats.getBlockedQueries(), 0);
    }
}
