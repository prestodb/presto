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
package com.facebook.presto.server;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToExecuting;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToFirstResult;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToQueued;
import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClusterStatsResource
{
    private HttpClient client;
    private TestingPrestoServer server;

    @BeforeClass
    public void setup()
            throws Exception
    {
        client = new JettyHttpClient();
        DistributedQueryRunner runner = createQueryRunner(ImmutableMap.of("query.client.timeout", "10s"));
        server = runner.getCoordinator();
        server.getResourceGroupManager().get().addConfigurationManagerFactory(new FileResourceGroupConfigurationManagerFactory());
        server.getResourceGroupManager().get()
                .setConfigurationManager("file", ImmutableMap.of("resource-groups.config-file", getResourceFilePath("resource_groups_config_simple.json")));
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(server);
        closeQuietly(client);
        server = null;
        client = null;
    }

    @Test(timeOut = 120_000)
    public void testClusterStatsAdjustedQueueSize()
            throws Exception
    {
        runToFirstResult(client, server, "SELECT * from tpch.sf101.orders");
        runToFirstResult(client, server, "SELECT * from tpch.sf102.orders");
        runToFirstResult(client, server, "SELECT * from tpch.sf102.orders");
        runToQueued(client, server, "SELECT * from tpch.sf104.orders");

        ClusterStatsResource.ClusterStats clusterStats = getClusterStats(true);
        assertNotNull(clusterStats);
        assertEquals(clusterStats.getRunningQueries(), 3);
        assertEquals(clusterStats.getQueuedQueries(), 1);
        assertEquals(clusterStats.getAdjustedQueueSize(), 0);
    }

    @Test(timeOut = 60_000, enabled = false)
    public void testGetClusterStats()
            throws Exception
    {
        runToExecuting(client, server, "SELECT * from tpch.sf100.orders");

        // Sleep to allow query to make some progress
        sleep(SECONDS.toMillis(5));

        ClusterStatsResource.ClusterStats clusterStats = getClusterStats(true);

        assertNotNull(clusterStats);
        assertEquals(clusterStats.getActiveWorkers(), 4);
        assertEquals(clusterStats.getRunningTasks(), 5);
        assertTrue(clusterStats.getRunningDrivers() > 0);
        assertEquals(clusterStats.getRunningQueries(), 1);
        assertEquals(clusterStats.getBlockedQueries(), 0);
        assertEquals(clusterStats.getQueuedQueries(), 0);
    }

    private ClusterStatsResource.ClusterStats getClusterStats(boolean followRedirects)
    {
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
                .setUri(uriBuilderFrom(server.getBaseUrl().resolve("/v1/cluster")).build())
                .setFollowRedirects(followRedirects)
                .build();

        return client.execute(request, createJsonResponseHandler(jsonCodec(ClusterStatsResource.ClusterStats.class)));
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
