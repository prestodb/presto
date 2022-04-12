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
import com.facebook.airlift.http.client.thrift.ThriftRequestUtils;
import com.facebook.airlift.http.client.thrift.ThriftResponseHandler;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.resourceGroups.FileResourceGroupConfigurationManagerFactory;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.server.ClusterStatsResource.ClusterStats;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.facebook.presto.utils.QueryExecutionClientUtil.runToExecuting;
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
        DistributedQueryRunner runner = createQueryRunner(ImmutableMap.of("query.client.timeout", "20s"));
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

    @DataProvider
    public Object[][] thriftEncodingToggle()
    {
        return new Object[][] {{true, Protocol.BINARY}, {true, Protocol.COMPACT}, {true, Protocol.FB_COMPACT}, {false, null}};
    }

    @Test(timeOut = 120_000, dataProvider = "thriftEncodingToggle")
    public void testClusterStatsAdjustedQueueSize(boolean useThriftEncoding, Protocol thriftProtocol)
    {
        runToExecuting(client, server, "SELECT * from tpch.sf101.orders");
        runToExecuting(client, server, "SELECT * from tpch.sf102.orders");
        runToExecuting(client, server, "SELECT * from tpch.sf103.orders");
        runToQueued(client, server, "SELECT * from tpch.sf104.orders");

        ClusterStats clusterStats = getClusterStats(true, useThriftEncoding, thriftProtocol);
        assertNotNull(clusterStats);
        assertEquals(clusterStats.getRunningQueries(), 3);
        assertEquals(clusterStats.getQueuedQueries(), 1);
        assertEquals(clusterStats.getAdjustedQueueSize(), 0);
    }

    @Test(timeOut = 60_000, dataProvider = "thriftEncodingToggle", enabled = false)
    public void testGetClusterStats(boolean useThriftEncoding, Protocol thriftProtocol)
            throws Exception
    {
        runToExecuting(client, server, "SELECT * from tpch.sf100.orders");

        // Sleep to allow query to make some progress
        sleep(SECONDS.toMillis(5));

        ClusterStats clusterStats = getClusterStats(true, useThriftEncoding, thriftProtocol);

        assertNotNull(clusterStats);
        assertEquals(clusterStats.getActiveWorkers(), 4);
        assertEquals(clusterStats.getRunningTasks(), 5);
        assertTrue(clusterStats.getRunningDrivers() > 0);
        assertEquals(clusterStats.getRunningQueries(), 1);
        assertEquals(clusterStats.getBlockedQueries(), 0);
        assertEquals(clusterStats.getQueuedQueries(), 0);
    }

    private ClusterStats getClusterStats(boolean followRedirects, boolean useThriftEncoding, Protocol thriftProtocol)
    {
        Request.Builder builder = useThriftEncoding ? ThriftRequestUtils.prepareThriftGet(thriftProtocol) : prepareGet();
        Request request = builder
                .setHeader(PRESTO_USER, "user")
                .setUri(uriBuilderFrom(server.getBaseUrl().resolve("/v1/cluster")).build())
                .setFollowRedirects(followRedirects)
                .build();
        if (useThriftEncoding) {
            ThriftCodecManager codecManager = new ThriftCodecManager();
            return client.execute(request, new ThriftResponseHandler<>(codecManager.getCodec(ClusterStats.class))).getValue();
        }
        else {
            return client.execute(request, createJsonResponseHandler(jsonCodec(ClusterStats.class)));
        }
    }

    private String getResourceFilePath(String fileName)
    {
        return this.getClass().getClassLoader().getResource(fileName).getPath();
    }
}
