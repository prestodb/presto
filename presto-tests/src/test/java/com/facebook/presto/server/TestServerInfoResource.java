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
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.spi.NodeStats;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.server.RequestHelpers.getJsonTransportBuilder;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunnerWithNoClusterReadyCheck;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true, groups = "TestServerInfoResource")
public class TestServerInfoResource
{
    private HttpClient client;
    private DistributedQueryRunner queryRunnerActive;
    private DistributedQueryRunner queryRunnerInactiveResourceManagers;
    private DistributedQueryRunner queryRunnerInactiveCoordinators;
    private DistributedQueryRunner queryRunnerInactiveResourceGroup;
    private ThriftCodecManager thriftCodeManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.client = new JettyHttpClient();
        this.thriftCodeManager = new ThriftCodecManager();
    }

    @DataProvider
    public Object[][] thriftEncodingToggle()
    {
        return new Object[][] {{true, Protocol.BINARY}, {true, Protocol.COMPACT}, {true, Protocol.FB_COMPACT}, {false, null}};
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(client);
        this.client = null;
    }

    @AfterClass(alwaysRun = true)
    public void serverTearDown()
    {
        if (queryRunnerActive != null) {
            for (TestingPrestoServer server : queryRunnerActive.getServers()) {
                closeQuietly(server);
            }
        }
        if (queryRunnerInactiveResourceManagers != null) {
            for (TestingPrestoServer server : queryRunnerInactiveResourceManagers.getServers()) {
                closeQuietly(server);
            }
        }
        if (queryRunnerInactiveCoordinators != null) {
            for (TestingPrestoServer server : queryRunnerInactiveCoordinators.getServers()) {
                closeQuietly(server);
            }
        }
        if (queryRunnerInactiveResourceGroup != null) {
            for (TestingPrestoServer server : queryRunnerInactiveResourceGroup.getServers()) {
                closeQuietly(server);
            }
        }
    }

    @BeforeGroups("createQueryRunner")
    public void createQueryRunnerSetup()
            throws Exception
    {
        queryRunnerActive = createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of("cluster.required-resource-managers-active", "1", "cluster.required-coordinators-active", "1"),
                ImmutableMap.of("query.client.timeout", "10s"), 2);
    }

    @Test(timeOut = 30_000, groups = {"createQueryRunner"}, dataProvider = "thriftEncodingToggle")
    public void testGetServerStateWithRequiredResourceManagerCoordinators(boolean useThriftEncoding, Protocol thriftProtocol)
    {
        TestingPrestoServer server = queryRunnerActive.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/state")).build();
        NodeState state = getNodeState(uri, useThriftEncoding, thriftProtocol);
        assertEquals(state, NodeState.ACTIVE);
    }

    @Test(timeOut = 30_000, groups = {"createQueryRunner"}, dataProvider = "thriftEncodingToggle", dependsOnMethods = "testGetServerStateWithRequiredResourceManagerCoordinators")
    public void testGetServerStatsWithRequiredResourceManagerCoordinators(boolean useThriftEncoding, Protocol thriftProtocol)
    {
        TestingPrestoServer server = queryRunnerActive.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/stats")).build();
        NodeStats stats = getNodeStats(uri, useThriftEncoding, thriftProtocol);
        assertEquals(stats.getNodeState(), NodeState.ACTIVE);
        assertEquals(stats.getLoadMetrics().isPresent(), false);
    }

    @BeforeGroups("getServerStateWithoutRequiredResourceManagers")
    public void createQueryRunnerWithNoClusterReadyCheckSetup()
            throws Exception
    {
        queryRunnerInactiveResourceManagers = createQueryRunnerWithNoClusterReadyCheck(
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableMap.of("cluster.required-resource-managers-active", "2", "cluster.required-coordinators-active", "1"),
                        ImmutableMap.of("query.client.timeout", "10s"), 2, false);
    }

    @Test(timeOut = 30_000, groups = {"getServerStateWithoutRequiredResourceManagers"}, dataProvider = "thriftEncodingToggle")
    public void testGetServerStateWithoutRequiredResourceManagers(boolean useThriftEncoding, Protocol thriftProtocol)
    {
        TestingPrestoServer server = queryRunnerInactiveResourceManagers.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/state")).build();
        NodeState state = getNodeState(uri, useThriftEncoding, thriftProtocol);
        assertEquals(state, NodeState.INACTIVE);
    }

    @Test(timeOut = 30_000, groups = {"getServerStateWithoutRequiredResourceManagers"}, dataProvider = "thriftEncodingToggle", dependsOnMethods = "testGetServerStateWithoutRequiredResourceManagers")
    public void testGetServerStatsWithoutRequiredResourceManagers(boolean useThriftEncoding, Protocol thriftProtocol)
    {
        TestingPrestoServer server = queryRunnerInactiveResourceManagers.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/stats")).build();
        NodeStats stats = getNodeStats(uri, useThriftEncoding, thriftProtocol);
        assertEquals(stats.getNodeState(), NodeState.INACTIVE);
        assertEquals(stats.getLoadMetrics().isPresent(), false);
    }

    @BeforeGroups("getServerStateWithoutRequiredCoordinators")
    public void getServerStateWithoutRequiredCoordinatorsSetup()
            throws Exception
    {
        queryRunnerInactiveCoordinators = createQueryRunnerWithNoClusterReadyCheck(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of("cluster.required-resource-managers-active", "1", "cluster.required-coordinators-active", "3"),
                ImmutableMap.of("query.client.timeout", "10s"), 2, false);
    }

    @Test(timeOut = 30_000, groups = {"getServerStateWithoutRequiredCoordinators"}, dataProvider = "thriftEncodingToggle")
    public void testGetServerStateWithoutRequiredCoordinators(boolean useThriftEncoding, Protocol thriftProtocol)
    {
        TestingPrestoServer server = queryRunnerInactiveCoordinators.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/state")).build();
        NodeState state = getNodeState(uri, useThriftEncoding, thriftProtocol);

        assertEquals(state, NodeState.INACTIVE);
    }

    @Test(timeOut = 30_000, groups = {"getServerStateWithoutRequiredCoordinators"}, dataProvider = "thriftEncodingToggle", dependsOnMethods = "testGetServerStateWithoutRequiredCoordinators")
    public void testGetServerStatsWithoutRequiredCoordinators(boolean useThriftEncoding, Protocol thriftProtocol)
    {
        TestingPrestoServer server = queryRunnerInactiveCoordinators.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/stats")).build();
        NodeStats stats = getNodeStats(uri, useThriftEncoding, thriftProtocol);
        assertEquals(stats.getNodeState(), NodeState.INACTIVE);
        assertEquals(stats.getLoadMetrics().isPresent(), false);
    }

    @BeforeGroups("createQueryRunnerWithNoClusterReadyCheckSkipLoadingResourceGroupConfigurationManager")
    public void createQueryRunnerWithNoClusterReadyCheckSkipLoadingResourceGroupConfigurationManager()
            throws Exception
    {
        queryRunnerInactiveResourceGroup = createQueryRunnerWithNoClusterReadyCheck(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of("cluster.required-resource-managers-active", "1", "cluster.required-coordinators-active", "1"),
                ImmutableMap.of("query.client.timeout", "10s"), 2, true);
    }

    @Test(groups = {"createQueryRunnerWithNoClusterReadyCheckSkipLoadingResourceGroupConfigurationManager"})
    public void testGetServerStateWhenResourceGroupConfigurationManagerNotLoaded()
            throws Exception
    {
        TestingPrestoServer server = queryRunnerInactiveResourceGroup.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/state")).build();
        NodeState state = getNodeState(uri, false, null);
        assertEquals(state, NodeState.INACTIVE);
    }

    @Test(groups = {"createQueryRunnerWithNoClusterReadyCheckSkipLoadingResourceGroupConfigurationManager"}, dependsOnMethods = "testGetServerStateWhenResourceGroupConfigurationManagerNotLoaded")
    public void testGetServerStatsWhenResourceGroupConfigurationManagerNotLoaded()
    {
        TestingPrestoServer server = queryRunnerInactiveResourceGroup.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/stats")).build();
        NodeStats stats = getNodeStats(uri, false, null);
        assertEquals(stats.getNodeState(), NodeState.INACTIVE);
        assertEquals(stats.getLoadMetrics().isPresent(), false);
    }

    private NodeState getNodeState(URI uri, boolean useThriftEncoding, Protocol thriftProtocol)
    {
        Request.Builder requestBuilder = useThriftEncoding ? ThriftRequestUtils.prepareThriftGet(thriftProtocol) : getJsonTransportBuilder(prepareGet());
        Request request = requestBuilder
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .build();
        if (useThriftEncoding) {
            return client.execute(request, new ThriftResponseHandler<>(thriftCodeManager.getCodec(NodeState.class))).getValue();
        }
        else {
            return client.execute(request, createJsonResponseHandler(jsonCodec(NodeState.class)));
        }
    }

    private NodeStats getNodeStats(URI uri, boolean useThriftEncoding, Protocol thriftProtocol)
    {
        Request.Builder requestBuilder = useThriftEncoding ? ThriftRequestUtils.prepareThriftGet(thriftProtocol) : getJsonTransportBuilder(prepareGet());
        Request request = requestBuilder
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .build();
        if (useThriftEncoding) {
            return client.execute(request, new ThriftResponseHandler<>(thriftCodeManager.getCodec(NodeStats.class))).getValue();
        }
        else {
            return client.execute(request, createJsonResponseHandler(jsonCodec(NodeStats.class)));
        }
    }
}
