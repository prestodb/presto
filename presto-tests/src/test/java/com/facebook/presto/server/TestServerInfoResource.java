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
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

import java.net.URI;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunnerWithNoClusterReadyCheck;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestServerInfoResource
{
    private HttpClient client;
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.client = new JettyHttpClient();
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        closeQuietly(client);
        client = null;
    }

    @AfterMethod
    public void serverTearDown()
    {
        for (TestingPrestoServer server : queryRunner.getServers()) {
            closeQuietly(server);
        }
    }

    @BeforeGroups("createQueryRunner")
    public void createQueryRunnerSetup()
            throws Exception
    {
        queryRunner = createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.of("cluster.required-resource-managers-active", "1", "cluster.required-coordinators-active", "1"),
                ImmutableMap.of("query.client.timeout", "10s"), 2);
    }

    @Test(timeOut = 30_000, groups = {"createQueryRunner"})
    public void testGetServerStateWithRequiredResourceManagerCoordinators()
    {
        TestingPrestoServer server = queryRunner.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/state")).build();
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .build();
        NodeState state = client.execute(request, createJsonResponseHandler(jsonCodec(NodeState.class)));
        assertEquals(state, NodeState.ACTIVE);
    }

    @BeforeGroups("getServerStateWithoutRequiredResourceManagers")
    public void createQueryRunnerWithNoClusterReadyCheckSetup()
            throws Exception
    {
        queryRunner = createQueryRunnerWithNoClusterReadyCheck(
                ImmutableMap.of(),
                ImmutableMap.of("cluster.required-resource-managers-active", "2", "cluster.required-coordinators-active", "1"),
                ImmutableMap.of("query.client.timeout", "10s"), 2);
    }

    @Test(timeOut = 30_000, groups = {"getServerStateWithoutRequiredResourceManagers"})
    public void testGetServerStateWithoutRequiredResourceManagers()
    {
        TestingPrestoServer server = queryRunner.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/state")).build();
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .build();
        NodeState state = client.execute(request, createJsonResponseHandler(jsonCodec(NodeState.class)));
        assertEquals(state, NodeState.INACTIVE);
    }

    @BeforeGroups("getServerStateWithoutRequiredCoordinators")
    public void getServerStateWithoutRequiredCoordinatorsSetup()
            throws Exception
    {
        queryRunner = createQueryRunnerWithNoClusterReadyCheck(
                ImmutableMap.of(),
                ImmutableMap.of("cluster.required-resource-managers-active", "1", "cluster.required-coordinators-active", "3"),
                ImmutableMap.of("query.client.timeout", "10s"), 2);
    }

    @Test(timeOut = 30_000, groups = {"getServerStateWithoutRequiredCoordinators"})
    public void testGetServerStateWithoutRequiredCoordinators()
    {
        TestingPrestoServer server = queryRunner.getCoordinator(0);
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/info/state")).build();
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .build();
        NodeState state = client.execute(request, createJsonResponseHandler(jsonCodec(NodeState.class)));
        assertEquals(state, NodeState.INACTIVE);
    }
}
