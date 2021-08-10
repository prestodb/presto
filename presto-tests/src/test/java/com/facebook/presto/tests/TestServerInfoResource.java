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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.server.ServerInfoResource;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.NodeState;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;

import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestServerInfoResource
{
    private static final long SHUTDOWN_TIMEOUT_MILLIS = 240_000;
    private static final Session TINY_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .build();
    private static final String COORDINATOR = "coordinator";

    private ListeningExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = MoreExecutors.listeningDecorator(newCachedThreadPool());
    }

    @AfterClass(alwaysRun = true)
    public void shutdown()
    {
        executor.shutdownNow();
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testServerActive()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, ImmutableMap.of())) {
            TestingPrestoServer coordinator = queryRunner.getServers()
                    .stream()
                    .filter(TestingPrestoServer::isCoordinator)
                    .findFirst()
                    .get();

            ServerInfoResource serverInfoResource = coordinator.getServerInfoResource();
            NodeState nodeState = serverInfoResource.getServerState();
            assertTrue(nodeState == NodeState.ACTIVE);
        }
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testServerInactiveThenActive()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, ImmutableMap.of())) {
            TestingPrestoServer coordinator = queryRunner.getServers()
                    .stream()
                    .filter(TestingPrestoServer::isCoordinator)
                    .findFirst()
                    .get();

            ServerInfoResource serverInfoResource = coordinator.getServerInfoResource();
            Response response = serverInfoResource.updateState(NodeState.INACTIVE);
            assertEquals(response.getStatus(), 200);
            NodeState nodeState = serverInfoResource.getServerState();
            assertTrue(nodeState == NodeState.INACTIVE);
            response = serverInfoResource.updateState(NodeState.ACTIVE);
            assertEquals(response.getStatus(), 200);
            nodeState = serverInfoResource.getServerState();
            assertTrue(nodeState == NodeState.ACTIVE);
        }
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testServerShutdown()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, ImmutableMap.of())) {
            TestingPrestoServer coordinator = queryRunner.getServers()
                    .stream()
                    .filter(TestingPrestoServer::isCoordinator)
                    .findFirst()
                    .get();

            ServerInfoResource serverInfoResource = coordinator.getServerInfoResource();
            Response response = serverInfoResource.updateState(NodeState.SHUTTING_DOWN);
            assertEquals(response.getStatus(), 200);
            NodeState nodeState = serverInfoResource.getServerState();
            assertTrue(nodeState == NodeState.SHUTTING_DOWN);
        }
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testServerShutdownFollowedByActive()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, ImmutableMap.of())) {
            TestingPrestoServer coordinator = queryRunner.getServers()
                    .stream()
                    .filter(TestingPrestoServer::isCoordinator)
                    .findFirst()
                    .get();

            ServerInfoResource serverInfoResource = coordinator.getServerInfoResource();
            serverInfoResource.updateState(NodeState.SHUTTING_DOWN);
            Response response = serverInfoResource.updateState(NodeState.ACTIVE);
            assertEquals(response.getStatus(), BAD_REQUEST.getStatusCode());
            assertEquals(response.getEntity(), "Cluster is shutting down");
        }
    }

    @Test(timeOut = SHUTDOWN_TIMEOUT_MILLIS)
    public void testServerShutdownFollowedByInactive()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(TINY_SESSION, ImmutableMap.of())) {
            TestingPrestoServer coordinator = queryRunner.getServers()
                    .stream()
                    .filter(TestingPrestoServer::isCoordinator)
                    .findFirst()
                    .get();

            ServerInfoResource serverInfoResource = coordinator.getServerInfoResource();
            serverInfoResource.updateState(NodeState.SHUTTING_DOWN);
            Response response = serverInfoResource.updateState(NodeState.INACTIVE);
            assertEquals(response.getStatus(), BAD_REQUEST.getStatusCode());
            assertEquals(response.getEntity(), "Cluster is shutting down");
        }
    }

    public static DistributedQueryRunner createQueryRunner(Session session, Map<String, String> properties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(session, 2, properties);

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
}
