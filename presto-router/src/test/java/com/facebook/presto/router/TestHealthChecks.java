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
package com.facebook.presto.router;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logging;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.ClientRequestFilterModule;
import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.RemoteClusterInfo;
import com.facebook.presto.router.cluster.RequestInfo;
import com.facebook.presto.server.MockHttpServletRequest;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import jakarta.servlet.http.HttpServletRequest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static com.facebook.presto.router.TestingRouterUtil.getConfigFile;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Thread.sleep;
import static java.time.Instant.now;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHealthChecks
{
    private List<TestingPrestoServer> prestoServers;
    private ClusterManager clusterManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        File tempFile = File.createTempFile("router", ".json");

        Logging.initialize();

        // set up server
        ImmutableList.Builder<TestingPrestoServer> builder = ImmutableList.builder();
        for (int i = 0; i < 3; ++i) {
            TestingPrestoServer server = new TestingPrestoServer();
            server.installPlugin(new TpchPlugin());
            server.createCatalog("tpch", "tpch");
            server.refreshNodes();
            builder.add(server);
        }

        prestoServers = builder.build();
        getConfigFile(prestoServers, tempFile);

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(), new JsonModule(),
                new JaxrsModule(true),
                new ServerSecurityModule(),
                new ClientRequestFilterModule(),
                new RouterModule(Optional.empty()));

        Injector injector = app.doNotInitializeLogging()
                .setRequiredConfigurationProperty("router.config-file", tempFile.getAbsolutePath())
                .setOptionalConfigurationProperty("presto.version", "test")
                .setOptionalConfigurationProperty("router.remote-state.cluster-unhealthy-timeout", "4s")
                .setOptionalConfigurationProperty("router.remote-state.polling-interval", "0.5s")
                .initialize();
        clusterManager = injector.getInstance(ClusterManager.class);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        for (TestingPrestoServer prestoServer : prestoServers) {
            prestoServer.close();
        }
    }

    static void waitUntil(Supplier<Boolean> condition, int value, TimeUnit unit)
            throws TimeoutException, InterruptedException
    {
        checkArgument(value > 0, "timeout value must be greater than 0");
        Instant start = now();
        long timeoutMillis = unit.toMillis(value);
        long sleepMillis = Math.min(timeoutMillis / 10, 50);
        Instant deadline = start.plusMillis(timeoutMillis);
        while (true) {
            if (condition.get()) {
                return;
            }
            if (now().isAfter(deadline)) {
                throw new TimeoutException();
            }
            sleep(sleepMillis);
        }
    }

    @Test
    public void testHealthChecks()
            throws InterruptedException, TimeoutException
    {
        TestingPrestoServer server0 = prestoServers.get(0);
        TestingPrestoServer server1 = prestoServers.get(1);
        TestingPrestoServer server2 = prestoServers.get(2);

        List<URI> healthyDestinations = getDestinations(3);
        assertTrue(healthyDestinations.contains(server0.getBaseUrl()));
        assertTrue(healthyDestinations.contains(server1.getBaseUrl()));
        assertTrue(healthyDestinations.contains(server2.getBaseUrl()));

        server0.stopResponding();
        waitUntil(() ->
                !clusterManager.getRemoteClusterInfos().get(server0.getBaseUrl()).isHealthy()
                        && clusterManager.getRemoteClusterInfos().get(server1.getBaseUrl()).isHealthy()
                        && clusterManager.getRemoteClusterInfos().get(server2.getBaseUrl()).isHealthy(), 2, MINUTES);

        healthyDestinations = getDestinations(3);
        assertFalse(healthyDestinations.contains(server0.getBaseUrl()));
        assertTrue(healthyDestinations.contains(server1.getBaseUrl()));
        assertTrue(healthyDestinations.contains(server2.getBaseUrl()));

        server0.startResponding();
        waitUntil(() -> prestoServers.stream()
                        .map(TestingPrestoServer::getBaseUrl)
                        .map(clusterManager.getRemoteClusterInfos()::get)
                        .allMatch(RemoteClusterInfo::isHealthy),
                2, MINUTES);

        healthyDestinations = getDestinations(3);
        assertTrue(healthyDestinations.contains(server0.getBaseUrl()));
        assertTrue(healthyDestinations.contains(server1.getBaseUrl()));
        assertTrue(healthyDestinations.contains(server2.getBaseUrl()));
    }

    private List<URI> getDestinations(int requests)
    {
        List<URI> destinations = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            destinations.add(getDestinationWrapper().orElse(null));
        }
        return destinations;
    }

    private Optional<URI> getDestinationWrapper()
    {
        HttpServletRequest request = new MockHttpServletRequest(
                ImmutableListMultimap.of(),
                "testRemote",
                ImmutableMap.of());
        return clusterManager.getDestination(new RequestInfo(request, ""));
    }
}
