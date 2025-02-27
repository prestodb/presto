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
import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.RequestInfo;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHealthChecks
{
    private List<TestingPrestoServerRouter> prestoServers;
    private ClusterManager clusterManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        String configTemplate = new String(
                Files.readAllBytes(Paths.get(this.getClass().getClassLoader().getResource("health-check-test-router-template.json").getPath())));

        File configFile = File.createTempFile("router", "json");

        Logging.initialize();

        // set up server
        ImmutableList.Builder<TestingPrestoServerRouter> builder = ImmutableList.builder();
        for (int i = 0; i < 3; ++i) {
            TestingPrestoServerRouter server = new TestingPrestoServerRouter();
            server.installPlugin(new TpchPlugin());
            server.createCatalog("tpch", "tpch");
            server.refreshNodes();
            builder.add(server);
        }

        prestoServers = builder.build();

        for (TestingPrestoServerRouter s : prestoServers) {
            configTemplate = configTemplate.replaceFirst("\\$\\{SERVERS}", String.format("\"%s\"", s.getBaseUrl().toString()));
        }

        FileOutputStream fileOutputStream = new FileOutputStream(configFile);
        fileOutputStream.write(configTemplate.getBytes(UTF_8));

        fileOutputStream.close();

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(), new JsonModule(),
                new JaxrsModule(true),
                new ServerSecurityModule(),
                new RouterModule());

        Injector injector = app.doNotInitializeLogging()
                .setRequiredConfigurationProperty("router.config-file", configFile.getAbsolutePath())
                .setRequiredConfigurationProperty("presto.version", "testversion")
                .initialize();
        clusterManager = injector.getInstance(ClusterManager.class);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        for (TestingPrestoServerRouter prestoServer : prestoServers) {
            prestoServer.close();
        }
    }

    @Test
    public void testHealthChecks()
            throws InterruptedException, IOException
    {
        clusterManager.refreshHealthStatuses();
        List<URI> destinations = getDestinations(3);
        assertTrue(destinations.contains(prestoServers.get(0).getBaseUrl()));

        prestoServers.get(0).stopResponding();
        Thread.sleep(4000);
        clusterManager.refreshHealthStatuses();
        destinations = getDestinations(3);
        assertFalse(destinations.contains(prestoServers.get(0).getBaseUrl()));

        prestoServers.get(0).startResponding();
        clusterManager.refreshHealthStatuses();
        destinations = getDestinations(3);
        assertTrue(destinations.contains(prestoServers.get(0).getBaseUrl()));
    }

    private List<URI> getDestinations(int requests)
    {
        List<URI> destinations = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            Optional<URI> destinationWrapper = getDestinationWrapper();
            if (!destinationWrapper.isPresent()) {
                destinations.add(URI.create("null"));
            }
            else {
                destinations.add(destinationWrapper.get());
            }
        }
        return destinations;
    }

    private Optional<URI> getDestinationWrapper()
    {
        HttpServletRequest request = new MockRouterHttpServletRequest(
                ImmutableListMultimap.of(),
                "testRemote",
                ImmutableMap.of());
        return clusterManager.getDestination(new RequestInfo(request, ""));
    }
}
