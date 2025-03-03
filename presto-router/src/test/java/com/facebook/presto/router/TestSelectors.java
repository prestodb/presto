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
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logging;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.RequestInfo;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.testing.assertions.Assert;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.servlet.http.HttpServletRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_TAGS;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestSelectors
{
    private List<TestingPrestoServer> prestoServers;
    private ClusterManager clusterManager;
    private LifeCycleManager lifeCycleManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        String configTemplate = new String(
                Files.readAllBytes(Paths.get(this.getClass().getClassLoader().getResource("selector-test-router-template.json").getPath())));

        File configFile = File.createTempFile("router", "json");

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

        for (TestingPrestoServer s : prestoServers) {
            configTemplate = configTemplate.replaceFirst("\\$\\{SERVERS}", String.format("[ \"%s\" ]", s.getBaseUrl().toString()));
        }

        FileOutputStream fileOutputStream = new FileOutputStream(configFile);
        fileOutputStream.write(configTemplate.getBytes(UTF_8));

        fileOutputStream.close();

        Bootstrap app = new Bootstrap(ImmutableList.<Module>builder()
                .add(new TestingNodeModule())
                .add(new TestingHttpServerModule())
                .add(new JsonModule())
                .add(new JaxrsModule(true))
                .add(new ServerSecurityModule())
                .add(new RouterModule())
                .build());

        Injector injector = app.doNotInitializeLogging().setRequiredConfigurationProperty("router.config-file", configFile.getAbsolutePath()).quiet().initialize();
        clusterManager = injector.getInstance(ClusterManager.class);
        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        for (TestingPrestoServer prestoServer : prestoServers) {
            prestoServer.close();
        }
        lifeCycleManager.stop();
    }

    @Test
    public void testSelectorRules()
    {
        synchronized (prestoServers) {
            String[][] headers = {
                    {"user1", "source1", "[tag1]"},
                    {"user2", "source2", "[tag2]"},
                    {"user3", "source3", "[tag3]"},
            };

            int groupIndex = 0;
            for (String[] header : headers) {
                Optional<URI> destinationWrapper = getDestinationWrapper(header[0], header[1], header[2]);
                if (destinationWrapper.isPresent()) {
                    URI destination = destinationWrapper.get();
                    assertEquals(destination.getPort(), prestoServers.get(groupIndex).getBaseUrl().getPort());
                }
                else {
                    Assert.fail();
                }
                groupIndex++;
            }
        }
    }

    @Test
    public void testMissingSelectorRules()
    {
        synchronized (prestoServers) {
            String[][] headers = {
                    {"NA", "source4", "[]"},
                    {"NA", "source5", "[]"},
                    {"NA", "source6", "[]"},
            };

            int groupIndex = 3;
            for (String[] header : headers) {
                Optional<URI> destinationWrapper = getDestinationWrapper(header[0], header[1], header[2]);
                if (destinationWrapper.isPresent()) {
                    URI destination = destinationWrapper.get();
                    assertEquals(destination.getPort(), prestoServers.get(groupIndex).getBaseUrl().getPort());
                }
                else {
                    Assert.fail();
                }
                groupIndex++;
            }
        }
    }

    private Optional<URI> getDestinationWrapper(String user, String source, String clientTags)
    {
        HttpServletRequest request = new MockRouterHttpServletRequest(
                ImmutableListMultimap.<String, String>builder()
                        .put(PRESTO_USER, user)
                        .put(PRESTO_SOURCE, source)
                        .put(PRESTO_CLIENT_TAGS, clientTags)
                        .build(),
                "testRemote",
                ImmutableMap.of());
        return clusterManager.getDestination(new RequestInfo(request, ""));
    }
}
