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
import com.facebook.presto.server.testing.TestingPrestoServer;
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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertFalse;

public class TestHealthChecks
    {
        private List<TestingPrestoServer> prestoServers;
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

            Bootstrap app = new Bootstrap(
                    new TestingNodeModule("test"),
                    new TestingHttpServerModule(), new JsonModule(),
                    new JaxrsModule(true),
                    new ServerSecurityModule(),
                    new RouterModule());

            Injector injector = app.doNotInitializeLogging().setRequiredConfigurationProperty("router.config-file", configFile.getAbsolutePath()).initialize();
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

        @Test(enabled = false)
        public void testHealthChecks()
        {
            prestoServers.get(0).stopResponding();
            List<URI> destinations = new ArrayList();
            for (int i = 0; i < 3; i++) {
               destinations.add(getDestinationWrapper().orElse(URI.create("null")));
            }
            assertFalse(destinations.contains(prestoServers.get(0).getBaseUrl()));
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
