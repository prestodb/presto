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
import com.facebook.airlift.http.server.HttpServerInfo;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logging;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.router.scheduler.CustomSchedulerManager;
import com.facebook.presto.router.security.RouterSecurityModule;
import com.facebook.presto.router.spec.GroupSpec;
import com.facebook.presto.router.spec.RouterSpec;
import com.facebook.presto.router.spec.SelectorRuleSpec;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.router.RouterRequestInfo;
import com.facebook.presto.spi.router.Scheduler;
import com.facebook.presto.spi.router.SchedulerFactory;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.router.TestClusterManager.runAndAssertQueryResults;
import static com.facebook.presto.router.scheduler.SchedulerType.CUSTOM_PLUGIN_SCHEDULER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCustomSchedulerManager
{
    private static final int NUM_CLUSTERS = 3;
    private static List<TestingPrestoServer> prestoServers;
    private HttpServerInfo httpServerInfo;
    private File configFile;
    private CustomSchedulerManager schedulerManager;
    private static List<URI> serverURIs;
    private static Path tempPluginSchedulerConfigFile;
    @BeforeClass
    public void setup()
            throws Exception
    {
        Logging.initialize();

        // Set up servers
        ImmutableList.Builder<TestingPrestoServer> builder = ImmutableList.builder();
        for (int i = 0; i < NUM_CLUSTERS; ++i) {
            builder.add(createPrestoServer());
        }
        prestoServers = builder.build();

        configFile = getRouterConfigFile(prestoServers);

        tempPluginSchedulerConfigFile = Files.createTempFile("router-scheduler-mock", ".properties");
        String schedulerName = "router-scheduler.name=mock";
        Files.write(tempPluginSchedulerConfigFile, schedulerName.getBytes());

        SchedulerFactory mockSchedulerFactory = new MockSchedulerFactory();
        CustomSchedulerManager customSchedulerManager = new CustomSchedulerManager(ImmutableList.of(mockSchedulerFactory), tempPluginSchedulerConfigFile.toFile());

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new RouterSecurityModule(),
                new RouterModule(Optional.of(customSchedulerManager)));

        Injector injector = app.doNotInitializeLogging()
                .setRequiredConfigurationProperty("router.config-file", configFile.getAbsolutePath())
                .setRequiredConfigurationProperty("presto.version", "test")
                .quiet().initialize();

        httpServerInfo = injector.getInstance(HttpServerInfo.class);
        schedulerManager = injector.getInstance(CustomSchedulerManager.class);
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
            throws Exception
    {
        for (TestingPrestoServer prestoServer : prestoServers) {
            prestoServer.close();
        }
    }

    static class MockSchedulerFactory
            implements SchedulerFactory
    {
        public static final String MOCK = "mock";

        @Override
        public String getName()
        {
            return MOCK;
        }

        @Override
        public Scheduler create(Map<String, String> config)
        {
            return new MockScheduler();
        }
    }

    public static class MockScheduler
            implements Scheduler
    {
        private List<URI> candidates;
        private int requestsMade;

        @Override
        public Optional<URI> getDestination(RouterRequestInfo routerRequestInfo)
        {
            ++requestsMade;
            return Optional.of(candidates.get(0));
        }

        @Override
        public void setCandidates(List<URI> candidates)
        {
            this.candidates = candidates;
        }

        public int getRequestsMade()
        {
            return requestsMade;
        }
    }

    public static File getRouterConfigFile(List<TestingPrestoServer> servers)
            throws IOException
    {
        Path path = Files.createTempFile("temp-config", ".json");
        serverURIs = servers.stream()
                .map(TestingPrestoServer::getBaseUrl)
                .collect(Collectors.toList());
        RouterSpec spec = new RouterSpec(ImmutableList.of(new GroupSpec("all", serverURIs, Optional.empty(), Optional.empty())),
                ImmutableList.of(new SelectorRuleSpec(Optional.empty(), Optional.empty(), Optional.empty(), "all")),
                Optional.of(CUSTOM_PLUGIN_SCHEDULER), Optional.empty(), Optional.empty());
        JsonCodec<RouterSpec> codec = jsonCodec(RouterSpec.class);
        Files.write(path, codec.toBytes(spec));
        return path.toFile();
    }

    private static TestingPrestoServer createPrestoServer()
            throws Exception
    {
        TestingPrestoServer server = new TestingPrestoServer();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        server.refreshNodes();

        return server;
    }

    public void testQuery()
            throws Exception
    {
        runAndAssertQueryResults(httpServerInfo.getHttpUri());

        MockScheduler mockScheduler = (MockScheduler) schedulerManager.getScheduler();
        assertEquals(mockScheduler.getRequestsMade(), 1);
    }

    @Test
    public void testCustomScheduler()
            throws Exception
    {
        schedulerManager.loadScheduler();
        Scheduler scheduler = schedulerManager.getScheduler();
        scheduler.setCandidates(serverURIs);

        URI target = scheduler.getDestination(new RouterRequestInfo("test")).orElse(new URI("invalid"));
        assertTrue(serverURIs.contains(target));
    }

    @DataProvider(name = "schedulerNameProvider")
    public Object[][] schedulerNameProvider()
    {
        return new Object[][]{
                {"router-scheduler.name=RANDOM"},
                {"router-scheduler.name=METRICS"},
                {"router-scheduler.name=MOCK"}
        };
    }

    @Test(dataProvider = "schedulerNameProvider", expectedExceptions = IllegalStateException.class,
            expectedExceptionsMessageRegExp = "Presto Router Scheduler.*is not registered")
    public void testCustomSchedulerFail(String schedulerName)
            throws Exception
    {
        Files.write(tempPluginSchedulerConfigFile, schedulerName.getBytes());
        schedulerManager.loadScheduler();
    }
}
