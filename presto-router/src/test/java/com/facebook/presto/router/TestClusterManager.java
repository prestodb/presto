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
import com.facebook.airlift.http.server.HttpServerInfo;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.log.Logging;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.jdbc.PrestoResultSet;
import com.facebook.presto.router.cluster.ClusterManager;
import com.facebook.presto.router.cluster.ClusterManager.ClusterStatusTracker;
import com.facebook.presto.router.cluster.RemoteInfoFactory;
import com.facebook.presto.router.cluster.RemoteStateConfig;
import com.facebook.presto.router.spec.RouterSpec;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.router.TestingRouterUtil.getConfigFile;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestClusterManager
{
    private static final int NUM_CLUSTERS = 3;
    private static final int NUM_QUERIES = 7;

    private List<TestingPrestoServer> prestoServers;
    private LifeCycleManager lifeCycleManager;
    private HttpServerInfo httpServerInfo;
    private ClusterStatusTracker clusterStatusTracker;
    private File configFile;
    private RemoteInfoFactory remoteInfoFactory;
    private RemoteStateConfig remoteStateConfig;

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
        Path tempFile = Files.createTempFile("temp-config", ".json");
        configFile = getConfigFile(prestoServers, tempFile.toFile());

        Bootstrap app = new Bootstrap(
                new TestingNodeModule("test"),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(true),
                new RouterModule());

        Injector injector = app.doNotInitializeLogging()
                .setRequiredConfigurationProperty("router.config-file", configFile.getAbsolutePath())
                .quiet().initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        httpServerInfo = injector.getInstance(HttpServerInfo.class);
        clusterStatusTracker = injector.getInstance(ClusterStatusTracker.class);

        // Store dependencies for later use
        remoteInfoFactory = injector.getInstance(RemoteInfoFactory.class);
        remoteStateConfig = injector.getInstance(RemoteStateConfig.class);
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

    public void testQuery()
            throws Exception
    {
        String sql = "SELECT row_number() OVER () n FROM tpch.tiny.orders";
        for (int i = 0; i < NUM_QUERIES; ++i) {
            try (Connection connection = createConnection(httpServerInfo.getHttpUri());
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                long count = 0;
                long sum = 0;
                while (rs.next()) {
                    count++;
                    sum += rs.getLong("n");
                }
                assertEquals(count, 15000);
                assertEquals(sum, (count / 2) * (1 + count));
            }
        }

        sleepUninterruptibly(10, SECONDS);
        assertEquals(clusterStatusTracker.getAllQueryInfos().size(), NUM_QUERIES);
        assertQueryState();
    }

    public void testConfigReload()
            throws IOException, InterruptedException, BrokenBarrierException, TimeoutException
    {
        Path tempDirPath = Files.createTempDirectory("tempdir");
        Path tempFile = Files.createTempFile(tempDirPath, "new-config", ".json");
        File newConfig = getConfigFile(prestoServers, tempFile.toFile());
        RouterConfig newRouterConfig = new RouterConfig();
        newRouterConfig.setConfigFile(newConfig.getAbsolutePath());

        CyclicBarrier barrier = new CyclicBarrier(2);
        ClusterManager barrierClusterManager = new BarrierClusterManager(newRouterConfig, remoteInfoFactory, barrier, lifeCycleManager);
        assertEquals(barrierClusterManager.getAllClusters().size(), 3);

        while (!barrierClusterManager.getIsWatchServiceStarted()) {
            Thread.sleep(10);
        }

        Path configFilePath = newConfig.toPath();
        String originalConfigContent = new String(Files.readAllBytes(configFilePath));

        JsonCodec<RouterSpec> jsonCodec = JsonCodec.jsonCodec(RouterSpec.class);
        RouterSpec spec = jsonCodec.fromJson(originalConfigContent);
        RouterSpec newSpec = new RouterSpec(ImmutableList.of(), spec.getSelectors(), Optional.ofNullable(spec.getSchedulerType()), spec.getPredictorUri());

        Files.write(newConfig.toPath(), jsonCodec.toBytes(newSpec));
        barrier.await(10, SECONDS);

        assertEquals(barrierClusterManager.getAllClusters().size(), 0);

        Files.write(newConfig.toPath(), originalConfigContent.getBytes());
        barrier.await(10, SECONDS);

        assertEquals(barrierClusterManager.getAllClusters().size(), 3);
    }

    private void assertQueryState()
            throws SQLException
    {
        String sql = "SELECT query_id, state FROM system.runtime.queries";
        int total = 0;
        for (TestingPrestoServer server : prestoServers) {
            try (Connection connection = createConnection(server.getBaseUrl());
                    Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(sql)) {
                String id = rs.unwrap(PrestoResultSet.class).getQueryId();
                int count = 0;
                while (rs.next()) {
                    if (!rs.getString("query_id").equals(id)) {
                        assertEquals(QueryState.valueOf(rs.getString("state")), QueryState.FINISHED);
                        count++;
                    }
                }
                assertTrue(count > 0);
                total += count;
            }
        }
        assertEquals(total, NUM_QUERIES);
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

    private static Connection createConnection(URI uri)
            throws SQLException
    {
        String url = format("jdbc:presto://%s:%s", uri.getHost(), uri.getPort());
        return DriverManager.getConnection(url, "test", null);
    }
}
