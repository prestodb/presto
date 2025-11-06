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
package com.facebook.presto.resourcemanager;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.event.client.EventClient;
import com.facebook.airlift.event.client.NullEventClient;
import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.node.NodeConfig;
import com.facebook.airlift.node.NodeInfo;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.resourceGroups.ResourceGroupRuntimeInfo;
import com.facebook.presto.memory.MemoryInfo;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.server.NodeStatus;
import com.facebook.presto.server.ResourceManagerResource;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.ClusterMemoryPoolInfo;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.JavaFeaturesConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.inject.Singleton;
import org.joda.time.DateTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestImplHttpResourceManagerClient
{
    ImplHttpResourceManagerClient testHttpClient;
    ResourceManagerClusterStateProvider clusterStateProvider;
    private ListeningExecutorService executor;
    private ScheduledExecutorService scheduler;
    private TestingHttpServer server;

    @BeforeMethod
    public void setUp() throws Exception
    {
        executor = listeningDecorator(newDirectExecutorService());
        scheduler = newSingleThreadScheduledExecutor();
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new JaxrsModule(),
                new TestingHttpServerModule(8080),
                new Module() {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(EventClient.class).to(NullEventClient.class);
                        jsonCodecBinder(binder).bindJsonCodec(NodeStatus.class);
                        jsonCodecBinder(binder).bindJsonCodec(BasicQueryInfo.class);
                        jsonCodecBinder(binder).bindListJsonCodec(ResourceGroupRuntimeInfo.class);
                        jsonCodecBinder(binder).bindMapJsonCodec(MemoryPoolId.class, ClusterMemoryPoolInfo.class);
                        binder.bind(ResourceManagerResource.class).in(Scopes.SINGLETON);
                        binder.bind(HttpResourceManagerClient.class).to(ImplHttpResourceManagerClient.class).in(Scopes.SINGLETON);
                        jaxrsBinder(binder).bind(ResourceManagerResource.class);
                        configBinder(binder).bindConfigDefaults(NodeConfig.class, config -> {
                            config.setEnvironment("test");
                        });
                        configBinder(binder).bindConfigDefaults(ResourceManagerConfig.class, config -> {
                            config.setNodeHeartbeatInterval(new Duration(10, SECONDS));
                            config.setResourceGroupRuntimeHeartbeatInterval(new Duration(10, SECONDS));
                            config.setResourceGroupRuntimeInfoTimeout(new Duration(10, SECONDS));
                        });
                        configBinder(binder).bindConfig(NodeConfig.class);
                        binder.bind(NodeInfo.class).in(Scopes.SINGLETON);
                        httpClientBinder(binder).bindHttpClient("resourceManager", ForResourceManager.class);
                    }

                    @Provides
                    @Singleton
                    InternalNodeManager providesInternalNodeManager()
                    {
                        InMemoryNodeManager manager = new InMemoryNodeManager();
                        manager.addNode(new ConnectorId("temp"), new InternalNode("rm", URI.create("http://127.0.0.1:8080/"), new NodeVersion("1"), false, true, false, false));
                        manager.addNode(new ConnectorId("one"), new InternalNode("coordinator", URI.create("http://fake.invalid/"), new NodeVersion("1"), true, false, false, false));
                        return manager;
                    }

                    @Provides
                    @Singleton
                    SessionPropertyManager provideSessionPropertyManager()
                    {
                        return createTestingSessionPropertyManager(
                                new SystemSessionProperties().getSessionProperties(),
                                new JavaFeaturesConfig(),
                                new NodeSpillConfig());
                    }

                    @Provides
                    @Singleton
                    @ForResourceManager
                    ListeningExecutorService provideListeningExecutorService()
                    {
                        return executor;
                    }

                    @Provides
                    @Singleton
                    ResourceManagerClusterStateProvider provideClusterStateProvider(
                            InternalNodeManager nodeManager,
                            SessionPropertyManager sessionPropertyManager)
                    {
                        return new ResourceManagerClusterStateProvider(
                                nodeManager,
                                sessionPropertyManager,
                                10,
                                Duration.valueOf("4s"),
                                Duration.valueOf("8s"),
                                Duration.valueOf("5s"),
                                Duration.valueOf("0s"),
                                Duration.valueOf("4s"),
                                true,
                                scheduler);
                    }
                    @Provides
                    @Singleton
                    @ForResourceManager
                    ScheduledExecutorService provideScheduledExecutorService()
                    {
                        return scheduler;
                    }
                });

        Injector injector = app
                .initialize();

        server = injector.getInstance(TestingHttpServer.class);
        server.start();

        testHttpClient = (ImplHttpResourceManagerClient) injector.getInstance(HttpResourceManagerClient.class);
        clusterStateProvider = injector.getInstance(ResourceManagerClusterStateProvider.class);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception
    {
        server.stop();
        executor.shutdownNow();
        scheduler.shutdownNow();
    }

    @Test
    public void testQueryHeartbeat()
    {
        testHttpClient.queryHeartbeat(Optional.empty(), "coordinator", createTestQueryInfo("temp", RUNNING), 1);
        assertEquals(clusterStateProvider.getClusterQueries().size(), 1);
    }

    @Test
    public void testNodeHeartbeat()
    {
        testHttpClient.nodeHeartbeat(Optional.empty(), createTestNodeStatus("node"));
        assertEquals(clusterStateProvider.getWorkerMemoryInfo().size(), 1);
    }

    @Test
    public void testResourceGroupRuntime()
    {
        List<ResourceGroupRuntimeInfo> runtimeInfo = ImmutableList.of(
                ResourceGroupRuntimeInfo.builder(new ResourceGroupId("test-group"))
                        .addRunningQueries(2)
                        .build());
        testHttpClient.nodeHeartbeat(Optional.empty(), createTestNodeStatus("temp"));
        testHttpClient.resourceGroupRuntimeHeartbeat(Optional.empty(), "temp", runtimeInfo);
        Map map = clusterStateProvider.getResourceGroupStates();
        assertEquals(map.size(), 1);
    }

    @Test
    public void testGetResourceGroupInfo()
    {
        testHttpClient.nodeHeartbeat(Optional.empty(), createTestNodeStatus("coordinator"));
        testHttpClient.queryHeartbeat(Optional.empty(), "coordinator", createTestQueryInfo("temp", RUNNING), 1);

        List<ResourceGroupRuntimeInfo> runtimeInfo = ImmutableList.of(
                ResourceGroupRuntimeInfo.builder(new ResourceGroupId("test-group"))
                        .addRunningQueries(2)
                        .build());
        testHttpClient.resourceGroupRuntimeHeartbeat(Optional.empty(), "coordinator", runtimeInfo);

        List<ResourceGroupRuntimeInfo> result = testHttpClient.getResourceGroupInfo(Optional.empty(), "temp");
        assertEquals(result.size(), 1);
        assertEquals(result.get(0).getResourceGroupId(), new ResourceGroupId("test-group"));
    }

    @Test
    public void testGetMemoryPools()
    {
        Map<MemoryPoolId, ClusterMemoryPoolInfo> result = testHttpClient.getMemoryPoolInfo(Optional.empty());
        assertEquals(result.size(), 2);
    }

    @Test
    public void testGetRunningTaskCount()
    {
        testHttpClient.queryHeartbeat(Optional.empty(), "coordinator", createTestQueryInfo("temp", RUNNING), 1);
        int result = testHttpClient.getRunningTaskCount(Optional.empty());
        assertEquals(result, 1);
    }

    private static BasicQueryInfo createTestQueryInfo(String queryId, QueryState state)
    {
        return new BasicQueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                Optional.of(new ResourceGroupId("test-group")),
                state,
                GENERAL_POOL,
                true,
                URI.create("http://localhost"),
                "",
                new BasicQueryStats(
                        DateTime.now().getMillis(),
                        DateTime.now().getMillis(),
                        new Duration(1, SECONDS),
                        new Duration(1, SECONDS),
                        new Duration(1, SECONDS),
                        new Duration(1, SECONDS),
                        new Duration(1, SECONDS),
                        1, 1, 1, 1, 1, 100, 1, 1, 1, 100, 1, 1, 1, 100,
                        new DataSize(1, MEGABYTE),
                        1, 1, 1,
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        new DataSize(1, MEGABYTE),
                        new Duration(1, SECONDS),
                        new Duration(1, SECONDS),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        new DataSize(1, MEGABYTE),
                        OptionalDouble.of(1.0)),
                null,
                Optional.empty(),
                ImmutableList.of(),
                Optional.empty());
    }

    private static NodeStatus createTestNodeStatus(String nodeId)
    {
        return new NodeStatus(nodeId,
                new NodeVersion("1"),
                "test",
                true,
                Duration.valueOf("1ms"),
                "loc",
                "loc",
                new MemoryInfo(new DataSize(1, MEGABYTE), ImmutableMap.of()),
                1,
                1,
                1,
                1,
                1,
                1);
    }
}
