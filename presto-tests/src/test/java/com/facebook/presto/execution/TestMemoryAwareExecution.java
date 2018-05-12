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
package com.facebook.presto.execution;

import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.memory.LocalMemoryManager;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.server.GracefulShutdownHandler;
import com.facebook.presto.server.ServerMainModule;
import com.facebook.presto.server.ShutdownAction;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.session.ResourceEstimates;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.testing.ProcedureTester;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.testing.TestingEventListenerManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.testing.TestingDiscoveryModule;
import io.airlift.event.client.EventModule;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.tracetoken.TraceTokenModule;
import io.airlift.units.DataSize;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.QueryState.WAITING_FOR_RESOURCES;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static io.airlift.units.DataSize.succinctBytes;

public class TestMemoryAwareExecution
{
    private QueryManager queryManager;
    private long totalAvailableMemory;
    private long queryMaxMemoryBytes;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        ImmutableMap.Builder<String, String> serverProperties = ImmutableMap.<String, String>builder()
                .put("coordinator", "true")
                .put("presto.version", "testversion")
                .put("task.concurrency", "4")
                .put("task.max-worker-threads", "4")
                .put("exchange.client-threads", "4")
                .put("query.max-memory-per-node", "512MB");

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new TestingNodeModule(Optional.of("test")))
                .add(new TestingHttpServerModule(0))
                .add(new JsonModule())
                .add(new JaxrsModule(true))
                .add(new MBeanModule())
                .add(new TestingJmxModule())
                .add(new EventModule())
                .add(new TraceTokenModule())
                .add(new ServerSecurityModule())
                .add(new ServerMainModule(new SqlParserOptions()))
                .add(binder -> {
                    binder.bind(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControlManager.class).to(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(EventListenerManager.class).to(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(ShutdownAction.class).to(TestingPrestoServer.TestShutdownAction.class).in(Scopes.SINGLETON);
                    binder.bind(GracefulShutdownHandler.class).in(Scopes.SINGLETON);
                    binder.bind(ProcedureTester.class).in(Scopes.SINGLETON);
                });

        modules.add(new TestingDiscoveryModule());

        Bootstrap app = new Bootstrap(modules.build());

        Map<String, String> optionalProperties = new HashMap<>();

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(serverProperties
                        .put("experimental.memory-aware-execution", "true")
                        .build())
                .setOptionalConfigurationProperties(optionalProperties)
                .quiet()
                .initialize();

        injector.getInstance(Announcer.class).start();

        ResourceGroupManager<?> resourceGroupManager = injector.getInstance(ResourceGroupManager.class);
        LocalMemoryManager localMemoryManager = injector.getInstance(LocalMemoryManager.class);

        ClusterMemoryManager clusterMemoryManager = injector.getInstance(ClusterMemoryManager.class);
        queryManager = injector.getInstance(QueryManager.class);
        queryMaxMemoryBytes = injector.getInstance(MemoryManagerConfig.class).getMaxQueryMemory().toBytes();

        // wait for cluster memory to be picked up
        while(clusterMemoryManager.getClusterMemoryBytes() == 0) {
            Thread.sleep(1000);
        }
        totalAvailableMemory = localMemoryManager.getPool(LocalMemoryManager.GENERAL_POOL).getMaxBytes();
    }

    @Test
    public void testWaitingForResources()
            throws Exception
    {
        QueryId normalQuery = queryWithResourceEstimate(new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()), queryManager);
        ResourceEstimates estimate = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(DataSize.valueOf("20GB")));
        QueryId highMemoryQuery = queryWithResourceEstimate(estimate, queryManager);
        assertState(normalQuery, RUNNING);
        assertState(highMemoryQuery, WAITING_FOR_RESOURCES);
        queryManager.failQuery(normalQuery, new Exception("Killed"));
        waitForState(normalQuery, FAILED);
    }

    @Test
    public void testPreAllocateTooMuch()
            throws Exception
    {
        DataSize tooMuch = succinctBytes(queryMaxMemoryBytes + 1);
        ResourceEstimates estimate = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(tooMuch));
        QueryId highMemoryQuery = queryWithResourceEstimate(estimate, queryManager);
        assertState(highMemoryQuery, FAILED);
    }

    @Test //(invocationCount = 10, invocationTimeOut = 10000)
    public void testStartWhenPreAllocationClears()
            throws Exception
    {
        ResourceEstimates estimate = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.of(succinctBytes(totalAvailableMemory)));

        QueryId highMemoryQuery1 = queryWithResourceEstimate(estimate, queryManager);
        assertState(highMemoryQuery1, RUNNING);

        QueryId highMemoryQuery2 = queryWithResourceEstimate(estimate, queryManager);
        assertState(highMemoryQuery2, WAITING_FOR_RESOURCES);

        queryManager.failQuery(highMemoryQuery1, new Exception("Killed"));
        System.out.println("Killed");
        waitForState(highMemoryQuery1, FAILED);
        assertState(highMemoryQuery2, RUNNING);

        queryManager.failQuery(highMemoryQuery2, new Exception("Killed"));
        waitForState(highMemoryQuery2, FAILED);
    }

    private static QueryId queryWithResourceEstimate(ResourceEstimates estimate, QueryManager queryManager)
    {
        return queryManager.createQuery(new TestingSessionContext(testSessionBuilder().setResourceEstimates(estimate).build()), "SELECT 1").getQueryId();
    }

    private void assertState(QueryId queryId, QueryState state)
            throws InterruptedException
    {
        assertEquals(waitForState(queryId, state), state);
    }

    private QueryState waitForState(QueryId queryId, QueryState state)
            throws InterruptedException
    {
        QueryState actualState;
        do {
            actualState = queryManager.getQueryInfo(queryId).getState();
            if (actualState.isDone() || actualState == state) {
                return actualState;
            }
            Thread.sleep(1000);
        } while(true);
    }
}
