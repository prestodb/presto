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
package com.facebook.presto.server.protocol;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.discovery.client.DiscoveryModule;
import com.facebook.airlift.event.client.EventModule;
import com.facebook.airlift.http.server.testing.TestingHttpServerModule;
import com.facebook.airlift.jaxrs.JaxrsModule;
import com.facebook.airlift.jaxrs.testing.MockUriInfo;
import com.facebook.airlift.jmx.testing.TestingJmxModule;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.node.testing.TestingNodeModule;
import com.facebook.airlift.tracetoken.TraceTokenModule;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.TestingSessionContext;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.GracefulShutdownHandler;
import com.facebook.presto.server.ServerMainModule;
import com.facebook.presto.server.ShutdownAction;
import com.facebook.presto.server.security.ServerSecurityModule;
import com.facebook.presto.server.smile.SmileModule;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.testing.ProcedureTester;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.testing.TestingEventListenerManager;
import com.facebook.presto.testing.TestingWarningCollectorModule;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.QueryState.FAILED;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestRedirectingQueryProvider
{
    private Injector injector;

    @BeforeTest
    public void setup()
    {
        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new DiscoveryModule())
                .add(new TestingNodeModule(Optional.of("test")))
                .add(new TestingHttpServerModule(8080))
                .add(new JsonModule())
                .add(installModuleIf(
                        FeaturesConfig.class,
                        FeaturesConfig::isJsonSerdeCodeGenerationEnabled,
                        binder -> jsonBinder(binder).addModuleBinding().to(AfterburnerModule.class)))
                .add(new SmileModule())
                .add(new JaxrsModule(true))
                .add(new MBeanModule())
                .add(new TestingJmxModule())
                .add(new EventModule())
                .add(new TraceTokenModule())
                .add(new ServerSecurityModule())
                .add(new ServerMainModule(new SqlParserOptions()))
                .add(new TestingWarningCollectorModule())
                .add(binder -> {
                    binder.bind(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControlManager.class).to(TestingAccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(EventListenerManager.class).to(TestingEventListenerManager.class).in(Scopes.SINGLETON);
                    binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
                    binder.bind(ShutdownAction.class).to(TestingPrestoServer.TestShutdownAction.class).in(Scopes.SINGLETON);
                    binder.bind(GracefulShutdownHandler.class).in(Scopes.SINGLETON);
                    binder.bind(ProcedureTester.class).in(Scopes.SINGLETON);
                    binder.bind(RedirectingQueryProvider.class).in(Scopes.SINGLETON);
                });

        Bootstrap app = new Bootstrap(modules.build());

        injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(getServerProperties())
                .quiet()
                .initialize();
    }

    @Test
    public void testRedirectingQueryProviderReturnsImmediatelyWithoutData()
            throws Exception
    {
        RedirectingQueryProvider provider = injector.getInstance(RedirectingQueryProvider.class);
        DispatchManager dispatchManager = injector.getInstance(DispatchManager.class);

        // Set up the query
        QueryId queryId = new QueryId("123");
        String scheme = "https";
        String host = "190.168.0.0";
        String slug = "slug";
        ListenableFuture<?> future = dispatchManager.createQuery(queryId, slug, new TestingSessionContext(TEST_SESSION), "select 1");
        tryGetFutureValue(future, 1, SECONDS);

        // Use the query provider to return the query
        Query query = provider.getQuery(queryId, slug);
        assertNotNull(query);
        ListenableFuture<QueryResults> resultsFuture = query.waitForResults(
                0,
                MockUriInfo.from(format("%s://190.168.0.1", scheme)),
                scheme,
                Optional.of((uriInfo, xForwardedProto) -> UriBuilder.fromUri(format("%s://%s", scheme, host))),
                new Duration(1, SECONDS),
                new DataSize(1, MEGABYTE));

        assertTrue(resultsFuture.isDone());
        assertFalse(resultsFuture.isCancelled());
        QueryResults results = resultsFuture.get();
        assertEquals(results.getNextUri(), URI.create(format("%s://%s/v1/statement/executing/%s/0?slug=%s", scheme, host, queryId, slug)));
        assertNull(results.getData());
    }

    @Test
    public void testRedirectingQueryProviderCancellation()
    {
        RedirectingQueryProvider provider = injector.getInstance(RedirectingQueryProvider.class);
        DispatchManager dispatchManager = injector.getInstance(DispatchManager.class);

        // Set up the query
        QueryId queryId = new QueryId("123");
        String scheme = "https";
        String host = "190.168.0.0";
        String slug = "slug";
        ListenableFuture<?> future = dispatchManager.createQuery(queryId, slug, new TestingSessionContext(TEST_SESSION), "select 1");
        tryGetFutureValue(future, 1, SECONDS);

        // Use the query provider to cancel the query
        provider.getQuery(queryId, slug);
        provider.cancel(queryId, slug);
        BasicQueryInfo queryInfo = dispatchManager.getQueryInfo(queryId);
        assertEquals(queryInfo.getState(), FAILED);
    }

    private Map<String, String> getServerProperties()
    {
        Map<String, String> serverProperties = new HashMap<>();
        serverProperties.put("coordinator", String.valueOf(true));
        serverProperties.put("presto.version", "testversion");
        serverProperties.put("task.concurrency", "4");
        serverProperties.put("task.max-worker-threads", "4");
        serverProperties.put("exchange.client-threads", "4");
        serverProperties.put("optimizer.ignore-stats-calculator-failures", "false");
        serverProperties.put("failure-detector.enabled", "false");
        return ImmutableMap.copyOf(serverProperties);
    }
}