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
package com.facebook.presto.dispatcher;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpClientConfig;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.jetty.JettyHttpClient;
import com.facebook.airlift.jaxrs.JsonMapper;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.smile.SmileModule;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchHandleResolver;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.CountDownLatch;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.testing.Closeables.closeQuietly;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.server.smile.SmileCodecBinder.smileCodecBinder;
import static com.facebook.presto.tests.tpch.TpchQueryRunner.createQueryRunner;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestQueryInfoFetcher
{
    private HttpClient client;
    private TestingPrestoServer server;
    private CountDownLatch countDownLatch;
    private RemoteQueryStats remoteQueryStats;
    private QueryInfoFetcherFactory factory;

    @BeforeClass
    public void setupClass()
            throws Exception
    {
        HttpClientConfig config = new HttpClientConfig();
        config.setRequestTimeout(new Duration(2, SECONDS));
        client = new JettyHttpClient(config);
        DistributedQueryRunner runner = createQueryRunner(ImmutableMap.of("query.client.timeout", "10s"));
        server = runner.getCoordinator();
    }

    @AfterClass(alwaysRun = true)
    public void teardownClass()
    {
        closeQuietly(server);
        closeQuietly(client);
        server = null;
        client = null;
    }

    @BeforeMethod
    public void setupTest()
    {
        countDownLatch = new CountDownLatch(1);
        remoteQueryStats = new RemoteQueryStats();
        factory = createQueryInfoFetcherFactory(remoteQueryStats);
    }

    @AfterMethod
    public void teardownTest()
    {
        factory.stop();
        assertEquals(countDownLatch.getCount(), 0);
        countDownLatch = null;
        factory = null;
        remoteQueryStats = null;
    }

    @DataProvider(name = "binary_transport")
    public Object[][] binaryTransportDp()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(timeOut = 60_000, dataProvider = "binary_transport")
    public void testGetQueryInfos(boolean isBinaryTransportEnabled)
            throws Exception
    {
        QueryResults queryResults = postQuery("SELECT * from tpch.sf1.orders");
        queryResults = runToFirstResult(queryResults);
        QueryId queryId = new QueryId(queryResults.getId());

        QueryInfoFetcher queryInfoFetcher = factory.create(t -> fail("Failed to fetch", t), queryId, "", TEST_SESSION);

        queryInfoFetcher.start(new InternalNode("server", server.getBaseUrl(), new NodeVersion(""), true));

        queryInfoFetcher.addFinalQueryInfoListener(queryInfo -> {
            assertEquals(queryInfo.getState(), FINISHED);
            countDownLatch.countDown();
        });

        runToCompletion(queryResults);

        assertEquals(queryInfoFetcher.getQueryInfo().getState(), FINISHED);

        assertTrue(countDownLatch.await(5, SECONDS));
        assertTrue(remoteQueryStats.getHttpResponseStats().getRequestSuccess() > 0);
        assertTrue(remoteQueryStats.getInfoRoundTripCount() > 0);
        assertTrue(remoteQueryStats.getInfoRoundTripMillis() > 0);
    }

    @Test(timeOut = 60_000, dataProvider = "binary_transport")
    public void testFinalQueryFailed(boolean isBinaryTransportEnabled)
            throws Exception
    {
        QueryResults queryResults = postQuery("SELECT * from tpch.sf1.orders");
        queryResults = runToFirstResult(queryResults);
        QueryId queryId = new QueryId(queryResults.getId());

        QueryInfoFetcher queryInfoFetcher = factory.create(t -> fail("Failed to fetch", t), queryId, "", TEST_SESSION);

        queryInfoFetcher.start(new InternalNode("server", server.getBaseUrl(), new NodeVersion(""), true));

        queryInfoFetcher.addFinalQueryInfoListener(queryInfo -> {
            assertEquals(queryInfo.getState(), FAILED);
            countDownLatch.countDown();
        });

        // Sleep to trigger client query expiration
        Thread.sleep(SECONDS.toMillis(20));

        assertEquals(queryInfoFetcher.getQueryInfo().getState(), FAILED);

        assertTrue(countDownLatch.await(5, SECONDS));
        assertTrue(remoteQueryStats.getHttpResponseStats().getRequestSuccess() > 0);
        assertTrue(remoteQueryStats.getInfoRoundTripCount() > 0);
        assertTrue(remoteQueryStats.getInfoRoundTripMillis() > 0);
    }

    @Test(timeOut = 120_000, dataProvider = "binary_transport")
    public void testCommunicationFailure(boolean isBinaryTransportEnabled)
            throws Exception
    {
        QueryResults queryResults = postQuery("SELECT * from tpch.sf1.orders");
        queryResults = runToFirstResult(queryResults);
        QueryId queryId = new QueryId(queryResults.getId());

        QueryInfoFetcher queryInfoFetcher = factory.create(t -> countDownLatch.countDown(), queryId, "", TEST_SESSION);

        queryInfoFetcher.start(new InternalNode("server", server.getBaseUrl(), new NodeVersion(""), true));
        try {
            server.stopResponding();

            queryInfoFetcher.addFinalQueryInfoListener(queryInfo -> fail("Communication failures should not update the final query info"));

            // Sleep to trigger client query expiration
            Thread.sleep(SECONDS.toMillis(20));

            assertEquals(queryInfoFetcher.getQueryInfo().getState(), QUEUED);

            assertTrue(countDownLatch.await(5, SECONDS));
            assertTrue(remoteQueryStats.getHttpResponseStats().getRequestFailure() > 0);
        }
        finally {
            server.startResponding();
        }
    }

    private QueryInfoFetcherFactory createQueryInfoFetcherFactory(RemoteQueryStats remoteQueryStats)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new SmileModule(),
                new HandleJsonModule(),
                binder -> {
                    binder.bind(JsonMapper.class);
                    configBinder(binder).bindConfig(FeaturesConfig.class);
                    binder.bind(TypeRegistry.class).in(Scopes.SINGLETON);
                    binder.bind(TypeManager.class).to(TypeRegistry.class).in(Scopes.SINGLETON);
                    jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
                    newSetBinder(binder, Type.class);
                    smileCodecBinder(binder).bindSmileCodec(QueryInfo.class);
                    jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
                    jsonBinder(binder).addKeySerializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionSerializer.class);
                    jsonBinder(binder).addKeyDeserializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionDeserializer.class);
                    configBinder(binder).bindConfig(DispatcherConfig.class);
                    configBinder(binder).bindConfig(InternalCommunicationConfig.class);
                    binder.bind(QueryInfoFetcherFactory.class).in(Scopes.SINGLETON);
                    binder.bind(RemoteQueryStats.class).toInstance(remoteQueryStats);
                    binder.bind(HttpClient.class).toInstance(client);
                    binder.bind(LocationFactory.class).to(MockLocationFactory.class);
                });
        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(ImmutableMap.<String, String>builder()
                        .put("dispatcher.remote-query-info.min-error-duration", "5s")
                        .put("dispatcher.remote-query-info.update-interval", "1ms")
                        .put("dispatcher.remote-query-info.refresh-max-wait", "5s")
                        .build())
                .quiet()
                .initialize();

        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        handleResolver.addConnectorName("tpch", new TpchHandleResolver());
        return injector.getInstance(QueryInfoFetcherFactory.class);
    }

    private QueryResults postQuery(String sql)
    {
        URI uri = uriBuilderFrom(server.getBaseUrl().resolve("/v1/statement")).build();
        Request request = preparePost()
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .setBodyGenerator(createStaticBodyGenerator(sql, UTF_8))
                .build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
    }

    private QueryResults runToFirstResult(QueryResults queryResults)
    {
        while (queryResults.getData() == null) {
            queryResults = getQuery(queryResults.getNextUri());
        }
        return queryResults;
    }

    private QueryResults getQuery(URI uri)
    {
        Request request = prepareGet()
                .setHeader(PRESTO_USER, "user")
                .setUri(uri)
                .build();
        return client.execute(request, createJsonResponseHandler(jsonCodec(QueryResults.class)));
    }

    private void runToCompletion(QueryResults queryResults)
    {
        while (queryResults.getNextUri() != null) {
            queryResults = getQuery(queryResults.getNextUri());
        }
    }

    private static class MockLocationFactory
            implements LocationFactory
    {
        @Override
        public URI createQueryLocation(QueryId queryId)
        {
            return null;
        }

        @Override
        public URI createQueryLocation(InternalNode node, QueryId queryId)
        {
            requireNonNull(queryId, "queryId is null");
            return uriBuilderFrom(node.getInternalUri())
                    .appendPath("/v1/query")
                    .appendPath(queryId.toString())
                    .build();
        }

        @Override
        public URI createStageLocation(StageId stageId)
        {
            return null;
        }

        @Override
        public URI createLocalTaskLocation(TaskId taskId)
        {
            return null;
        }

        @Override
        public URI createLegacyTaskLocation(InternalNode node, TaskId taskId)
        {
            return null;
        }

        @Override
        public URI createTaskLocation(InternalNode node, TaskId taskId)
        {
            return null;
        }

        @Override
        public URI createMemoryInfoLocation(InternalNode node)
        {
            return null;
        }
    }
}