/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.ExchangePlanFragmentSource;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.operator.Page;
import com.facebook.presto.server.QueryDriversOperator.QueryDriversIterator;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragmentSource;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.Request;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHttpQueryProvider
{
    private ApacheHttpClient httpClient;
    private TestingHttpServer server1;
    private TestingHttpServer server2;
    private TestingHttpServer server3;
    private ExecutorService executor;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        try {
            server1 = createServer();
            server2 = createServer();
            server3 = createServer();
            executor = Executors.newCachedThreadPool();
            httpClient = new ApacheHttpClient();
        }
        catch (Exception | Error e) {
            teardown();
        }
    }

    private TestingHttpServer createServer()
            throws Exception
    {
        Injector injector = Guice.createInjector(
                new TestingNodeModule(),
                new InMemoryEventModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(QueryResource.class).in(Scopes.SINGLETON);
                        binder.bind(TaskResource.class).in(Scopes.SINGLETON);
                        binder.bind(QueryManager.class).to(MockQueryManager.class).in(Scopes.SINGLETON);
                        binder.bind(MockTaskManager.class).in(Scopes.SINGLETON);
                        binder.bind(TaskManager.class).to(Key.get(MockTaskManager.class)).in(Scopes.SINGLETON);
                        binder.bind(PagesMapper.class).in(Scopes.SINGLETON);
                    }
                },
                new ConfigurationModule(new ConfigurationFactory(ImmutableMap.<String, String>of())));

        TestingHttpServer server = injector.getInstance(TestingHttpServer.class);
        server.start();
        return server;
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        if (server1 != null) {
            server1.stop();
        }
        if (server2 != null) {
            server2.stop();
        }
        if (server3 != null) {
            server3.stop();
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Test
    public void testQuery()
            throws Exception
    {
        QueryDriversOperator operator = new QueryDriversOperator(10,
                createHttpQueryProvider(server1),
                createHttpQueryProvider(server2),
                createHttpQueryProvider(server3)
        );

        int count = 0;
        for (Page page : operator) {
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                count++;
            }
        }
        assertEquals(count, 312 * 3);
    }

    @Test
    public void testCancel()
            throws Exception
    {
        QueryDriversOperator operator = new QueryDriversOperator(10,
                createHttpQueryProvider(server1),
                createHttpQueryProvider(server2),
                createHttpQueryProvider(server3)
        );

        int count = 0;
        QueryDriversIterator iterator = operator.iterator();
        while (count < 20 && iterator.hasNext()) {
            Page page = iterator.next();
            BlockCursor cursor = page.getBlock(0).cursor();
            while (count < 20 && cursor.advanceNextPosition()) {
                count++;
            }
        }
        assertEquals(count, 20);

        // verify we have more data
        assertTrue(iterator.hasNext());

        // cancel the iterator
        iterator.close();
        assertFalse(iterator.hasNext());
    }

    private HttpTaskClient createHttpQueryProvider(TestingHttpServer httpServer)
    {
        ImmutableMap<String, List<PlanFragmentSource>> fragmentSources = ImmutableMap.of();
        PlanFragment planFragment = new PlanFragment(32, false, ImmutableMap.<Symbol, Type>of(), new ExchangeNode(22, ImmutableList.<Symbol>of()));

        QueryFragmentRequest fragmentRequest = new QueryFragmentRequest(planFragment,
                ImmutableList.<PlanFragmentSource>of(),
                ImmutableMap.<String, ExchangePlanFragmentSource>of(),
                ImmutableList.of("out"));

        Request request = preparePost()
                .setUri(httpServer.getBaseUrl().resolve("/v1/task"))
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(jsonCodec(QueryFragmentRequest.class), fragmentRequest))
                .build();

        JsonResponse<TaskInfo> response = httpClient.execute(request, createFullJsonResponseHandler(jsonCodec(TaskInfo.class)));
        Preconditions.checkState(response.getStatusCode() == 201,
                "Expected response code from %s to be 201, but was %s: %s",
                request.getUri(),
                response.getStatusCode(),
                response.getStatusMessage());
        String location = response.getHeader("Location");
        Preconditions.checkState(location != null);

        TaskInfo taskInfo = response.getValue();

        // schedule table scan task on remote node
        // todo we don't need a QueryDriverProvider
        return new HttpTaskClient(taskInfo.getTaskId(),
                URI.create(location),
                "out",
                taskInfo.getTupleInfos(),
                httpClient,
                executor,
                jsonCodec(TaskInfo.class));
    }
}
