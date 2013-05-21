/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.TaskSource;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.operator.ExchangeClientFactory;
import com.facebook.presto.operator.ExchangeOperator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.netty.StandaloneNettyAsyncHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.server.MockQueryManager.TUPLE_INFOS;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_CATALOG;
import static com.facebook.presto.sql.analyzer.Session.DEFAULT_SCHEMA;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestExchangeOperator
{
    private final List<LifeCycleManager> lifeCycleManagers = new ArrayList<>();
    private ExchangeClientFactory exchangeClientFactory;
    private ExecutorService executor;
    private AsyncHttpClient httpClient;
    private TestingHttpServer server1;
    private TestingHttpServer server2;
    private TestingHttpServer server3;

    @BeforeClass
    public void setup()
            throws Exception
    {
        try {
            server1 = createServer();
            server2 = createServer();
            server3 = createServer();
            httpClient = new StandaloneNettyAsyncHttpClient("test");
            executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
            exchangeClientFactory = new ExchangeClientFactory(new DataSize(32, Unit.MEGABYTE), new DataSize(10, Unit.MEGABYTE), 3, httpClient, executor);
        }
        catch (Throwable e) {
            teardown();
            throw e;
        }
    }

    private TestingHttpServer createServer()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
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
                        binder.bind(StageResource.class).in(Scopes.SINGLETON);
                        binder.bind(TaskResource.class).in(Scopes.SINGLETON);
                        binder.bind(QueryManager.class).to(MockQueryManager.class).in(Scopes.SINGLETON);
                        binder.bind(MockTaskManager.class).in(Scopes.SINGLETON);
                        binder.bind(TaskManager.class).to(Key.get(MockTaskManager.class)).in(Scopes.SINGLETON);
                        binder.bind(PagesMapper.class).in(Scopes.SINGLETON);
                        binder.bind(NodeManager.class).to(InMemoryNodeManager.class).in(Scopes.SINGLETON);
                        binder.bind(LocationFactory.class).to(HttpLocationFactory.class).in(Scopes.SINGLETON);
                    }
                });

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManagers.add(injector.getInstance(LifeCycleManager.class));

        return injector.getInstance(TestingHttpServer.class);
    }

    @AfterClass
    public void teardown()
            throws Exception
    {
        for (LifeCycleManager lifeCycleManager : lifeCycleManagers) {
            lifeCycleManager.stop();
        }
        if (httpClient != null) {
            httpClient.close();
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @Test
    public void testQuery()
            throws Exception
    {
        ExchangeOperator operator = new ExchangeOperator(exchangeClientFactory, TUPLE_INFOS);

        operator.addSplit(new RemoteSplit(scheduleTask(server1), TUPLE_INFOS));
        operator.addSplit(new RemoteSplit(scheduleTask(server2), TUPLE_INFOS));
        operator.addSplit(new RemoteSplit(scheduleTask(server3), TUPLE_INFOS));
        operator.noMoreSplits();

        int count = 0;
        PageIterator iterator = operator.iterator(new OperatorStats());
        while (iterator.hasNext()) {
            Page page = iterator.next();
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
        ExchangeOperator operator = new ExchangeOperator(exchangeClientFactory, TUPLE_INFOS);

        operator.addSplit(new RemoteSplit(scheduleTask(server1), TUPLE_INFOS));
        operator.addSplit(new RemoteSplit(scheduleTask(server2), TUPLE_INFOS));
        operator.addSplit(new RemoteSplit(scheduleTask(server3), TUPLE_INFOS));
        operator.noMoreSplits();

        int count = 0;

        PageIterator iterator = operator.iterator(new OperatorStats());
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

    private URI scheduleTask(TestingHttpServer httpServer)
            throws Exception
    {
        PlanFragment planFragment = new PlanFragment(new PlanFragmentId("32"), null, ImmutableMap.<Symbol, Type>of(), new ExchangeNode(new PlanNodeId("1"), new PlanFragmentId("22"), ImmutableList.<Symbol>of()));

        Session session = new Session("user", "test", DEFAULT_CATALOG, DEFAULT_SCHEMA, null, null);
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(session,
                planFragment,
                ImmutableList.<TaskSource>of(),
                new OutputBuffers(ImmutableSet.of("out"), true));

        String taskId = "query.stage." + httpServer.getPort() + "_" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        Request request = preparePost()
                .setUri(httpServer.getBaseUrl().resolve("/v1/task/" + taskId))
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(jsonCodec(TaskUpdateRequest.class), updateRequest))
                .build();

        JsonResponse<TaskInfo> response = httpClient.execute(request, createFullJsonResponseHandler(jsonCodec(TaskInfo.class)));
        Preconditions.checkState(response.getStatusCode() == HttpStatus.OK.code(),
                "Expected response code from %s to be %s, but was %s: %s",
                request.getUri(),
                HttpStatus.OK,
                response.getStatusCode(),
                response.getStatusMessage());
        TaskInfo taskInfo = response.getValue();

        URI outputLocation = httpServer.getBaseUrl().resolve("/v1/task/" + taskInfo.getTaskId() + "/results/out");
        return outputLocation;
    }
}
