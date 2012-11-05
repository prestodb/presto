/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.nblock.BlockCursor;
import com.facebook.presto.noperator.Page;
import com.facebook.presto.server.QueryDriversOperator.QueryDriversIterator;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestHttpQueryProvider
{
    private AsyncHttpClient httpClient ;
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
            httpClient = new AsyncHttpClient(new ApacheHttpClient(), executor);
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
                        binder.bind(QueryManager.class).to(SimpleQueryManager.class).in(Scopes.SINGLETON);
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
        executor.shutdownNow();
    }

    @Test
    public void testQuery()
            throws Exception
    {
        QueryDriversOperator operator = new QueryDriversOperator(10,
                new HttpQueryProvider("query", httpClient, server1.getBaseUrl().resolve("/v1/presto/query")),
                new HttpQueryProvider("query", httpClient, server2.getBaseUrl().resolve("/v1/presto/query")),
                new HttpQueryProvider("query", httpClient, server3.getBaseUrl().resolve("/v1/presto/query"))
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
                new HttpQueryProvider("query", httpClient, server1.getBaseUrl().resolve("/v1/presto/query")),
                new HttpQueryProvider("query", httpClient, server2.getBaseUrl().resolve("/v1/presto/query")),
                new HttpQueryProvider("query", httpClient, server3.getBaseUrl().resolve("/v1/presto/query"))
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
        iterator.cancel();

        // due to buffering in the iterator, we still have one more element, so read it and
        // verify there are no more elements
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        assertFalse(iterator.hasNext());
    }
}
