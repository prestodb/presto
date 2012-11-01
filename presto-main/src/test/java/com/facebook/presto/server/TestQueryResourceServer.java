/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.noperator.Page;
import com.facebook.presto.serde.PagesSerde;
import com.facebook.presto.slice.ByteArraySlice;
import com.facebook.presto.slice.InputStreamSliceInput;
import com.facebook.presto.slice.SliceInput;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StatusResponseHandler;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import static com.facebook.presto.server.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static org.testng.Assert.assertEquals;

public class TestQueryResourceServer
{
    private HttpClient client;
    private TestingHttpServer server;

    @BeforeMethod
    public void setup()
            throws Exception
    {
        Injector injector = Guice.createInjector(
                new TestingNodeModule(),
                new InMemoryEventModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new Module() {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(QueryResource.class).in(Scopes.SINGLETON);
                        binder.bind(QueryManager.class).to(SimpleQueryManager.class).in(Scopes.SINGLETON);
                        binder.bind(BlockMapper.class).in(Scopes.SINGLETON);
                        binder.bind(BlocksMapper.class).in(Scopes.SINGLETON);
                        binder.bind(PagesMapper.class).in(Scopes.SINGLETON);
                    }
                },
                new ConfigurationModule(new ConfigurationFactory(ImmutableMap.<String, String>of())));

        server = injector.getInstance(TestingHttpServer.class);
        server.start();
        client = new ApacheHttpClient();
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testQuery()
            throws Exception
    {
        URI location = client.execute(preparePost().setUri(uriFor("/v1/presto/query")).build(), new CreatedResponseHandler());


        assertEquals(loadData(location), 220);
        assertEquals(loadData(location), 44 + 48);

        StatusResponse response = client.execute(prepareDelete().setUri(location).build(), StatusResponseHandler.createStatusResponseHandler());
        assertEquals(response.getStatusCode(), Status.NO_CONTENT.getStatusCode());
    }

    private int loadData(URI location)
    {
        List<Page> pages = client.execute(
                prepareGet().setUri(location).build(),
                new PageResponseHandler(TupleInfo.SINGLE_VARBINARY));

        int count = 0;
        for (Page page : pages) {
            count += page.getPositionCount();
        }
        return count;
    }

    private URI uriFor(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

    private static class CreatedResponseHandler implements ResponseHandler<URI, RuntimeException>
    {
        @Override
        public RuntimeException handleException(Request request, Exception exception)
        {
            throw Throwables.propagate(exception);
        }

        @Override
        public URI handle(Request request, Response response)
        {
            if (response.getStatusCode() != Status.CREATED.getStatusCode()) {
                throw new UnexpectedResponseException(
                        String.format("Expected response code to be 201 CREATED, but was %d %s", response.getStatusCode(), response.getStatusMessage()),
                        request,
                        response);
            }
            String location = response.getHeader("Location");
            if (location == null) {
                throw new UnexpectedResponseException("Response does not contain a Location header", request, response);
            }
            return URI.create(location);
        }
    }

    public static class PageResponseHandler implements ResponseHandler<List<Page>, RuntimeException>
    {
        private final TupleInfo info;

        public PageResponseHandler(TupleInfo info)
        {
            this.info = info;
        }

        @Override
        public RuntimeException handleException(Request request, Exception exception)
        {
            throw Throwables.propagate(exception);
        }

        @Override
        public List<Page> handle(Request request, Response response)
        {
            if (response.getStatusCode() != Status.OK.getStatusCode()) {
                if (response.getStatusCode() == Status.GONE.getStatusCode()) {
                    return ImmutableList.of();
                }
                throw new UnexpectedResponseException(
                        String.format("Expected response code to be 200, but was %d: %s", response.getStatusCode(), response.getStatusMessage()),
                        request,
                        response);
            }
            String contentType = response.getHeader("Content-Type");

            if (!MediaType.valueOf(contentType).isCompatible(PRESTO_PAGES_TYPE)) {
                throw new UnexpectedResponseException(String.format("Expected %s response from server but got %s", PRESTO_PAGES_TYPE, contentType), request, response);
            }

            try {
                SliceInput sliceInput = new InputStreamSliceInput(response.getInputStream());
                byte[] bytes = ByteStreams.toByteArray(sliceInput);
                ByteArraySlice byteArraySlice = Slices.wrappedBuffer(bytes);
                List<Page> pages = ImmutableList.copyOf(PagesSerde.readPages(byteArraySlice.getInput()));
                return pages;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

}
