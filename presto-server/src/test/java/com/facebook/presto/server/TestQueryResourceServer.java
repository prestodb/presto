/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.operator.Page;
import com.facebook.presto.serde.PagesSerde;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.common.net.MediaType;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.event.client.InMemoryEventModule;
import io.airlift.http.client.ApacheHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static com.google.common.base.Charsets.UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestQueryResourceServer
{
    private static final MediaType MEDIA_TYPE_JSON = MediaType.create("application", "json");

    private HttpClient client;
    private LifeCycleManager lifeCycleManager;
    private TestingHttpServer server;

    @BeforeMethod
    public void setup()
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
                        binder.bind(LocationFactory.class).to(HttpLocationFactory.class).in(Scopes.SINGLETON);
                    }
                });

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        server = injector.getInstance(TestingHttpServer.class);

        client = new ApacheHttpClient();
    }

    @AfterMethod
    public void teardown()
            throws Exception
    {
        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testQuery()
            throws Exception
    {
        URI location = client.execute(preparePost().setUri(uriFor("/v1/query")).setBodyGenerator(createStaticBodyGenerator("query", UTF_8)).build(), new CreatedResponseHandler());
        assertQueryStatus(location, QueryState.RUNNING);

        QueryInfo queryInfo = client.execute(prepareGet().setUri(location).build(), createJsonResponseHandler(jsonCodec(QueryInfo.class)));
        TaskInfo taskInfo = queryInfo.getOutputStage().getTasks().get(0);
        URI outputLocation = uriFor("/v1/task/" + taskInfo.getTaskId() + "/results/out");

        assertEquals(loadData(outputLocation), 220);
        assertQueryStatus(location, QueryState.RUNNING);

        assertEquals(loadData(outputLocation), 44 + 48);
        assertQueryStatus(location, QueryState.FINISHED);

        // cancel the query
        StatusResponse cancelResponse = client.execute(prepareDelete().setUri(location).build(), createStatusResponseHandler());
        assertQueryStatus(location, QueryState.FINISHED);
        assertEquals(cancelResponse.getStatusCode(), HttpStatus.NO_CONTENT.code());
    }

    private void assertQueryStatus(URI location, QueryState expectedQueryState)
    {
        URI statusUri = uriBuilderFrom(location).build();
        JsonResponse<QueryInfo> response = client.execute(prepareGet().setUri(statusUri).build(), createFullJsonResponseHandler(jsonCodec(QueryInfo.class)));
        if (expectedQueryState == QueryState.FINISHED && response.getStatusCode() == HttpStatus.GONE.code()) {
            // when query finishes the server may delete it
            return;
        }
        QueryInfo queryInfo = response.getValue();
        assertEquals(queryInfo.getState(), expectedQueryState);
    }

    private int loadData(URI location)
    {
        List<Page> pages = client.execute(
                prepareGet().setUri(location).build(),
                new PageResponseHandler());

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

    private static class CreatedResponseHandler
            implements ResponseHandler<URI, RuntimeException>
    {
        @Override
        public RuntimeException handleException(Request request, Exception exception)
        {
            throw Throwables.propagate(exception);
        }

        @Override
        public URI handle(Request request, Response response)
        {
            if (response.getStatusCode() != HttpStatus.CREATED.code()) {
                throw new UnexpectedResponseException(
                        String.format("Expected response code to be 201 CREATED, but was %s %s", response.getStatusCode(), response.getStatusMessage()),
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

    public static class PageResponseHandler
            implements ResponseHandler<List<Page>, RuntimeException>
    {
        @Override
        public RuntimeException handleException(Request request, Exception exception)
        {
            throw Throwables.propagate(exception);
        }

        @Override
        public List<Page> handle(Request request, Response response)
        {
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                if (response.getStatusCode() == HttpStatus.GONE.code()) {
                    return ImmutableList.of();
                }
                throw new UnexpectedResponseException(
                        String.format("Expected response code to be 200, but was %s: %s", response.getStatusCode(), response.getStatusMessage()),
                        request,
                        response);
            }
            String contentType = response.getHeader("Content-Type");

            if (!MediaType.parse(contentType).is(PRESTO_PAGES_TYPE)) {
                throw new UnexpectedResponseException(String.format("Expected %s response from server but got %s", PRESTO_PAGES_TYPE, contentType), request, response);
            }

            try {
                SliceInput sliceInput = new InputStreamSliceInput(response.getInputStream());
                byte[] bytes = ByteStreams.toByteArray(sliceInput);
                Slice byteArraySlice = Slices.wrappedBuffer(bytes);
                List<Page> pages = ImmutableList.copyOf(PagesSerde.readPages(byteArraySlice.getInput()));
                return pages;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
