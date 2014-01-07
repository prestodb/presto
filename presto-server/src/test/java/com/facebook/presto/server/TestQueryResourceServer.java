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
package com.facebook.presto.server;

import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.operator.HttpPageBufferClient.PageResponseHandler;
import com.facebook.presto.operator.HttpPageBufferClient.PagesResponse;
import com.facebook.presto.operator.Page;
import com.google.common.base.Throwables;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;

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
                        binder.bind(NodeManager.class).to(InMemoryNodeManager.class).in(Scopes.SINGLETON);
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

    @Test(enabled = false)
    public void testQuery()
            throws Exception
    {
        URI location = client.execute(preparePost().setUri(uriFor("/v1/query")).setBodyGenerator(createStaticBodyGenerator("query", UTF_8)).build(), new CreatedResponseHandler());
        assertQueryStatus(location, QueryState.RUNNING);

        QueryInfo queryInfo = client.execute(prepareGet().setUri(location).build(), createJsonResponseHandler(jsonCodec(QueryInfo.class)));
        TaskInfo taskInfo = queryInfo.getOutputStage().getTasks().get(0);
        URI outputLocation = uriFor("/v1/task/" + taskInfo.getTaskId() + "/results/out");

        long sequenceId = 0;
        PagesResponse response = client.execute(
                prepareGet().setUri(uriBuilderFrom(outputLocation).appendPath(String.valueOf(sequenceId)).build()).build(),
                new PageResponseHandler());
        List<Page> pages = response.getPages();
        assertEquals(countPositions(pages), 220);
        assertQueryStatus(location, QueryState.RUNNING);

        sequenceId += pages.size();
        response = client.execute(
                prepareGet().setUri(uriBuilderFrom(outputLocation).appendPath(String.valueOf(sequenceId)).build()).build(),
                new PageResponseHandler());
        pages = response.getPages();
        assertEquals(countPositions(pages), 44 + 48);

        sequenceId += pages.size();
        response = client.execute(
                prepareGet().setUri(uriBuilderFrom(outputLocation).appendPath(String.valueOf(sequenceId)).build()).build(),
                new PageResponseHandler());
        pages = response.getPages();
        assertEquals(countPositions(pages), 0);

        assertQueryStatus(location, QueryState.FINISHED);

        // cancel the query
        StatusResponse cancelResponse = client.execute(prepareDelete().setUri(location).build(), createStatusResponseHandler());
        assertQueryStatus(location, QueryState.FINISHED);
        assertEquals(cancelResponse.getStatusCode(), HttpStatus.NO_CONTENT.code());
    }

    private int countPositions(List<Page> pages)
    {
        int count = 0;
        for (Page page : pages) {
            count += page.getPositionCount();
        }
        return count;
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

    private URI uriFor(String path)
    {
        return server.getBaseUrl().resolve(path);
    }

    private static class CreatedResponseHandler
            implements ResponseHandler<URI, RuntimeException>
    {
        @Override
        public URI handleException(Request request, Exception exception)
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
}
