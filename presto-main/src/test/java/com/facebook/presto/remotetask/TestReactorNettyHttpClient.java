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
package com.facebook.presto.remotetask;

import com.facebook.airlift.http.client.HttpClient.HttpResponseFuture;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonCodecFactory;
import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.server.remotetask.HttpClientConnectionPoolStats;
import com.facebook.presto.server.remotetask.HttpClientStats;
import com.facebook.presto.server.remotetask.ReactorNettyHttpClient;
import com.facebook.presto.server.remotetask.ReactorNettyHttpClientConfig;
import com.facebook.presto.server.smile.AdaptingJsonResponseHandler;
import com.facebook.presto.server.smile.JsonResponseWrapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;

import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.presto.execution.TaskState.PLANNED;
import static com.facebook.presto.server.RequestHelpers.getJsonTransportBuilder;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;

public class TestReactorNettyHttpClient
{
    private static DisposableServer server;
    private static ReactorNettyHttpClient reactorNettyHttpClient;
    private static JsonCodec<TaskStatus> taskStatusCodec;

    private static final TaskStatus TEST_TASK_STATUS_HTTP11 = new TaskStatus(
            11111L,
            99999L,
            123,
            PLANNED,
            URI.create("http://localhost:8080/v1/task/1234"),
            ImmutableSet.of(),
            ImmutableList.of(),
            0,
            0,
            0.0,
            false,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0L,
            0L);

    @BeforeClass
    public static void setUp()
    {
        taskStatusCodec = new JsonCodecFactory(new JsonObjectMapperProvider()).jsonCodec(TaskStatus.class);
        server = HttpServer.create()
                .port(8080)
                .protocol(HttpProtocol.HTTP11)
                .route(routes ->
                        routes.get("/v1/task/1234/status", (request, response) ->
                                response.header("Content-Type", "application/json").sendString(Mono.just(taskStatusCodec.toJson(TEST_TASK_STATUS_HTTP11)))))
                .bindNow();
        ReactorNettyHttpClientConfig reactorNettyHttpClientConfig = new ReactorNettyHttpClientConfig()
                .setRequestTimeout(new Duration(30, TimeUnit.SECONDS))
                .setConnectTimeout(new Duration(30, TimeUnit.SECONDS));
        reactorNettyHttpClient = new ReactorNettyHttpClient(reactorNettyHttpClientConfig, new HttpClientConnectionPoolStats(), new HttpClientStats());
    }

    @AfterClass
    public static void tearDown()
    {
        server.disposeNow();
    }

    @Test
    public void testGetTaskStatus()
            throws ExecutionException, InterruptedException
    {
        AdaptingJsonResponseHandler<TaskStatus> responseHandler = createAdaptingJsonResponseHandler(taskStatusCodec);
        Request request = getJsonTransportBuilder(prepareGet()).setUri(uriBuilderFrom(TEST_TASK_STATUS_HTTP11.getSelf()).appendPath("status").build()).build();

        HttpResponseFuture response = reactorNettyHttpClient.executeAsync(request, responseHandler);
        TaskStatus a = (TaskStatus) ((JsonResponseWrapper) response.get()).getValue();

        assertEquals(a.getState(), TEST_TASK_STATUS_HTTP11.getState());
        assertEquals(a.getTaskInstanceIdLeastSignificantBits(), TEST_TASK_STATUS_HTTP11.getTaskInstanceIdLeastSignificantBits());
        assertEquals(a.getTaskInstanceIdMostSignificantBits(), TEST_TASK_STATUS_HTTP11.getTaskInstanceIdMostSignificantBits());
        assertEquals(a.getVersion(), TEST_TASK_STATUS_HTTP11.getVersion());
        assertEquals(a.getSelf(), TEST_TASK_STATUS_HTTP11.getSelf());
    }
}
