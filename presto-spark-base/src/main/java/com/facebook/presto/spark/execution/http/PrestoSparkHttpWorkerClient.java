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
package com.facebook.presto.spark.execution.http;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.HttpRpcShuffleClient;
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.operator.RpcShuffleClient;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;

import static com.facebook.airlift.http.client.HttpStatus.familyForStatusCode;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static java.util.Objects.requireNonNull;

/**
 * An abstraction of HTTP client that communicates with the locally running Presto worker process. It exposes worker endpoints to simple method calls.
 */
@ThreadSafe
public class PrestoSparkHttpWorkerClient
        implements RpcShuffleClient
{
    private static final Logger log = Logger.get(PrestoSparkHttpWorkerClient.class);

    private final HttpClient httpClient;
    private final URI location;
    private final TaskId taskId;

    public PrestoSparkHttpWorkerClient(HttpClient httpClient, TaskId taskId, URI location)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.location = requireNonNull(location, "location is null");
    }

    /**
     * Get results from a native engine task that ends with none shuffle operator. It always fetches from a single buffer.
     */
    @Override
    public ListenableFuture<PageBufferClient.PagesResponse> getResults(
            long token,
            DataSize maxResponseSize)
    {
        URI uri = uriBuilderFrom(location)
                .appendPath(taskId.toString())
                .appendPath("/results/0")
                .appendPath(String.valueOf(token))
                .build();
        return httpClient.executeAsync(
                prepareGet()
                        .setHeader(PRESTO_MAX_SIZE, maxResponseSize.toString())
                        .setUri(uri)
                        .build(),
                new HttpRpcShuffleClient.PageResponseHandler());
    }

    @Override
    public void acknowledgeResultsAsync(long nextToken)
    {
        URI uri = uriBuilderFrom(location)
                .appendPath(taskId.toString())
                .appendPath("/results/0")
                .appendPath(String.valueOf(nextToken))
                .appendPath("acknowledge")
                .build();
        httpClient.executeAsync(
                prepareGet().setUri(uri).build(),
                new ResponseHandler<Void, RuntimeException>()
                {
                    @Override
                    public Void handleException(Request request,
                                                Exception exception)
                    {
                        log.debug(exception, "Acknowledge request failed: %s", uri);
                        return null;
                    }

                    @Override
                    public Void handle(Request request, Response response)
                    {
                        if (familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                            log.debug("Unexpected acknowledge response code: %s", response.getStatusCode());
                        }
                        return null;
                    }
                });
    }

    @Override
    public ListenableFuture<?> abortResults()
    {
        URI uri = uriBuilderFrom(location)
                .appendPath(taskId.toString())
                .build();
        return httpClient.executeAsync(
                prepareDelete().setUri(uri).build(),
                createStatusResponseHandler());
    }

    @Override
    public Throwable rewriteException(Throwable throwable)
    {
        return null;
    }

    public URI getLocation()
    {
        return location;
    }
}
