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
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskId;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.StatusResponseHandler.StatusResponse;
import io.airlift.json.JsonCodec;

import java.net.URI;

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePut;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class DynamicFilterClient
{
    private final JsonCodec<DynamicFilterSummary> summaryJsonCodec;
    private final URI coordinatorURI;
    private final HttpClient httpClient;
    private final TaskId taskId;
    private final String source;
    private final int driverId;
    private final int expectedDriversCount;

    public DynamicFilterClient(JsonCodec<DynamicFilterSummary> summaryJsonCodec, URI coordinatorURI, HttpClient httpClient, TaskId taskId, String source, int driverId, int expectedDriversCount)
    {
        this.summaryJsonCodec = requireNonNull(summaryJsonCodec, "summaryJsonCodec is null");
        this.coordinatorURI = requireNonNull(coordinatorURI, "coordinatorURI is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.source = requireNonNull(source, "source is null");
        this.driverId = driverId;
        this.expectedDriversCount = expectedDriversCount;
    }

    public HttpResponseFuture<JsonResponse<DynamicFilterSummary>> getSummary()
    {
        return httpClient.executeAsync(
                prepareGet()
                        .setUri(HttpUriBuilder.uriBuilderFrom(coordinatorURI)
                                .appendPath("/v1/dynamic-filter")
                                .appendPath("/" + taskId.getQueryId())
                                .appendPath("/" + source)
                                .build())
                        .build(),
                createFullJsonResponseHandler(jsonCodec(DynamicFilterSummary.class)));
    }

    public ListenableFuture<StatusResponse> storeSummary(DynamicFilterSummary summary)
    {
        return httpClient.executeAsync(
                preparePut()
                        .setUri(HttpUriBuilder.uriBuilderFrom(coordinatorURI)
                                .appendPath("/v1/dynamic-filter")
                                .appendPath("/" + taskId.getQueryId())
                                .appendPath("/" + source)
                                .appendPath("/" + taskId.getStageId().getId())
                                .appendPath("/" + taskId.getId())
                                .appendPath("/" + driverId)
                                .appendPath("/" + expectedDriversCount)
                                .build())
                        .addHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                        .setBodyGenerator(jsonBodyGenerator(summaryJsonCodec, summary))
                        .build(),
                createStatusResponseHandler());
    }
}
