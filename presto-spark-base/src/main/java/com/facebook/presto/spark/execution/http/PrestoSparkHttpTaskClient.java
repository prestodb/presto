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
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskExecutionId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.HttpRpcShuffleClient;
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.operator.RpcShuffleClient;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.BatchTaskUpdateRequest;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.http.client.HttpStatus.familyForStatusCode;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * An abstraction of HTTP client that communicates with the locally running Presto worker process. It exposes worker endpoints to simple method calls.
 */
@ThreadSafe
public class PrestoSparkHttpTaskClient
        implements RpcShuffleClient
{
    private static final Logger log = Logger.get(PrestoSparkHttpTaskClient.class);
    private static final String TASK_URI = "/v1/task/";

    private final HttpClient httpClient;
    private final URI location;
    private final URI taskUri;
    private final TaskExecutionId taskExecutionId;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<BatchTaskUpdateRequest> taskUpdateRequestCodec;
    private final Duration infoRefreshMaxWait;

    public PrestoSparkHttpTaskClient(
            HttpClient httpClient,
            TaskExecutionId taskExecutionId,
            URI location,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<BatchTaskUpdateRequest> taskUpdateRequestCodec,
            Duration infoRefreshMaxWait)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.taskExecutionId = requireNonNull(taskExecutionId, "taskId is null");
        this.location = requireNonNull(location, "location is null");
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        this.taskUpdateRequestCodec = requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        this.taskUri = getTaskUri(location, taskExecutionId);
        this.infoRefreshMaxWait = requireNonNull(infoRefreshMaxWait, "infoRefreshMaxWait is null");
    }

    /**
     * Get results from a native engine task that ends with none shuffle operator. It always fetches from a single buffer.
     */
    @Override
    public ListenableFuture<PageBufferClient.PagesResponse> getResults(
            long token,
            DataSize maxResponseSize)
    {
        URI uri = uriBuilderFrom(taskUri)
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
        URI uri = uriBuilderFrom(taskUri)
                .appendPath("/results/0")
                .appendPath(String.valueOf(nextToken))
                .appendPath("acknowledge")
                .build();
        httpClient.executeAsync(
                prepareGet().setUri(uri).build(),
                new ResponseHandler<Void, RuntimeException>()
                {
                    @Override
                    public Void handleException(Request request, Exception exception)
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
        return httpClient.executeAsync(
                prepareDelete().setUri(
                        uriBuilderFrom(taskUri)
                                .appendPath("/results/0")
                                .build())
                        .build(),
                createStatusResponseHandler());
    }

    @Override
    public Throwable rewriteException(Throwable throwable)
    {
        return null;
    }

    public ListenableFuture<BaseResponse<TaskInfo>> getTaskInfo()
    {
        Request request = setContentTypeHeaders(false, prepareGet())
                .setHeader(PRESTO_MAX_WAIT, infoRefreshMaxWait.toString())
                .setUri(taskUri)
                .build();
        ResponseHandler responseHandler = createAdaptingJsonResponseHandler(taskInfoCodec);
        return httpClient.executeAsync(request, responseHandler);
    }

    public ListenableFuture<BaseResponse<TaskInfo>> updateTask(
            List<TaskSource> sources,
            PlanFragment planFragment,
            TableWriteInfo tableWriteInfo,
            Optional<String> shuffleWriteInfo,
            Session session,
            OutputBuffers outputBuffers)
    {
        Optional<byte[]> fragment = Optional.of(planFragment.toBytes(planFragmentCodec));
        Optional<TableWriteInfo> writeInfo = Optional.of(tableWriteInfo);
        TaskUpdateRequest updateRequest = new TaskUpdateRequest(
                session.toSessionRepresentation(),
                session.getIdentity().getExtraCredentials(),
                fragment,
                sources,
                outputBuffers,
                writeInfo);
        BatchTaskUpdateRequest batchTaskUpdateRequest = new BatchTaskUpdateRequest(updateRequest, shuffleWriteInfo);

        URI batchTaskUri = uriBuilderFrom(taskUri)
                .appendPath("batch")
                .build();
        log.info(format("BatchTaskUpdate: \n %s", taskUpdateRequestCodec.toJson(batchTaskUpdateRequest)));
        return httpClient.executeAsync(
                setContentTypeHeaders(false, preparePost())
                        .setUri(batchTaskUri)
                        .setBodyGenerator(createStaticBodyGenerator(taskUpdateRequestCodec.toBytes(batchTaskUpdateRequest)))
                        .build(),
                createAdaptingJsonResponseHandler(taskInfoCodec));
    }

    public URI getLocation()
    {
        return location;
    }

    private URI getTaskUri(URI baseUri, TaskExecutionId taskExecutionId)
    {
        return uriBuilderFrom(baseUri)
                .appendPath(TASK_URI)
                .appendPath(taskExecutionId.toString())
                .build();
    }
}
