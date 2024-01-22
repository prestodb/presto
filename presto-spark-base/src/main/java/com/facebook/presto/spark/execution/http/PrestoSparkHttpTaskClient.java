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
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.HttpRpcShuffleClient.PageResponseHandler;
import com.facebook.presto.operator.PageBufferClient.PagesResponse;
import com.facebook.presto.server.RequestErrorTracker;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.http.client.HttpStatus.familyForStatusCode;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * An abstraction of HTTP client that communicates with the locally running Presto worker process. It exposes worker endpoints to simple method calls.
 */
@ThreadSafe
public class PrestoSparkHttpTaskClient
{
    private static final String TASK_URI = "/v1/task/";

    private final HttpClient httpClient;
    private final URI location;
    private final URI taskUri;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<BatchTaskUpdateRequest> taskUpdateRequestCodec;
    private final Duration infoRefreshMaxWait;
    private final ScheduledExecutorService errorRetryScheduledExecutor;
    private final Duration remoteTaskMaxErrorDuration;

    public PrestoSparkHttpTaskClient(
            HttpClient httpClient,
            TaskId taskId,
            URI location,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<BatchTaskUpdateRequest> taskUpdateRequestCodec,
            Duration infoRefreshMaxWait,
            ScheduledExecutorService errorRetryScheduledExecutor,
            Duration remoteTaskMaxErrorDuration)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.location = requireNonNull(location, "location is null");
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        this.taskUpdateRequestCodec = requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        this.taskUri = createTaskUri(location, taskId);
        this.infoRefreshMaxWait = requireNonNull(infoRefreshMaxWait, "infoRefreshMaxWait is null");
        this.errorRetryScheduledExecutor = requireNonNull(errorRetryScheduledExecutor, "errorRetryScheduledExecutor is null");
        this.remoteTaskMaxErrorDuration = requireNonNull(remoteTaskMaxErrorDuration, "remoteTaskMaxErrorDuration is null");
    }

    /**
     * Get results from a native engine task that ends with none shuffle operator. It always fetches from a single buffer.
     */
    public ListenableFuture<PagesResponse> getResults(long token, DataSize maxResponseSize)
    {
        RequestErrorTracker errorTracker = new RequestErrorTracker(
                "NativeExecution",
                location,
                NATIVE_EXECUTION_TASK_ERROR,
                "getResults encountered too many errors talking to native process",
                remoteTaskMaxErrorDuration,
                errorRetryScheduledExecutor,
                "sending update request to native process");
        SettableFuture<PagesResponse> result = SettableFuture.create();
        scheduleGetResultsRequest(token, maxResponseSize, errorTracker, result);
        return result;
    }

    private void scheduleGetResultsRequest(
            long token,
            DataSize maxResponseSize,
            RequestErrorTracker errorTracker,
            SettableFuture<PagesResponse> result)
    {
        ListenableFuture<PagesResponse> responseFuture = transformAsync(
                errorTracker.acquireRequestPermit(),
                ignored -> {
                    errorTracker.startRequest();
                    return httpClient.executeAsync(prepareGetResultsRequest(token, maxResponseSize), new PageResponseHandler());
                },
                errorRetryScheduledExecutor);
        addCallback(responseFuture, new FutureCallback<PagesResponse>()
        {
            @Override
            public void onSuccess(PagesResponse response)
            {
                errorTracker.requestSucceeded();
                result.set(response);
            }

            @Override
            public void onFailure(Throwable failure)
            {
                if (failure instanceof PrestoException) {
                    // do not retry on PrestoException
                    result.setException(failure);
                    return;
                }
                try {
                    errorTracker.requestFailed(failure);
                    scheduleGetResultsRequest(token, maxResponseSize, errorTracker, result);
                }
                catch (Throwable t) {
                    result.setException(t);
                }
            }
        }, errorRetryScheduledExecutor);
    }

    private Request prepareGetResultsRequest(long token, DataSize maxResponseSize)
    {
        return prepareGet()
                .setHeader(PRESTO_MAX_SIZE, maxResponseSize.toString())
                .setUri(uriBuilderFrom(taskUri)
                        .appendPath("/results/0")
                        .appendPath(String.valueOf(token))
                        .build())
                .build();
    }

    public void acknowledgeResultsAsync(long nextToken)
    {
        URI uri = uriBuilderFrom(taskUri)
                .appendPath("/results/0")
                .appendPath(String.valueOf(nextToken))
                .appendPath("acknowledge")
                .build();
        httpExecuteAsync(prepareGet().setUri(uri).build(), null);
    }

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

    public ListenableFuture<BaseResponse<TaskInfo>> getTaskInfo()
    {
        Request request = setContentTypeHeaders(false, prepareGet())
                .setHeader(PRESTO_MAX_WAIT, infoRefreshMaxWait.toString())
                .setUri(taskUri)
                .build();

        return httpExecuteAsync(request, taskInfoCodec);
    }

    private <T> ListenableFuture<BaseResponse<T>> httpExecuteAsync(Request request, JsonCodec<T> codec)
    {
        return httpClient.executeAsync(request, new ResponseHandler<BaseResponse<T>, RuntimeException>()
        {
            @Override
            public BaseResponse<T> handleException(Request request, Exception exception)
            {
                throw propagate(request, exception);
            }

            @Override
            public BaseResponse<T> handle(Request request, Response response)
            {
                if (familyForStatusCode(response.getStatusCode()) != HttpStatus.Family.SUCCESSFUL) {
                    throw new RuntimeException(format("Unexpected http response code: %s", response.getStatusCode()));
                }

                if (codec == null) {
                    return null;
                }

                try {
                    return createAdaptingJsonResponseHandler(codec).handle(request, response);
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public BaseResponse<TaskInfo> updateTask(
            List<TaskSource> sources,
            PlanFragment planFragment,
            TableWriteInfo tableWriteInfo,
            Optional<String> shuffleWriteInfo,
            Optional<String> broadcastBasePath,
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
        BatchTaskUpdateRequest batchTaskUpdateRequest = new BatchTaskUpdateRequest(updateRequest, shuffleWriteInfo, broadcastBasePath);

        URI batchTaskUri = uriBuilderFrom(taskUri)
                .appendPath("batch")
                .build();
        return httpClient.execute(
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

    public URI getTaskUri()
    {
        return taskUri;
    }

    private URI createTaskUri(URI baseUri, TaskId taskId)
    {
        return uriBuilderFrom(baseUri)
                .appendPath(TASK_URI)
                .appendPath(taskId.toString())
                .build();
    }
}
