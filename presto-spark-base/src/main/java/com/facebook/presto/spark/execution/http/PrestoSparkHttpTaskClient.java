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

import com.facebook.airlift.http.client.HeaderName;
import com.facebook.airlift.http.client.HttpClient;
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
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandler;
import com.facebook.presto.server.SimpleHttpResponseHandlerStats;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
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
        scheduleGetResultsRequest(prepareGetResultsRequest(token, maxResponseSize), errorTracker, result);
        return result;
    }

    private void scheduleGetResultsRequest(
            Request request,
            RequestErrorTracker errorTracker,
            SettableFuture<PagesResponse> result)
    {
        ListenableFuture<PagesResponse> responseFuture = transformAsync(
                errorTracker.acquireRequestPermit(),
                ignored -> {
                    errorTracker.startRequest();
                    return httpClient.executeAsync(request, new PageResponseHandler());
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
                    scheduleGetResultsRequest(request, errorTracker, result);
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
        Request request = prepareGet().setUri(uri).build();
        executeWithRetries("acknowledgeResults", "acknowledge task results are received", request, new BytesResponseHandler());
    }

    public ListenableFuture<Void> abortResultsAsync()
    {
        Request request = prepareDelete().setUri(
                        uriBuilderFrom(taskUri)
                                .appendPath("/results/0")
                                .build())
                .build();
        return asVoidFuture(executeWithRetries("abortResults", "abort task results", request, new BytesResponseHandler()));
    }

    private static ListenableFuture<Void> asVoidFuture(ListenableFuture<?> future)
    {
        return Futures.transform(future, (ignored) -> null, directExecutor());
    }

    public TaskInfo getTaskInfo()
    {
        Request request = setContentTypeHeaders(false, prepareGet())
                .setHeader(PRESTO_MAX_WAIT, infoRefreshMaxWait.toString())
                .setUri(taskUri)
                .build();
        ListenableFuture<TaskInfo> future = executeWithRetries(
                "getTaskInfo",
                "get remote task info", request,
                createAdaptingJsonResponseHandler(taskInfoCodec));
        return getFutureValue(future);
    }

    public TaskInfo updateTask(
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

        Request request = setContentTypeHeaders(false, preparePost())
                .setUri(uriBuilderFrom(taskUri)
                        .appendPath("batch")
                        .build())
                .setBodyGenerator(createStaticBodyGenerator(taskUpdateRequestCodec.toBytes(batchTaskUpdateRequest)))
                .build();
        ListenableFuture<TaskInfo> future = executeWithRetries(
                "updateTask",
                "create or update remote task",
                request,
                createAdaptingJsonResponseHandler(taskInfoCodec));
        return getFutureValue(future);
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

    private <T> ListenableFuture<T> executeWithRetries(
            String name,
            String description,
            Request request,
            ResponseHandler<BaseResponse<T>, RuntimeException> responseHandler)
    {
        RequestErrorTracker errorTracker = new RequestErrorTracker(
                "NativeExecution",
                location,
                NATIVE_EXECUTION_TASK_ERROR,
                name + " encountered too many errors talking to native process",
                remoteTaskMaxErrorDuration,
                errorRetryScheduledExecutor,
                description);
        SettableFuture<T> result = SettableFuture.create();
        scheduleRequest(request, responseHandler, errorTracker, result);
        return result;
    }

    private <T> void scheduleRequest(
            Request request,
            ResponseHandler<BaseResponse<T>, RuntimeException> responseHandler,
            RequestErrorTracker errorTracker,
            SettableFuture<T> result)
    {
        ListenableFuture<BaseResponse<T>> responseFuture = transformAsync(
                errorTracker.acquireRequestPermit(),
                ignored -> {
                    errorTracker.startRequest();
                    return httpClient.executeAsync(request, responseHandler);
                },
                errorRetryScheduledExecutor);
        SimpleHttpResponseCallback<T> callback = new SimpleHttpResponseCallback<T>()
        {
            @Override
            public void success(T value)
            {
                result.set(value);
            }

            @Override
            public void failed(Throwable failure)
            {
                if (failure instanceof PrestoException) {
                    // do not retry on PrestoException
                    result.setException(failure);
                    return;
                }
                try {
                    errorTracker.requestFailed(failure);
                    scheduleRequest(request, responseHandler, errorTracker, result);
                }
                catch (Throwable t) {
                    result.setException(t);
                }
            }

            @Override
            public void fatal(Throwable cause)
            {
                result.setException(cause);
            }
        };
        addCallback(
                responseFuture,
                new SimpleHttpResponseHandler<>(
                        callback,
                        location,
                        new SimpleHttpResponseHandlerStats(),
                        REMOTE_TASK_ERROR),
                errorRetryScheduledExecutor);
    }

    private static class BytesResponseHandler
            implements ResponseHandler<BaseResponse<byte[]>, RuntimeException>
    {
        @Override
        public BaseResponse<byte[]> handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public BaseResponse<byte[]> handle(Request request, Response response)
        {

            return new BytesResponse(
                    response.getStatusCode(),
                    response.getStatusMessage(),
                    response.getHeaders(),
                    readResponseBytes(response));
        }

        private static byte[] readResponseBytes(Response response)
        {
            try {
                InputStream inputStream = response.getInputStream();
                if (inputStream == null) {
                    return new byte[] {};
                }
                return ByteStreams.toByteArray(inputStream);
            }
            catch (IOException e) {
                throw new RuntimeException("Error reading response from server", e);
            }
        }
    }

    private static class BytesResponse
            implements BaseResponse<byte[]>
    {
        private final int statusCode;
        private final String statusMessage;
        private final ListMultimap<HeaderName, String> headers;
        private final byte[] bytes;

        public BytesResponse(int statusCode, String statusMessage, ListMultimap<HeaderName, String> headers, byte[] bytes)
        {
            this.statusCode = statusCode;
            this.statusMessage = requireNonNull(statusMessage, "statusMessage is null");
            this.headers = ImmutableListMultimap.copyOf(requireNonNull(headers, "headers is null"));
            this.bytes = bytes;
        }

        @Override
        public int getStatusCode()
        {
            return statusCode;
        }

        @Override
        public String getStatusMessage()
        {
            return statusMessage;
        }

        @Override
        public String getHeader(String name)
        {
            List<String> values = getHeaders().get(HeaderName.of(name));
            return values.isEmpty() ? null : values.get(0);
        }

        @Override
        public List<String> getHeaders(String name)
        {
            return headers.get(HeaderName.of(name));
        }

        @Override
        public ListMultimap<HeaderName, String> getHeaders()
        {
            return headers;
        }

        @Override
        public boolean hasValue()
        {
            return true;
        }

        @Override
        public byte[] getValue()
        {
            return bytes;
        }

        @Override
        public int getResponseSize()
        {
            return bytes.length;
        }

        @Override
        public byte[] getResponseBytes()
        {
            return bytes;
        }

        @Override
        public Exception getException()
        {
            return null;
        }
    }
}
