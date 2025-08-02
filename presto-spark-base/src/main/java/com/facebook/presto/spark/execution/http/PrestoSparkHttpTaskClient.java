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
import com.facebook.airlift.http.client.HttpStatus;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.PageBufferClient.PagesResponse;
import com.facebook.presto.operator.PageTransportErrorException;
import com.facebook.presto.server.SimpleHttpResponseCallback;
import com.facebook.presto.server.SimpleHttpResponseHandlerStats;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spark.execution.http.server.RequestErrorTracker;
import com.facebook.presto.spark.execution.http.server.SimpleHttpResponseHandler;
import com.facebook.presto.spark.execution.http.server.smile.BaseResponse;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.io.ByteStreams;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;

import javax.annotation.concurrent.ThreadSafe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.http.client.Request.Builder.prepareDelete;
import static com.facebook.airlift.http.client.Request.Builder.prepareGet;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.airlift.http.client.ResponseHandlerUtils.propagate;
import static com.facebook.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.operator.PageBufferClient.PagesResponse.createEmptyPagesResponse;
import static com.facebook.presto.operator.PageBufferClient.PagesResponse.createPagesResponse;
import static com.facebook.presto.spark.execution.http.server.RequestHelpers.setContentTypeHeaders;
import static com.facebook.presto.spark.execution.http.server.smile.AdaptingJsonResponseHandler.createAdaptingJsonResponseHandler;
import static com.facebook.presto.spi.StandardErrorCode.NATIVE_EXECUTION_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.page.PagesSerdeUtil.readSerializedPages;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.util.concurrent.Futures.addCallback;
import static com.google.common.util.concurrent.Futures.transformAsync;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
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
    private final Executor executor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final Duration remoteTaskMaxErrorDuration;

    public PrestoSparkHttpTaskClient(
            HttpClient httpClient,
            TaskId taskId,
            URI location,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<BatchTaskUpdateRequest> taskUpdateRequestCodec,
            Duration infoRefreshMaxWait,
            Executor executor,
            ScheduledExecutorService scheduledExecutorService,
            Duration remoteTaskMaxErrorDuration)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.location = requireNonNull(location, "location is null");
        this.taskInfoCodec = requireNonNull(taskInfoCodec, "taskInfoCodec is null");
        this.planFragmentCodec = requireNonNull(planFragmentCodec, "planFragmentCodec is null");
        this.taskUpdateRequestCodec = requireNonNull(taskUpdateRequestCodec, "taskUpdateRequestCodec is null");
        this.taskUri = createTaskUri(location, taskId);
        this.infoRefreshMaxWait = requireNonNull(infoRefreshMaxWait, "infoRefreshMaxWait is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.scheduledExecutorService = requireNonNull(scheduledExecutorService, "scheduledExecutorService is null");
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
                scheduledExecutorService,
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
                executor);
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
        }, executor);
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
                "get remote task info",
                request,
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
        Optional<byte[]> fragment = Optional.of(planFragment.bytesForTaskSerialization(planFragmentCodec));
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
                scheduledExecutorService,
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
                executor);
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
                executor);
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
        private final ListMultimap<HeaderName, String> headers;
        private final byte[] bytes;

        public BytesResponse(int statusCode, ListMultimap<HeaderName, String> headers, byte[] bytes)
        {
            this.statusCode = statusCode;
            this.headers = ImmutableListMultimap.copyOf(requireNonNull(headers, "headers is null"));
            this.bytes = bytes;
        }

        @Override
        public int getStatusCode()
        {
            return statusCode;
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

    public static class PageResponseHandler
            implements ResponseHandler<PagesResponse, RuntimeException>
    {
        @Override
        public PagesResponse handleException(Request request, Exception exception)
        {
            throw propagate(request, exception);
        }

        @Override
        public PagesResponse handle(Request request, Response response)
        {
            try {
                // no content means no content was created within the wait period, but query is still ok
                // if job is finished, complete is set in the response
                if (response.getStatusCode() == HttpStatus.NO_CONTENT.code()) {
                    return createEmptyPagesResponse(
                            getTaskInstanceId(request, response),
                            getToken(request, response),
                            getNextToken(request, response),
                            getComplete(request, response));
                }

                // otherwise we must have gotten an OK response, everything else is considered fatal
                if (response.getStatusCode() != HttpStatus.OK.code()) {
                    StringBuilder body = new StringBuilder();
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream(), UTF_8))) {
                        // Get up to 1000 lines for debugging
                        for (int i = 0; i < 1000; i++) {
                            String line = reader.readLine();
                            // Don't output more than 100KB
                            if (line == null || body.length() + line.length() > 100 * 1024) {
                                break;
                            }
                            body.append(line + "\n");
                        }
                    }
                    catch (RuntimeException | IOException e) {
                        // Ignored. Just return whatever message we were able to decode
                    }
                    throw new PageTransportErrorException(
                            HostAddress.fromUri(request.getUri()),
                            format("Expected response code to be 200, but was %s:%n%s",
                                    response.getStatusCode(),
                                    body.toString()));
                }

                // invalid content type can happen when an error page is returned, but is unlikely given the above 200
                String contentType = response.getHeader(CONTENT_TYPE);
                if (contentType == null) {
                    throw new PageTransportErrorException(
                            HostAddress.fromUri(request.getUri()),
                            format("%s header is not set: %s", CONTENT_TYPE, response));
                }
                if (!mediaTypeMatches(contentType, PRESTO_PAGES_TYPE)) {
                    throw new PageTransportErrorException(
                            HostAddress.fromUri(request.getUri()),
                            format("Expected %s response from server but got %s", PRESTO_PAGES_TYPE, contentType));
                }

                String taskInstanceId = getTaskInstanceId(request, response);
                long token = getToken(request, response);
                long nextToken = getNextToken(request, response);
                boolean complete = getComplete(request, response);

                try (SliceInput input = new InputStreamSliceInput(response.getInputStream())) {
                    List<SerializedPage> pages = ImmutableList.copyOf(readSerializedPages(input));
                    return createPagesResponse(taskInstanceId, token, nextToken, pages, complete);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            catch (PageTransportErrorException e) {
                throw new PageTransportErrorException(
                        e.getRemoteHost(),
                        "Error fetching " + request.getUri().toASCIIString(),
                        e);
            }
        }

        private static String getTaskInstanceId(Request request, Response response)
        {
            String taskInstanceId = response.getHeader(PRESTO_TASK_INSTANCE_ID);
            if (taskInstanceId == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.getUri()), format("Expected %s header", PRESTO_TASK_INSTANCE_ID));
            }
            return taskInstanceId;
        }

        private static long getToken(Request request, Response response)
        {
            String tokenHeader = response.getHeader(PRESTO_PAGE_TOKEN);
            if (tokenHeader == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.getUri()), format("Expected %s header", PRESTO_PAGE_TOKEN));
            }
            return Long.parseLong(tokenHeader);
        }

        private static long getNextToken(Request request, Response response)
        {
            String nextTokenHeader = response.getHeader(PRESTO_PAGE_NEXT_TOKEN);
            if (nextTokenHeader == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.getUri()), format("Expected %s header", PRESTO_PAGE_NEXT_TOKEN));
            }
            return Long.parseLong(nextTokenHeader);
        }

        private static boolean getComplete(Request request, Response response)
        {
            String bufferComplete = response.getHeader(PRESTO_BUFFER_COMPLETE);
            if (bufferComplete == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.getUri()), format("Expected %s header", PRESTO_BUFFER_COMPLETE));
            }
            return Boolean.parseBoolean(bufferComplete);
        }

        private static boolean mediaTypeMatches(String value, MediaType range)
        {
            try {
                return MediaType.parse(value).is(range);
            }
            catch (IllegalArgumentException | IllegalStateException e) {
                return false;
            }
        }
    }
}
