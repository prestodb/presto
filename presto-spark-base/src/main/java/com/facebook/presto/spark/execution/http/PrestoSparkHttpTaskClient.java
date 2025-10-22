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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.SliceInput;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.annotation.concurrent.ThreadSafe;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
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
import static com.google.common.net.MediaType.JSON_UTF_8;
import static com.google.common.util.concurrent.Futures.addCallback;
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
    private static final Logger log = Logger.get(PrestoSparkHttpTaskClient.class);
    private final OkHttpClient httpClient;
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
            OkHttpClient httpClient,
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
        ListenableFuture<Void> permitFuture = (ListenableFuture<Void>) errorTracker.acquireRequestPermit();
        addCallback(permitFuture, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void ignored)
            {
                errorTracker.startRequest();
                httpClient.newCall(request).enqueue(new Callback() {
                    @Override
                    public void onFailure(Call call, IOException e)
                    {
                        handleGetResultsFailure(e, errorTracker, request, result);
                    }

                    @Override
                    public void onResponse(Call call, Response response)
                    {
                        try {
                            BaseResponse<PagesResponse> baseResponse = new PageResponseHandler().handle(request, response);
                            if (baseResponse.hasValue()) {
                                errorTracker.requestSucceeded();
                                result.set(baseResponse.getValue());
                            }
                            else {
                                Exception exception = baseResponse.getException();
                                if (exception != null) {
                                    handleGetResultsFailure(exception, errorTracker, request, result);
                                }
                                else {
                                    handleGetResultsFailure(new RuntimeException("Empty response without exception"), errorTracker, request, result);
                                }
                            }
                        }
                        catch (Exception e) {
                            handleGetResultsFailure(e, errorTracker, request, result);
                        }
                        finally {
                            response.close();
                        }
                    }
                });
            }

            @Override
            public void onFailure(Throwable t)
            {
                result.setException(t);
            }
        }, executor);
    }

    private void handleGetResultsFailure(Throwable failure, RequestErrorTracker errorTracker,
                                         Request request, SettableFuture<PagesResponse> result)
    {
        log.info("Received failure response with exception %s", failure);
        if (Arrays.stream(failure.getSuppressed()).anyMatch(t -> t instanceof PrestoException)) {
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

    private Request prepareGetResultsRequest(long token, DataSize maxResponseSize)
    {
        HttpUrl url = HttpUrl.get(taskUri).newBuilder()
                .addPathSegment("results")
                .addPathSegment("0")
                .addPathSegment(String.valueOf(token))
                .build();

        return new Request.Builder()
                .url(url)
                .get()
                .addHeader(PRESTO_MAX_SIZE, maxResponseSize.toString())
                .build();
    }

    public void acknowledgeResultsAsync(long nextToken)
    {
        HttpUrl url = HttpUrl.get(taskUri).newBuilder()
                .addPathSegment("results")
                .addPathSegment("0")
                .addPathSegment(String.valueOf(nextToken))
                .addPathSegment("acknowledge")
                .build();
        Request request = new Request.Builder().url(url).get().build();

        // Execute asynchronously without waiting for result
        RequestErrorTracker errorTracker = new RequestErrorTracker(
                "NativeExecution",
                location,
                NATIVE_EXECUTION_TASK_ERROR,
                "acknowledgeResults encountered too many errors talking to native process",
                remoteTaskMaxErrorDuration,
                scheduledExecutorService,
                "acknowledge task results are received");
        SettableFuture<Void> result = SettableFuture.create();
        scheduleVoidRequest(request, new BytesResponseHandler(), errorTracker, result);
    }

    public ListenableFuture<Void> abortResultsAsync()
    {
        HttpUrl url = HttpUrl.get(taskUri).newBuilder()
                .addPathSegment("results")
                .addPathSegment("0")
                .build();
        Request request = new Request.Builder().url(url).delete().build();

        RequestErrorTracker errorTracker = new RequestErrorTracker(
                "NativeExecution",
                location,
                NATIVE_EXECUTION_TASK_ERROR,
                "abortResults encountered too many errors talking to native process",
                remoteTaskMaxErrorDuration,
                scheduledExecutorService,
                "abort task results");
        SettableFuture<Void> result = SettableFuture.create();
        scheduleVoidRequest(request, new BytesResponseHandler(), errorTracker, result);
        return result;
    }

    public TaskInfo getTaskInfo()
    {
        Request request = setContentTypeHeaders(new Request.Builder())
                .addHeader(PRESTO_MAX_WAIT, infoRefreshMaxWait.toString())
                .url(taskUri.toString())
                .get()
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

        HttpUrl url = HttpUrl.get(taskUri).newBuilder()
                .addPathSegment("batch")
                .build();
        byte[] requestBody = taskUpdateRequestCodec.toBytes(batchTaskUpdateRequest);
        Request request = setContentTypeHeaders(new Request.Builder())
                .url(url)
                .post(RequestBody.create(MediaType.parse(JSON_UTF_8.toString()), requestBody))
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
        return HttpUrl.get(baseUri).newBuilder()
                .addPathSegment("v1")
                .addPathSegment("task")
                .addPathSegment(taskId.toString())
                .build()
                .uri();
    }

    private <T> ListenableFuture<T> executeWithRetries(
            String name,
            String description,
            Request request,
            OkHttpResponseHandler<T> responseHandler)
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
            OkHttpResponseHandler<T> responseHandler,
            RequestErrorTracker errorTracker,
            SettableFuture<T> result)
    {
        ListenableFuture<Void> permitFuture = (ListenableFuture<Void>) errorTracker.acquireRequestPermit();
        addCallback(permitFuture, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void ignored)
            {
                errorTracker.startRequest();
                httpClient.newCall(request).enqueue(new Callback() {
                    @Override
                    public void onFailure(Call call, IOException e)
                    {
                        handleFailure(e, errorTracker, request, responseHandler, result);
                    }

                    @Override
                    public void onResponse(Call call, Response response) throws IOException
                    {
                        try {
                            BaseResponse<T> baseResponse = responseHandler.handle(request, response);
                            SimpleHttpResponseCallback<T> callback = new SimpleHttpResponseCallback<T>() {
                                @Override
                                public void success(T value)
                                {
                                    errorTracker.requestSucceeded();
                                    result.set(value);
                                }

                                @Override
                                public void failed(Throwable failure)
                                {
                                    handleFailure(failure, errorTracker, request, responseHandler, result);
                                }

                                @Override
                                public void fatal(Throwable cause)
                                {
                                    result.setException(cause);
                                }
                            };

                            addCallback(Futures.immediateFuture(baseResponse),
                                    new SimpleHttpResponseHandler<>(
                                            callback,
                                            location,
                                            new SimpleHttpResponseHandlerStats(),
                                            REMOTE_TASK_ERROR),
                                    executor);
                        }
                        catch (Exception e) {
                            handleFailure(e, errorTracker, request, responseHandler, result);
                        }
                        finally {
                            response.close();
                        }
                    }
                });
            }

            public void onFailure(Throwable t)
            {
                result.setException(t);
            }
        }, executor);
    }

    private <T> void handleFailure(Throwable failure, RequestErrorTracker errorTracker,
                                   Request request, OkHttpResponseHandler<T> responseHandler,
                                   SettableFuture<T> result)
    {
        if (failure instanceof PrestoException) {
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

    private <T> void scheduleVoidRequest(
            Request request,
            OkHttpResponseHandler<T> responseHandler,
            RequestErrorTracker errorTracker,
            SettableFuture<Void> result)
    {
        ListenableFuture<Void> permitFuture = (ListenableFuture<Void>) errorTracker.acquireRequestPermit();
        addCallback(permitFuture, new FutureCallback<Void>() {
            @Override
            public void onSuccess(Void ignored)
            {
                errorTracker.startRequest();
                httpClient.newCall(request).enqueue(new Callback() {
                    @Override
                    public void onFailure(Call call, IOException e)
                    {
                        handleVoidFailure(e, errorTracker, request, responseHandler, result);
                    }

                    @Override
                    public void onResponse(Call call, Response response)
                    {
                        try {
                            responseHandler.handle(request, response);
                            errorTracker.requestSucceeded();
                            result.set(null); // For void requests, we just complete with null
                        }
                        catch (Exception e) {
                            handleVoidFailure(e, errorTracker, request, responseHandler, result);
                        }
                        finally {
                            response.close();
                        }
                    }
                });
            }

            @Override
            public void onFailure(Throwable t)
            {
                result.setException(t);
            }
        }, executor);
    }

    private <T> void handleVoidFailure(Throwable failure, RequestErrorTracker errorTracker,
                                       Request request, OkHttpResponseHandler<T> responseHandler,
                                       SettableFuture<Void> result)
    {
        if (failure instanceof PrestoException) {
            result.setException(failure);
            return;
        }
        try {
            errorTracker.requestFailed(failure);
            scheduleVoidRequest(request, responseHandler, errorTracker, result);
        }
        catch (Throwable t) {
            result.setException(t);
        }
    }

    private static class BytesResponseHandler
            implements OkHttpResponseHandler<byte[]>
    {
        @Override
        public BaseResponse<byte[]> handle(Request request, Response response) throws IOException
        {
            return new BytesResponse(
                    response.code(),
                    convertHeaders(response),
                    readResponseBytes(response));
        }

        private static ListMultimap<OkHttpHeaderName, String> convertHeaders(Response response)
        {
            ImmutableListMultimap.Builder<OkHttpHeaderName, String> builder = ImmutableListMultimap.builder();
            for (String name : response.headers().names()) {
                for (String value : response.headers().values(name)) {
                    builder.put(OkHttpHeaderName.of(name), value);
                }
            }
            return builder.build();
        }

        private static byte[] readResponseBytes(Response response)
        {
            try {
                ResponseBody body = response.body();
                if (body == null) {
                    return new byte[] {};
                }
                return body.bytes();
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
        private final ListMultimap<OkHttpHeaderName, String> headers;
        private final byte[] bytes;

        public BytesResponse(int statusCode, ListMultimap<OkHttpHeaderName, String> headers, byte[] bytes)
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
            List<String> values = getHeaders(name);
            return values.isEmpty() ? null : values.get(0);
        }

        @Override
        public List<String> getHeaders(String name)
        {
            return headers.get(OkHttpHeaderName.of(name));
        }

        @Override
        public ListMultimap<OkHttpHeaderName, String> getHeaders()
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

    private static class PagesBaseResponse
            implements BaseResponse<PagesResponse>
    {
        private final int statusCode;
        private final ListMultimap<OkHttpHeaderName, String> headers;
        private final PagesResponse pagesResponse;

        public PagesBaseResponse(int statusCode, ListMultimap<OkHttpHeaderName, String> headers, PagesResponse pagesResponse)
        {
            this.statusCode = statusCode;
            this.headers = ImmutableListMultimap.copyOf(requireNonNull(headers, "headers is null"));
            this.pagesResponse = requireNonNull(pagesResponse, "pagesResponse is null");
        }

        @Override
        public int getStatusCode()
        {
            return statusCode;
        }

        @Override
        public String getHeader(String name)
        {
            List<String> values = getHeaders(name);
            return values.isEmpty() ? null : values.get(0);
        }

        @Override
        public List<String> getHeaders(String name)
        {
            return headers.get(OkHttpHeaderName.of(name));
        }

        @Override
        public ListMultimap<OkHttpHeaderName, String> getHeaders()
        {
            return headers;
        }

        @Override
        public boolean hasValue()
        {
            return pagesResponse != null;
        }

        @Override
        public PagesResponse getValue()
        {
            return pagesResponse;
        }

        @Override
        public int getResponseSize()
        {
            return 0; // Not used for PagesResponse
        }

        @Override
        public byte[] getResponseBytes()
        {
            return new byte[0]; // Not used for PagesResponse
        }

        @Override
        public Exception getException()
        {
            return null;
        }
    }

    public static class PageResponseHandler
            implements OkHttpResponseHandler<PagesResponse>
    {
        @Override
        public BaseResponse<PagesResponse> handle(Request request, Response response) throws IOException
        {
            try {
                // no content means no content was created within the wait period, but query is still ok
                // if job is finished, complete is set in the response
                if (response.code() == 204) {
                    PagesResponse pagesResponse = createEmptyPagesResponse(
                            getTaskInstanceId(request, response),
                            getToken(request, response),
                            getNextToken(request, response),
                            getComplete(request, response));
                    return new PagesBaseResponse(response.code(), convertHeaders(response), pagesResponse);
                }

                // otherwise we must have gotten an OK response, everything else is considered fatal
                if (response.code() != 200) {
                    StringBuilder body = new StringBuilder();
                    try {
                        ResponseBody responseBody = response.body();
                        if (responseBody != null) {
                            try (BufferedReader reader = new BufferedReader(new InputStreamReader(responseBody.byteStream(), UTF_8))) {
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
                        }
                    }
                    catch (RuntimeException | IOException e) {
                        // Ignored. Just return whatever message we were able to decode
                    }
                    throw new PageTransportErrorException(
                            HostAddress.fromUri(request.url().uri()),
                            format("Expected response code to be 200, but was %s:%n%s",
                                    response.code(),
                                    body.toString()));
                }

                // invalid content type can happen when an error page is returned, but is unlikely given the above 200
                String contentType = response.header(CONTENT_TYPE);
                if (contentType == null) {
                    throw new PageTransportErrorException(
                            HostAddress.fromUri(request.url().uri()),
                            format("%s header is not set: %s", CONTENT_TYPE, response));
                }
                if (!mediaTypeMatches(contentType, PRESTO_PAGES_TYPE)) {
                    throw new PageTransportErrorException(
                            HostAddress.fromUri(request.url().uri()),
                            format("Expected %s response from server but got %s", PRESTO_PAGES_TYPE, contentType));
                }

                String taskInstanceId = getTaskInstanceId(request, response);
                long token = getToken(request, response);
                long nextToken = getNextToken(request, response);
                boolean complete = getComplete(request, response);

                ResponseBody responseBody = response.body();
                if (responseBody == null) {
                    throw new PageTransportErrorException(
                            HostAddress.fromUri(request.url().uri()),
                            "Response body is null");
                }

                SliceInput input = new InputStreamSliceInput(responseBody.byteStream());
                List<SerializedPage> pages = ImmutableList.copyOf(readSerializedPages(input));
                PagesResponse pagesResponse = createPagesResponse(taskInstanceId, token, nextToken, pages, complete);
                return new PagesBaseResponse(response.code(), convertHeaders(response), pagesResponse);
            }
            catch (PageTransportErrorException e) {
                throw new PageTransportErrorException(
                        e.getRemoteHost(),
                        "Error fetching " + request.url(),
                        e);
            }
        }

        private static ListMultimap<OkHttpHeaderName, String> convertHeaders(Response response)
        {
            ImmutableListMultimap.Builder<OkHttpHeaderName, String> builder = ImmutableListMultimap.builder();
            for (String name : response.headers().names()) {
                for (String value : response.headers().values(name)) {
                    builder.put(OkHttpHeaderName.of(name), value);
                }
            }
            return builder.build();
        }

        private static String getTaskInstanceId(Request request, Response response)
        {
            String taskInstanceId = response.header(PRESTO_TASK_INSTANCE_ID);
            if (taskInstanceId == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.url().uri()), format("Expected %s header", PRESTO_TASK_INSTANCE_ID));
            }
            return taskInstanceId;
        }

        private static long getToken(Request request, Response response)
        {
            String tokenHeader = response.header(PRESTO_PAGE_TOKEN);
            if (tokenHeader == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.url().uri()), format("Expected %s header", PRESTO_PAGE_TOKEN));
            }
            return Long.parseLong(tokenHeader);
        }

        private static long getNextToken(Request request, Response response)
        {
            String nextTokenHeader = response.header(PRESTO_PAGE_NEXT_TOKEN);
            if (nextTokenHeader == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.url().uri()), format("Expected %s header", PRESTO_PAGE_NEXT_TOKEN));
            }
            return Long.parseLong(nextTokenHeader);
        }

        private static boolean getComplete(Request request, Response response)
        {
            String bufferComplete = response.header(PRESTO_BUFFER_COMPLETE);
            if (bufferComplete == null) {
                throw new PageTransportErrorException(HostAddress.fromUri(request.url().uri()), format("Expected %s header", PRESTO_BUFFER_COMPLETE));
            }
            return Boolean.parseBoolean(bufferComplete);
        }

        private static boolean mediaTypeMatches(String value, com.google.common.net.MediaType range)
        {
            try {
                com.google.common.net.MediaType parsedMediaType = com.google.common.net.MediaType.parse(value);
                return parsedMediaType.is(range);
            }
            catch (IllegalArgumentException | IllegalStateException e) {
                return false;
            }
        }
    }
}
