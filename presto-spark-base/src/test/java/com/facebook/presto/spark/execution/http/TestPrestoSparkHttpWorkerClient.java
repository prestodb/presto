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
import com.facebook.airlift.http.client.RequestStats;
import com.facebook.airlift.http.client.Response;
import com.facebook.airlift.http.client.ResponseHandler;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.spark.execution.HttpNativeExecutionTaskResultFetcher;
import com.facebook.presto.spi.page.PageCodecMarker;
import com.facebook.presto.spi.page.PagesSerdeUtil;
import com.facebook.presto.spi.page.SerializedPage;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilder;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.fail;

public class TestPrestoSparkHttpWorkerClient
{
    private static final String TASK_ROOT_PATH = "/v1/task";
    private static final URI BASE_URI = uriBuilder()
            .scheme("http")
            .host("localhost")
            .port(8080)
            .build();
    private static final Duration NO_DURATION = new Duration(0, TimeUnit.MILLISECONDS);

    @Test
    public void testResultGet()
    {
        TaskId taskId = new TaskId(
                "testid",
                0,
                0,
                0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(new TestingHttpClient(NO_DURATION), taskId, uri);
        ListenableFuture<PageBufferClient.PagesResponse> future = workerClient.getResults(
                0,
                new DataSize(32, DataSize.Unit.MEGABYTE));
        try {
            PageBufferClient.PagesResponse page = future.get();
            assertEquals(0, page.getToken());
            assertEquals(true, page.isClientComplete());
            assertEquals(taskId.toString(), page.getTaskInstanceId());
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testResultAcknowledge()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(new TestingHttpClient(NO_DURATION), taskId, uri);
        workerClient.acknowledgeResultsAsync(1);
    }

    @Test
    public void testResultAbort()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(new TestingHttpClient(NO_DURATION), taskId, uri);
        ListenableFuture<?> future = workerClient.abortResults();
        try {
            future.get();
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testResultFetcher()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(new TestingHttpClient(NO_DURATION), taskId, uri);
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(1),
                workerClient,
                taskId,
                Optional.of(new Duration(30, TimeUnit.SECONDS)));
        CompletableFuture<Void> future = taskResultFetcher.start();
        try {
            future.get();
            List<SerializedPage> pages = new ArrayList<>();
            Optional<SerializedPage> page = taskResultFetcher.pollPage();
            while (page.isPresent()) {
                pages.add(page.get());
                page = taskResultFetcher.pollPage();
            }

            assertEquals(1, pages.size());
            assertEquals(0, pages.get(0).getSizeInBytes());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testResultFetcherFail()
    {
        // Test request timeout.
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(new Duration(500, TimeUnit.MILLISECONDS)),
                taskId,
                uri);
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(1),
                workerClient,
                taskId,
                Optional.of(new Duration(200, TimeUnit.MILLISECONDS)));
        CompletableFuture<Void> future = taskResultFetcher.start();
        assertThrows(ExecutionException.class, future::get);
    }

    private static class TestingHttpResponseFuture<T>
            extends CompletableFuture<T>
            implements HttpClient.HttpResponseFuture<T>
    {
        @Override
        public String getState()
        {
            return null;
        }

        @Override
        public void addListener(Runnable listener, Executor executor)
        {
        }
    }

    private static class TestingHttpClient
            implements com.facebook.airlift.http.client.HttpClient
    {
        private final Duration mockDelay;
        private final ScheduledExecutorService executor;

        public TestingHttpClient(Duration mockDelay)
        {
            this.mockDelay = mockDelay;
            this.executor = newScheduledThreadPool(1);
        }

        @Override
        public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler) throws E
        {
            try {
                return executeAsync(request, responseHandler).get();
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
        {
            TestingHttpResponseFuture<T> future = new TestingHttpResponseFuture<T>();
            executor.schedule(
                    () ->
                    {
                        URI uri = request.getUri();
                        String method = request.getMethod();
                        ListMultimap<String, String> headers = request.getHeaders();
                        String path = uri.getPath();
                        String taskId = getTaskId(uri);
                        if (method.equalsIgnoreCase("GET")) {
                            if (path.contains("/results")) {
                                // GET /v1/task/{taskId}/results/{bufferId}/{token}/acknowledge
                                if (path.contains("acknowledge")) {
                                    try {
                                        future.complete(responseHandler.handle(request, TestingResponse.createDummyResultResponse()));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        future.completeExceptionally(e);
                                    }
                                }
                                // GET /v1/task/{taskId}/results/{bufferId}/{token}
                                else {
                                    try {
                                        future.complete(responseHandler.handle(
                                                request,
                                                TestingResponse.createResultResponse(
                                                        HttpStatus.OK,
                                                        taskId,
                                                        0,
                                                        1,
                                                        true)));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        future.completeExceptionally(e);
                                    }
                                }
                            }
                        } else if (method.equalsIgnoreCase("DELETE")) {
                            // DELETE /v1/task/{taskId}
                            try {
                                future.complete(responseHandler.handle(request, TestingResponse.createDummyResultResponse()));
                            } catch (Exception e) {
                                e.printStackTrace();
                                future.completeExceptionally(e);
                            }
                        }
                        future.completeExceptionally(new Exception("Unknown path " + path));
                    },
                    (long) mockDelay.getValue(),
                    mockDelay.getUnit());
            return future;
        }

        @Override
        public RequestStats getStats()
        {
            return null;
        }

        @Override
        public long getMaxContentLength()
        {
            return 0;
        }

        @Override
        public void close()
        {
        }

        @Override
        public boolean isClosed()
        {
            return false;
        }

        private String getTaskId(URI uri)
        {
            String fromTaskId = uri.getPath().substring(TASK_ROOT_PATH.length() + 1);
            int endPosition = fromTaskId.indexOf("/");
            if (endPosition < 0) {
                return fromTaskId;
            }
            return fromTaskId.substring(0, endPosition);
        }
    }

    public static class TestingResponse
            implements Response
    {
        private final int statusCode;
        private final String statusMessage;
        private final ListMultimap<HeaderName, String> headers;
        private InputStream inputStream;

        private TestingResponse()
        {
            this.statusCode = HttpStatus.OK.code();
            this.statusMessage = HttpStatus.OK.toString();
            this.headers = ArrayListMultimap.create();
        }

        private TestingResponse(
                int statusCode,
                String statusMessage,
                ListMultimap<HeaderName, String> headers,
                InputStream inputStream)
        {
            this.statusCode = statusCode;
            this.statusMessage = statusMessage;
            this.headers = headers;
            this.inputStream = inputStream;
        }

        public static Response createDummyResultResponse()
        {
            return new TestingResponse();
        }

        public static Response createResultResponse(
                HttpStatus httpStatus,
                String taskId,
                long token,
                long nextToken,
                boolean bufferComplete)
        {
            DynamicSliceOutput slicedOutput = new DynamicSliceOutput(1024);
            SerializedPage serializedPage = new SerializedPage(
                    EMPTY_SLICE,
                    PageCodecMarker.none(),
                    0,
                    0,
                    0);
            PagesSerdeUtil.writeSerializedPage(slicedOutput, serializedPage);
            ListMultimap<HeaderName, String> headers = ArrayListMultimap.create();
            headers.put(HeaderName.of(PRESTO_PAGE_TOKEN), String.valueOf(token));
            headers.put(HeaderName.of(PRESTO_PAGE_NEXT_TOKEN), String.valueOf(nextToken));
            headers.put(HeaderName.of(PRESTO_BUFFER_COMPLETE), String.valueOf(bufferComplete));
            headers.put(HeaderName.of(PRESTO_TASK_INSTANCE_ID), taskId);
            headers.put(HeaderName.of(CONTENT_TYPE), PRESTO_PAGES_TYPE.toString());
            return new TestingResponse(
                    httpStatus.code(),
                    httpStatus.toString(),
                    headers,
                    slicedOutput.slice().getInput());
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
        public ListMultimap<HeaderName, String> getHeaders()
        {
            return headers;
        }

        @Override
        public long getBytesRead()
        {
            return 0;
        }

        @Override
        public InputStream getInputStream()
        {
            return inputStream;
        }
    }
}
