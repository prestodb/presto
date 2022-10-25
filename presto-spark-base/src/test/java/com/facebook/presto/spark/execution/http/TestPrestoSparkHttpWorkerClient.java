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
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.HttpNativeExecutionTaskInfoFetcher;
import com.facebook.presto.spark.execution.HttpNativeExecutionTaskResultFetcher;
import com.facebook.presto.spark.execution.NativeExecutionTask;
import com.facebook.presto.spark.execution.NativeExecutionTaskFactory;
import com.facebook.presto.spi.page.PageCodecMarker;
import com.facebook.presto.spi.page.PagesSerdeUtil;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.net.MediaType;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilder;
import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.execution.TaskTestUtils.SPLIT;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.spark.execution.HttpNativeExecutionTaskInfoFetcher.GET_TASK_INFO_INTERVALS;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
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
    private static final JsonCodec<TaskInfo> TASK_INFO_JSON_CODEC = JsonCodec.jsonCodec(TaskInfo.class);
    private static final JsonCodec<PlanFragment> PLAN_FRAGMENT_JSON_CODEC = JsonCodec.jsonCodec(PlanFragment.class);
    private static final JsonCodec<TaskUpdateRequest> TASK_UPDATE_REQUEST_JSON_CODEC = JsonCodec.jsonCodec(TaskUpdateRequest.class);

    @Test
    public void testResultGet()
    {
        TaskId taskId = new TaskId(
                "testid",
                0,
                0,
                0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        ListenableFuture<PageBufferClient.PagesResponse> future = workerClient.getResults(
                0,
                new DataSize(32, MEGABYTE));
        try {
            PageBufferClient.PagesResponse page = future.get();
            assertEquals(0, page.getToken());
            assertEquals(true, page.isClientComplete());
            assertEquals(taskId.toString(), page.getTaskInstanceId());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testResultAcknowledge()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        workerClient.acknowledgeResultsAsync(1);
    }

    @Test
    public void testResultAbort()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        ListenableFuture<?> future = workerClient.abortResults();
        try {
            future.get();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGetTaskInfo()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        ListenableFuture<BaseResponse<TaskInfo>> future = workerClient.getTaskInfo();
        try {
            TaskInfo taskInfo = future.get().getValue();
            assertEquals(taskInfo.getTaskId().toString(), taskId.toString());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testUpdateTask()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);

        Set<ScheduledSplit> splits = new HashSet<>();
        splits.add(SPLIT);
        List<TaskSource> sources = new ArrayList<>();
        ListenableFuture<BaseResponse<TaskInfo>> future = workerClient.updateTask(
                sources,
                createPlanFragment(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()),
                TestingSession.testSessionBuilder().build(),
                createInitialEmptyOutputBuffers(PARTITIONED));

        try {
            TaskInfo taskInfo = future.get().getValue();
            assertEquals(taskInfo.getTaskId().toString(), taskId.toString());
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testResultFetcher()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(1),
                workerClient,
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
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testResultFetcherMultipleNonEmptyResults()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        int serializedPageSize = (int) new DataSize(1, MEGABYTE).toBytes();
        int numPages = 10;
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(
                        new TestingResponseManager(
                            taskId.toString(),
                                () -> NO_DURATION,
                            new TestingResponseManager.TestingResultResponseManager()
                            {
                                private int requestCount;

                                @Override
                                public Response createResultResponse(String taskId) throws Exception
                                {
                                    requestCount++;
                                    if (requestCount < numPages) {
                                        return createResultResponseHelper(
                                                HttpStatus.OK,
                                                taskId,
                                                requestCount - 1,
                                                requestCount,
                                                false,
                                                serializedPageSize);
                                    }
                                    else if (requestCount == numPages) {
                                        return createResultResponseHelper(
                                                HttpStatus.OK,
                                                taskId,
                                                requestCount - 1,
                                                requestCount,
                                                true,
                                                serializedPageSize);
                                    }
                                    else {
                                        throw new Exception("Retrieving results after buffer completion");
                                    }
                                }
                            })),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(1),
                workerClient,
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

            assertEquals(numPages, pages.size());
            for (int i = 0; i < numPages; i++) {
                assertEquals(pages.get(i).getSizeInBytes(), serializedPageSize);
            }
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail();
        }
    }

    private static class BreakingLimitResponseManager
            extends TestingResponseManager.TestingResultResponseManager
    {
        private final int serializedPageSize;
        private final int numPages;

        private int requestCount;

        public BreakingLimitResponseManager(int serializedPageSize, int numPages)
        {
            this.serializedPageSize = serializedPageSize;
            this.numPages = numPages;
        }

        @Override
        public Response createResultResponse(String taskId) throws Exception
        {
            requestCount++;
            if (requestCount < numPages) {
                return createResultResponseHelper(
                        HttpStatus.OK,
                        taskId,
                        requestCount - 1,
                        requestCount,
                        false,
                        serializedPageSize);
            }
            else if (requestCount == numPages) {
                return createResultResponseHelper(
                        HttpStatus.OK,
                        taskId,
                        requestCount - 1,
                        requestCount,
                        true,
                        serializedPageSize);
            }
            else {
                throw new Exception("Retrieving results after buffer completion");
            }
        }

        public int getRemainingPageCount()
        {
            return numPages - requestCount;
        }
    }

    @Test
    public void testResultFetcherExceedingBufferLimit()
    {
        int numPages = 10;
        int serializedPageSize = (int) new DataSize(32, MEGABYTE).toBytes();
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        BreakingLimitResponseManager breakingLimitResponseManager =
                new BreakingLimitResponseManager(serializedPageSize, numPages);

        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(
                        new TestingResponseManager(
                                taskId.toString(),
                                () -> NO_DURATION,
                                breakingLimitResponseManager)),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(10),
                workerClient,
                Optional.of(new Duration(30, TimeUnit.SECONDS)));
        CompletableFuture<Void> future = taskResultFetcher.start();
        try {
            Optional<SerializedPage> page = Optional.empty();
            while (!page.isPresent()) {
                page = taskResultFetcher.pollPage();
            }
            // Wait a bit for fetches to overwhelm memory.
            Thread.sleep(5000);
            assertEquals(breakingLimitResponseManager.getRemainingPageCount(), 5);
            List<SerializedPage> pages = new ArrayList<>();
            pages.add(page.get());
            while (pages.size() < numPages) {
                page = taskResultFetcher.pollPage();
                page.ifPresent(pages::add);
            }
            future.get();
            assertEquals(numPages, pages.size());
            for (int i = 0; i < numPages; i++) {
                assertEquals(pages.get(i).getSizeInBytes(), serializedPageSize);
            }
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail();
        }
    }

    private static class TimeoutResponseManager
            extends TestingResponseManager.TestingResultResponseManager
    {
        private final int serializedPageSize;
        private final int numPages;
        private final int numInitialTimeouts;

        private int requestCount;
        private int timeoutCount;

        public TimeoutResponseManager(int serializedPageSize, int numPages, int numInitialTimeouts)
        {
            this.serializedPageSize = serializedPageSize;
            this.numPages = numPages;
            this.numInitialTimeouts = numInitialTimeouts;
        }

        @Override
        public Response createResultResponse(String taskId) throws Exception
        {
            if (++timeoutCount <= numInitialTimeouts) {
                // Returning some random error stuff. This will not be handled by the server anyways as it is a timed out request
                return createResultResponseHelper(
                        HttpStatus.OK,
                        taskId,
                        9999,
                        9999,
                        false,
                        10);
            }
            requestCount++;
            if (requestCount < numPages) {
                return createResultResponseHelper(
                        HttpStatus.OK,
                        taskId,
                        requestCount - 1,
                        requestCount,
                        false,
                        serializedPageSize);
            }
            else if (requestCount == numPages) {
                return createResultResponseHelper(
                        HttpStatus.OK,
                        taskId,
                        requestCount - 1,
                        requestCount,
                        true,
                        serializedPageSize);
            }
            else {
                throw new Exception("Retrieving results after buffer completion");
            }
        }
    }

    @Test
    public void testResultFetcherRequestTimeoutRecovery()
    {
        int numPages = 10;
        int serializedPageSize = 0;
        // Time out count less than MAX_HTTP_TIMEOUT_RETRIES (5).
        // Expecting recovery from failed timed out requests
        int numTimeouts = 3;

        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        TimeoutResponseManager timeoutResponseManager =
                new TimeoutResponseManager(serializedPageSize, numPages, numTimeouts);

        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(
                        new TestingResponseManager(
                            taskId.toString(),
                            new TestingResponseManager.DelayGenerator()
                            {
                                private int requestCount;

                                @Override
                                public Duration getDelay()
                                {
                                    if (++requestCount <= numTimeouts) {
                                        // The timed out request duration in the test has to be LESS than
                                        // result fetching interval in order for each timed out request to
                                        // hit the handler to trigger expected testing behavior. If the
                                        // request duration is larger than fetching interval, the response
                                        // manager behavior for the timed-out request will be triggered in
                                        // a later fetching interval, causing unexpected behavior. Ideally
                                        // we want response manager behavior to be triggered within the
                                        // invoking fetching interval.
                                        return new Duration(150, TimeUnit.MILLISECONDS);
                                    }
                                    return NO_DURATION;
                                }
                            },
                            timeoutResponseManager)),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(10),
                workerClient,
                Optional.of(new Duration(75, TimeUnit.MILLISECONDS)));
        CompletableFuture<Void> future = taskResultFetcher.start();
        try {
            future.get();
            List<SerializedPage> pages = new ArrayList<>();
            Optional<SerializedPage> page = taskResultFetcher.pollPage();
            while (page.isPresent()) {
                pages.add(page.get());
                page = taskResultFetcher.pollPage();
            }

            assertEquals(pages.size(), numPages);
            for (int i = 0; i < numPages; i++) {
                assertEquals(pages.get(i).getSizeInBytes(), serializedPageSize);
            }
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testResultFetcherAlwaysTimeout()
    {
        // Test request timeout.
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString(), () -> new Duration(500, TimeUnit.MILLISECONDS))),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(1),
                workerClient,
                Optional.of(new Duration(200, TimeUnit.MILLISECONDS)));
        CompletableFuture<Void> future = taskResultFetcher.start();
        assertThrows(ExecutionException.class, future::get);
    }

    @Test
    public void testInfoFetcher()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        URI uri = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
        PrestoSparkHttpWorkerClient workerClient = new PrestoSparkHttpWorkerClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                uri,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        HttpNativeExecutionTaskInfoFetcher taskInfoFetcher = new HttpNativeExecutionTaskInfoFetcher(
                newScheduledThreadPool(1),
                workerClient,
                newSingleThreadExecutor());
        assertFalse(taskInfoFetcher.getTaskInfo().isPresent());
        taskInfoFetcher.start();
        try {
            Thread.sleep(3 * GET_TASK_INFO_INTERVALS.toMillis());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
        assertTrue(taskInfoFetcher.getTaskInfo().isPresent());
    }

    @Test
    public void testNativeExecutionTask()
    {
        // We need multi-thread scheduler to increase scheduling concurrency.
        // Otherwise async execution assumption is not going to hold with a
        // single thread.
        ScheduledExecutorService scheduler = newScheduledThreadPool(4);
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        NativeExecutionTaskFactory factory = new NativeExecutionTaskFactory(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                newSingleThreadExecutor(),
                scheduler,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC);
        List<TaskSource> sources = new ArrayList<>();
        NativeExecutionTask task = factory.createNativeExecutionTask(
                testSessionBuilder().build(),
                uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build(),
                taskId,
                createPlanFragment(),
                sources,
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()));

        assertFalse(task.getTaskInfo().isPresent());
        try {
            assertFalse(task.pollResult().isPresent());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }

        List<SerializedPage> resultPages = new ArrayList<>();

        // Start polling results
        ScheduledFuture scheduledFuture = scheduler.scheduleAtFixedRate(() ->
        {
            try {
                Optional<SerializedPage> page = task.pollResult();
                page.ifPresent(resultPages::add);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                fail();
            }
        }, 0, 200, TimeUnit.MILLISECONDS);

        // Start task
        try {
            task.start().handle((v, t) ->
            {
                if (t != null) {
                    t.getCause().printStackTrace();
                    fail();
                }
                try {
                    // Wait for a bit to allow enough time to consume results completely.
                    Thread.sleep(400);
                    assertFalse(task.pollResult().isPresent());
                    assertFalse(resultPages.isEmpty());
                    task.stop();
                    scheduledFuture.cancel(false);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                    fail();
                }
                return null;
            }).get();
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            fail();
        }
    }

    private static class TestingHttpResponseFuture<T>
            extends AbstractFuture<T>
            implements HttpClient.HttpResponseFuture<T>
    {
        @Override
        public String getState()
        {
            return null;
        }

        public void complete(T value)
        {
            super.set(value);
        }

        public void completeExceptionally(Throwable t)
        {
            super.setException(t);
        }
    }

    private static class TestingHttpClient
            implements com.facebook.airlift.http.client.HttpClient
    {
        private final ScheduledExecutorService executor;
        private final TestingResponseManager responseManager;

        public TestingHttpClient(TestingResponseManager responseManager)
        {
            this.executor = newScheduledThreadPool(10);
            this.responseManager = responseManager;
        }

        @Override
        public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler) throws E
        {
            try {
                return executeAsync(request, responseHandler).get();
            }
            catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public <T, E extends Exception> HttpResponseFuture<T> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
        {
            TestingHttpResponseFuture<T> future = new TestingHttpResponseFuture<T>();
            Duration delay = responseManager.getDelay();
            executor.schedule(
                    () ->
                    {
                        URI uri = request.getUri();
                        String method = request.getMethod();
                        ListMultimap<String, String> headers = request.getHeaders();
                        String path = uri.getPath();
                        String taskId = getTaskId(uri);
                        if (method.equalsIgnoreCase("GET")) {
                            // GET /v1/task/{taskId}
                            if (Pattern.compile("\\/v1\\/task\\/[a-zA-Z0-9]+.[0-9]+.[0-9]+.[0-9]+\\z").matcher(path).find()) {
                                try {
                                    future.complete(responseHandler.handle(request, responseManager.createTaskInfoResponse(HttpStatus.OK)));
                                }
                                catch (Exception e) {
                                    e.printStackTrace();
                                    future.completeExceptionally(e);
                                }
                            }
                            // GET /v1/task/{taskId}/results/{bufferId}/{token}/acknowledge
                            else if (Pattern.compile(".*\\/results\\/[0-9]+\\/[0-9]+\\/acknowledge\\z").matcher(path).find()) {
                                try {
                                    future.complete(responseHandler.handle(request, responseManager.createDummyResultResponse()));
                                }
                                catch (Exception e) {
                                    e.printStackTrace();
                                    future.completeExceptionally(e);
                                }
                            }
                            // GET /v1/task/{taskId}/results/{bufferId}/{token}
                            else if (Pattern.compile(".*\\/results\\/[0-9]+\\/[0-9]+\\z").matcher(path).find()) {
                                try {
                                    future.complete(responseHandler.handle(
                                            request,
                                            responseManager.createResultResponse()));
                                }
                                catch (Exception e) {
                                    e.printStackTrace();
                                    future.completeExceptionally(e);
                                }
                            }
                        }
                        else if (method.equalsIgnoreCase("POST")) {
                            // POST /v1/task/{taskId}
                            if (Pattern.compile("\\/v1\\/task\\/[a-zA-Z0-9]+.[0-9]+.[0-9]+.[0-9]+\\z").matcher(path).find()) {
                                try {
                                    future.complete(responseHandler.handle(request, responseManager.createTaskInfoResponse(HttpStatus.OK)));
                                }
                                catch (Exception e) {
                                    e.printStackTrace();
                                    future.completeExceptionally(e);
                                }
                            }
                        }
                        else if (method.equalsIgnoreCase("DELETE")) {
                            // DELETE /v1/task/{taskId}
                            if (Pattern.compile("\\/v1\\/task\\/[a-zA-Z0-9]+.[0-9]+.[0-9]+.[0-9]+\\z").matcher(path).find()) {
                                try {
                                    future.complete(responseHandler.handle(request, responseManager.createDummyResultResponse()));
                                }
                                catch (Exception e) {
                                    e.printStackTrace();
                                    future.completeExceptionally(e);
                                }
                            }
                        }
                        future.completeExceptionally(new Exception("Unknown path " + path));
                    },
                    (long) delay.getValue(),
                    delay.getUnit());
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

    /**
     * A stateful response manager for testing purpose. The lifetime of an instantiation of this class should be equivalent to the lifetime of the http client.
     */
    public static class TestingResponseManager
    {
        private static final JsonCodec<TaskInfo> taskInfoCodec = JsonCodec.jsonCodec(TaskInfo.class);
        private final TestingResultResponseManager resultResponseManager;
        private final Optional<DelayGenerator> delayGenerator;
        private final String taskId;

        public TestingResponseManager(String taskId)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = new TestingResultResponseManager();
            this.delayGenerator = Optional.empty();
        }

        public TestingResponseManager(String taskId, DelayGenerator delayGenerator)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = new TestingResultResponseManager();
            this.delayGenerator = Optional.of(requireNonNull(delayGenerator, "delayGenerator is null"));
        }

        public TestingResponseManager(String taskId, DelayGenerator delayGenerator, TestingResultResponseManager resultResponseManager)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = requireNonNull(resultResponseManager, "resultResponseManager is null.");
            this.delayGenerator = Optional.of(requireNonNull(delayGenerator, "delayGenerator is null"));
        }

        public Duration getDelay()
        {
            if (delayGenerator.isPresent()) {
                return delayGenerator.get().getDelay();
            }
            return NO_DURATION;
        }

        public Response createDummyResultResponse()
        {
            return new TestingResponse();
        }

        public Response createResultResponse() throws Exception
        {
            return resultResponseManager.createResultResponse(taskId);
        }

        public Response createTaskInfoResponse(HttpStatus httpStatus)
        {
            ListMultimap<HeaderName, String> headers = ArrayListMultimap.create();
            headers.put(HeaderName.of(CONTENT_TYPE), String.valueOf(MediaType.create("application", "json")));
            TaskInfo taskInfo = TaskInfo.createInitialTask(
                    TaskId.valueOf(taskId),
                    uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build(),
                    new ArrayList<>(),
                    new TaskStats(DateTime.now(), null),
                    "dummy-node");
            return new TestingResponse(
                    httpStatus.code(),
                    httpStatus.toString(),
                    headers,
                    new ByteArrayInputStream(taskInfoCodec.toBytes(taskInfo)));
        }

        /**
         * Manager for result fetching related endpoints. It maintains any stateful information inside itself. Callers can extend this class to create their own response handling
         * logic.
         */
        public static class TestingResultResponseManager
        {
            /**
             * A dummy implementation of result creation logic. It shall be overriden by users to create customized result returning logic.
             */
            public Response createResultResponse(String taskId)
                    throws Exception
            {
                return createResultResponseHelper(HttpStatus.OK,
                        taskId,
                        0,
                        1,
                        true,
                        0);
            }

            protected Response createResultResponseHelper(
                    HttpStatus httpStatus,
                    String taskId,
                    long token,
                    long nextToken,
                    boolean bufferComplete,
                    int serializedPageSizeBytes)
            {
                DynamicSliceOutput slicedOutput = new DynamicSliceOutput(1024);
                PagesSerdeUtil.writeSerializedPage(slicedOutput, createSerializedPage(serializedPageSizeBytes));
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
        }

        public interface DelayGenerator
        {
            Duration getDelay();
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

    private static SerializedPage createSerializedPage(int numBytes)
    {
        byte[] bytes = new byte[numBytes];
        Arrays.fill(bytes, (byte) 8);
        Slice slice = Slices.wrappedBuffer(bytes);
        return new SerializedPage(
                slice,
                PageCodecMarker.none(),
                0,
                numBytes,
                0);
    }
}
