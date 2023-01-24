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
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.operator.PageTransportErrorException;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.BatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.HttpNativeExecutionTaskInfoFetcher;
import com.facebook.presto.spark.execution.HttpNativeExecutionTaskResultFetcher;
import com.facebook.presto.spark.execution.NativeExecutionProcess;
import com.facebook.presto.spark.execution.NativeExecutionProcessFactory;
import com.facebook.presto.spark.execution.NativeExecutionTask;
import com.facebook.presto.spark.execution.NativeExecutionTaskFactory;
import com.facebook.presto.spark.execution.property.NativeExecutionConnectorConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionNodeConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionSystemConfig;
import com.facebook.presto.spark.execution.property.PrestoSparkWorkerProperty;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
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
import com.google.common.util.concurrent.SettableFuture;
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
import static com.facebook.presto.client.NodeVersion.UNKNOWN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.execution.TaskTestUtils.SPLIT;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

public class TestPrestoSparkHttpClient
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
    private static final JsonCodec<BatchTaskUpdateRequest> TASK_UPDATE_REQUEST_JSON_CODEC = JsonCodec.jsonCodec(BatchTaskUpdateRequest.class);
    private static final JsonCodec<ServerInfo> SERVER_INFO_JSON_CODEC = JsonCodec.jsonCodec(ServerInfo.class);
    private static final ScheduledExecutorService errorScheduler = newScheduledThreadPool(4);
    private static final ScheduledExecutorService updateScheduledExecutor = newScheduledThreadPool(4);

    @Test
    public void testResultGet()
    {
        TaskId taskId = new TaskId(
                "testid",
                0,
                0,
                0);

        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));
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

        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));
        workerClient.acknowledgeResultsAsync(1);
    }

    @Test
    public void testResultAbort()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);

        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));
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

        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));
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

        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));

        Set<ScheduledSplit> splits = new HashSet<>();
        splits.add(SPLIT);
        List<TaskSource> sources = new ArrayList<>();
        ListenableFuture<BaseResponse<TaskInfo>> future = workerClient.updateTask(
                sources,
                createPlanFragment(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()),
                Optional.empty(),
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
    public void testGetServerInfo()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        ServerInfo expected = new ServerInfo(UNKNOWN, "test", true, false, Optional.of(Duration.valueOf("2m")));

        PrestoSparkHttpServerClient workerClient = new PrestoSparkHttpServerClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                BASE_URI,
                SERVER_INFO_JSON_CODEC);
        ListenableFuture<BaseResponse<ServerInfo>> future = workerClient.getServerInfo();
        try {
            ServerInfo serverInfo = future.get().getValue();
            assertEquals(serverInfo, expected);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGetServerInfoWithRetry()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        ScheduledExecutorService scheduler = newScheduledThreadPool(1);
        ServerInfo expected = new ServerInfo(UNKNOWN, "test", true, false, Optional.of(Duration.valueOf("2m")));
        Duration maxTimeout = new Duration(1, TimeUnit.MINUTES);
        NativeExecutionProcess process = createNativeExecutionProcess(
                taskId,
                scheduler,
                maxTimeout,
                new TestingResponseManager(taskId.toString(), new FailureRetryResponseManager(5)),
                new TaskManagerConfig());

        SettableFuture<ServerInfo> future = process.getServerInfoWithRetry();
        try {
            ServerInfo serverInfo = future.get();
            assertEquals(serverInfo, expected);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testGetServerInfoWithRetryTimeout()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);
        ScheduledExecutorService scheduler = newScheduledThreadPool(1);
        Duration maxTimeout = new Duration(0, TimeUnit.MILLISECONDS);
        NativeExecutionProcess process = createNativeExecutionProcess(
                taskId,
                scheduler,
                maxTimeout,
                new TestingResponseManager(taskId.toString(), new FailureRetryResponseManager(5)),
                new TaskManagerConfig());

        SettableFuture<ServerInfo> future = process.getServerInfoWithRetry();
        Exception exception = expectThrows(ExecutionException.class, future::get);
        assertTrue(exception.getMessage().contains("Encountered too many errors talking to native process. The process may have crashed or be under too much load"));
    }

    @Test
    public void testResultFetcher()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);

        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(1),
                workerClient);
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
        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(
                        new TestingResponseManager(
                            taskId.toString(),
                            new TestingResponseManager.TestingResultResponseManager()
                            {
                                private int requestCount;

                                @Override
                                public Response createResultResponse(String taskId)
                                        throws PageTransportErrorException
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
                                        fail("Retrieving results after buffer completion");
                                        return null;
                                    }
                                }
                            })),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(1),
                workerClient);
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
        public Response createResultResponse(String taskId)
                throws PageTransportErrorException
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
                fail("Retrieving results after buffer completion");
                return null;
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

        BreakingLimitResponseManager breakingLimitResponseManager =
                new BreakingLimitResponseManager(serializedPageSize, numPages);

        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(
                        new TestingResponseManager(
                                taskId.toString(),
                                breakingLimitResponseManager)),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(10),
                workerClient);
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
        public Response createResultResponse(String taskId)
                throws PageTransportErrorException
        {
            if (++timeoutCount <= numInitialTimeouts) {
                throw new PageTransportErrorException(new HostAddress("localhost", 8080), "Mock HttpClient Timeout");
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
                fail("Retrieving results after buffer completion");
                return null;
            }
        }
    }

    @Test
    public void testResultFetcherTransportErrorRecovery()
    {
        int numPages = 10;
        int serializedPageSize = 0;
        // Transport error count less than MAX_TRANSPORT_ERROR_RETRIES (5).
        // Expecting recovery from failed requests
        int numTransportErrors = 3;

        TaskId taskId = new TaskId("testid", 0, 0, 0);

        TimeoutResponseManager timeoutResponseManager =
                new TimeoutResponseManager(serializedPageSize, numPages, numTransportErrors);

        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(
                        new TestingResponseManager(
                                taskId.toString(),
                                timeoutResponseManager)),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(10),
                workerClient);
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
    public void testResultFetcherTransportErrorFail()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);

        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString(), new TimeoutResponseManager(0, 10, 10))),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(1),
                workerClient);
        CompletableFuture<Void> future = taskResultFetcher.start();
        assertThrows(ExecutionException.class, future::get);
    }

    @Test
    public void testInfoFetcher()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0);

        Duration fetchInterval = new Duration(1, TimeUnit.SECONDS);
        PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                new TestingHttpClient(new TestingResponseManager(taskId.toString())),
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS));
        HttpNativeExecutionTaskInfoFetcher taskInfoFetcher = new HttpNativeExecutionTaskInfoFetcher(
                newScheduledThreadPool(1),
                workerClient,
                newSingleThreadExecutor(),
                new Duration(1, TimeUnit.SECONDS));
        assertFalse(taskInfoFetcher.getTaskInfo().isPresent());
        taskInfoFetcher.start();
        try {
            Thread.sleep(3 * fetchInterval.toMillis());
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
        TaskManagerConfig config = new TaskManagerConfig();
        config.setInfoRefreshMaxWait(new Duration(5, TimeUnit.SECONDS));
        config.setInfoUpdateInterval(new Duration(200, TimeUnit.MILLISECONDS));
        List<TaskSource> sources = new ArrayList<>();
        try {
            NativeExecutionTaskFactory taskFactory = new NativeExecutionTaskFactory(
                    new TestingHttpClient(new TestingResponseManager(taskId.toString(), new TimeoutResponseManager(0, 10, 0))),
                    newSingleThreadExecutor(),
                    scheduler,
                    TASK_INFO_JSON_CODEC,
                    PLAN_FRAGMENT_JSON_CODEC,
                    TASK_UPDATE_REQUEST_JSON_CODEC,
                    config);
            NativeExecutionTask task = taskFactory.createNativeExecutionTask(
                    testSessionBuilder().build(),
                    BASE_URI,
                    taskId,
                    createPlanFragment(),
                    sources,
                    new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()),
                    Optional.empty());
            assertNotNull(task);
            assertFalse(task.getTaskInfo().isPresent());
            assertFalse(task.pollResult().isPresent());

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
                    assertEquals(resultPages.size(), 10);
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

    private NativeExecutionProcess createNativeExecutionProcess(
            TaskId taskId,
            ScheduledExecutorService scheduler,
            Duration maxErrorDuration,
            TestingResponseManager responseManager,
            TaskManagerConfig config)
    {
        ScheduledExecutorService errorScheduler = newScheduledThreadPool(4);
        PrestoSparkWorkerProperty workerProperty = new PrestoSparkWorkerProperty(
                new NativeExecutionSystemConfig(),
                new NativeExecutionConnectorConfig(),
                new NativeExecutionNodeConfig());
        NativeExecutionProcessFactory factory = new NativeExecutionProcessFactory(
                new TestingHttpClient(responseManager),
                newSingleThreadExecutor(),
                errorScheduler,
                SERVER_INFO_JSON_CODEC,
                config,
                workerProperty);
        List<TaskSource> sources = new ArrayList<>();
        return factory.createNativeExecutionProcess(
                testSessionBuilder().build(),
                BASE_URI,
                maxErrorDuration);
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
        public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
                throws E
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
            executor.schedule(
                    () ->
                    {
                        URI uri = request.getUri();
                        String method = request.getMethod();
                        ListMultimap<String, String> headers = request.getHeaders();
                        String path = uri.getPath();
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
                            // GET /v1/info
                            else if (Pattern.compile("\\/v1\\/info").matcher(path).find()) {
                                try {
                                    future.complete(responseHandler.handle(
                                            request,
                                            responseManager.createServerInfoResponse()));
                                }
                                catch (Exception e) {
                                    e.printStackTrace();
                                    future.completeExceptionally(e);
                                }
                            }
                        }
                        else if (method.equalsIgnoreCase("POST")) {
                            // POST /v1/task/{taskId}/batch
                            if (Pattern.compile("\\/v1\\/task\\/[a-zA-Z0-9]+.[0-9]+.[0-9]+.[0-9]+\\/batch\\z").matcher(path).find()) {
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
                    (long) NO_DURATION.getValue(),
                    NO_DURATION.getUnit());
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
        private static final JsonCodec<ServerInfo> serverInfoCodec = JsonCodec.jsonCodec(ServerInfo.class);
        private final TestingResultResponseManager resultResponseManager;
        private final TestingServerResponseManager serverResponseManager;
        private final String taskId;

        public TestingResponseManager(String taskId)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = new TestingResultResponseManager();
            this.serverResponseManager = new TestingServerResponseManager();
        }

        public TestingResponseManager(String taskId, TestingResultResponseManager resultResponseManager)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = requireNonNull(resultResponseManager, "resultResponseManager is null.");
            this.serverResponseManager = new TestingServerResponseManager();
        }

        public TestingResponseManager(String taskId, TestingServerResponseManager serverResponseManager)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = new TestingResultResponseManager();
            this.serverResponseManager = requireNonNull(serverResponseManager, "serverResponseManager is null");
        }

        public Response createDummyResultResponse()
        {
            return new TestingResponse();
        }

        public Response createResultResponse()
                throws PageTransportErrorException
        {
            return resultResponseManager.createResultResponse(taskId);
        }

        public Response createServerInfoResponse()
                throws PrestoException
        {
            return serverResponseManager.createServerInfoResponse();
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
         * Manager for server related endpoints. It maintains any stateful information inside itself. Callers can extend this class to create their own response handling
         * logic.
         */
        public static class TestingServerResponseManager
        {
            public Response createServerInfoResponse()
                    throws PrestoException
            {
                ServerInfo serverInfo = new ServerInfo(UNKNOWN, "test", true, false, Optional.of(Duration.valueOf("2m")));
                HttpStatus httpStatus = HttpStatus.OK;
                ListMultimap<HeaderName, String> headers = ArrayListMultimap.create();
                headers.put(HeaderName.of(CONTENT_TYPE), String.valueOf(MediaType.create("application", "json")));
                return new TestingResponse(
                        httpStatus.code(),
                        httpStatus.toString(),
                        headers,
                        new ByteArrayInputStream(serverInfoCodec.toBytes(serverInfo)));
            }
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
                    throws PageTransportErrorException
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

    private static class FailureRetryResponseManager
            extends TestingResponseManager.TestingServerResponseManager
    {
        private final int maxRetryCount;
        private int retryCount;

        public FailureRetryResponseManager(int maxRetryCount)
        {
            this.maxRetryCount = maxRetryCount;
        }

        @Override
        public Response createServerInfoResponse()
                throws PrestoException
        {
            if (retryCount++ < maxRetryCount) {
                throw new RuntimeException("Get ServerInfo request failure.");
            }

            return super.createServerInfoResponse();
        }
    }
}
