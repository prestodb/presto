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
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.PageBufferClient;
import com.facebook.presto.operator.PageTransportErrorException;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.server.smile.BaseResponse;
import com.facebook.presto.spark.execution.nativeprocess.HttpNativeExecutionTaskInfoFetcher;
import com.facebook.presto.spark.execution.nativeprocess.HttpNativeExecutionTaskResultFetcher;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcess;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcessFactory;
import com.facebook.presto.spark.execution.property.NativeExecutionConnectorConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionNodeConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionSystemConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionVeloxConfig;
import com.facebook.presto.spark.execution.property.PrestoSparkWorkerProperty;
import com.facebook.presto.spark.execution.task.NativeExecutionTask;
import com.facebook.presto.spark.execution.task.NativeExecutionTaskFactory;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoTransportException;
import com.facebook.presto.spi.page.PageCodecMarker;
import com.facebook.presto.spi.page.PagesSerdeUtil;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
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
import static com.facebook.presto.common.ErrorType.USER_ERROR;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
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

    @Test
    public void testResultGet()
    {
        TaskId taskId = new TaskId(
                "testid",
                0,
                0,
                0,
                0);

        PrestoSparkHttpTaskClient workerClient = createWorkerClient(taskId);
        ListenableFuture<PageBufferClient.PagesResponse> future = workerClient.getResults(
                0,
                new DataSize(32, MEGABYTE));
        try {
            PageBufferClient.PagesResponse page = future.get();
            assertEquals(0, page.getToken());
            assertTrue(page.isClientComplete());
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
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);

        PrestoSparkHttpTaskClient workerClient = createWorkerClient(taskId);
        workerClient.acknowledgeResultsAsync(1);
    }

    private PrestoSparkHttpTaskClient createWorkerClient(TaskId taskId)
    {
        return createWorkerClient(taskId, new TestingHttpClient(new TestingResponseManager(taskId.toString())));
    }

    private PrestoSparkHttpTaskClient createWorkerClient(TaskId taskId, TestingHttpClient httpClient)
    {
        return new PrestoSparkHttpTaskClient(
                httpClient,
                taskId,
                BASE_URI,
                TASK_INFO_JSON_CODEC,
                PLAN_FRAGMENT_JSON_CODEC,
                TASK_UPDATE_REQUEST_JSON_CODEC,
                new Duration(1, TimeUnit.SECONDS),
                new HashMap<>());
    }

    HttpNativeExecutionTaskResultFetcher createResultFetcher(PrestoSparkHttpTaskClient workerClient)
    {
        return createResultFetcher(workerClient, new Object());
    }

    HttpNativeExecutionTaskResultFetcher createResultFetcher(PrestoSparkHttpTaskClient workerClient, Object lock)
    {
        return new HttpNativeExecutionTaskResultFetcher(
                newScheduledThreadPool(1),
                newScheduledThreadPool(1),
                workerClient,
                newSingleThreadExecutor(),
                new Duration(1, TimeUnit.SECONDS),
                lock);
    }

    @Test
    public void testResultAbort()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);

        PrestoSparkHttpTaskClient workerClient = createWorkerClient(taskId);
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
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);

        PrestoSparkHttpTaskClient workerClient = createWorkerClient(taskId);
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
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);

        PrestoSparkHttpTaskClient workerClient = createWorkerClient(taskId);

        List<TaskSource> sources = new ArrayList<>();
        BaseResponse<TaskInfo> response = workerClient.updateTask(
                sources,
                createPlanFragment(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()),
                Optional.empty(),
                Optional.empty(),
                TestingSession.testSessionBuilder().build(),
                createInitialEmptyOutputBuffers(PARTITIONED));

        try {
            TaskInfo taskInfo = response.getValue();
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
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);
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
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);
        ServerInfo expected = new ServerInfo(UNKNOWN, "test", true, false, Optional.of(Duration.valueOf("2m")));
        Duration maxTimeout = new Duration(1, TimeUnit.MINUTES);
        NativeExecutionProcess process = createNativeExecutionProcess(
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
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);
        Duration maxTimeout = new Duration(0, TimeUnit.MILLISECONDS);
        NativeExecutionProcess process = createNativeExecutionProcess(
                maxTimeout,
                new TestingResponseManager(taskId.toString(), new FailureRetryResponseManager(5)),
                new TaskManagerConfig());

        SettableFuture<ServerInfo> future = process.getServerInfoWithRetry();
        Exception exception = expectThrows(ExecutionException.class, future::get);
        assertTrue(exception.getMessage().contains("Native process launch failed with multiple retries"));
    }

    @Test
    public void testResultFetcher()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);

        PrestoSparkHttpTaskClient workerClient = createWorkerClient(taskId);
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = createResultFetcher(workerClient);
        taskResultFetcher.start();
        try {
            List<SerializedPage> pages = new ArrayList<>();
            Optional<SerializedPage> page = taskResultFetcher.pollPage();
            while (page.isPresent()) {
                pages.add(page.get());
                page = taskResultFetcher.pollPage();
            }

            assertEquals(1, pages.size());
            assertEquals(0, pages.get(0).getSizeInBytes());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
    }

    private List<SerializedPage> fetchResults(HttpNativeExecutionTaskResultFetcher taskResultFetcher, int numPages)
            throws InterruptedException
    {
        List<SerializedPage> pages = new ArrayList<>();
        for (int i = 0; i < 1_000 && pages.size() < numPages; ++i) {
            Optional<SerializedPage> page = taskResultFetcher.pollPage();
            if (page.isPresent()) {
                pages.add(page.get());
            }
        }
        return pages;
    }

    @Test
    public void testResultFetcherMultipleNonEmptyResults()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);
        int serializedPageSize = (int) new DataSize(1, MEGABYTE).toBytes();
        int numPages = 10;
        PrestoSparkHttpTaskClient workerClient = createWorkerClient(
                taskId,
                new TestingHttpClient(
                        new TestingResponseManager(taskId.toString(), new TestingResponseManager.TestingResultResponseManager()
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
                        })));
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = createResultFetcher(workerClient);
        taskResultFetcher.start();
        try {
            List<SerializedPage> pages = fetchResults(taskResultFetcher, numPages);

            assertEquals(numPages, pages.size());
            for (int i = 0; i < numPages; i++) {
                assertEquals(pages.get(i).getSizeInBytes(), serializedPageSize);
            }
        }
        catch (InterruptedException e) {
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
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);

        BreakingLimitResponseManager breakingLimitResponseManager =
                new BreakingLimitResponseManager(serializedPageSize, numPages);

        PrestoSparkHttpTaskClient workerClient = createWorkerClient(
                taskId,
                new TestingHttpClient(
                        new TestingResponseManager(
                                taskId.toString(),
                                breakingLimitResponseManager)));
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = createResultFetcher(workerClient);
        taskResultFetcher.start();
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

            assertEquals(numPages, pages.size());
            for (int i = 0; i < numPages; i++) {
                assertEquals(pages.get(i).getSizeInBytes(), serializedPageSize);
            }
        }
        catch (InterruptedException e) {
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

        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);

        TimeoutResponseManager timeoutResponseManager =
                new TimeoutResponseManager(serializedPageSize, numPages, numTransportErrors);

        PrestoSparkHttpTaskClient workerClient = createWorkerClient(
                taskId,
                new TestingHttpClient(
                        new TestingResponseManager(
                                taskId.toString(),
                                timeoutResponseManager)));
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = createResultFetcher(workerClient);
        taskResultFetcher.start();
        try {
            List<SerializedPage> pages = fetchResults(taskResultFetcher, numPages);

            assertEquals(pages.size(), numPages);
            for (int i = 0; i < numPages; i++) {
                assertEquals(pages.get(i).getSizeInBytes(), serializedPageSize);
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testResultFetcherTransportErrorFail() throws InterruptedException
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);

        PrestoSparkHttpTaskClient workerClient = createWorkerClient(
                taskId,
                new TestingHttpClient(new TestingResponseManager(taskId.toString(), new TimeoutResponseManager(0, 10, 10))));
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = createResultFetcher(workerClient);
        taskResultFetcher.start();
        try {
            for (int i = 0; i < 1_000; ++i) {
                taskResultFetcher.pollPage();
            }
            fail("Expected an exception");
        }
        catch (PrestoTransportException e) {
            assertTrue(e.getMessage().startsWith("TaskResultsFetcher encountered too many errors talking to native process"));
        }
    }

    @Test
    public void testResultFetcherWaitOnSignal()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);
        Object lock = new Object();

        PrestoSparkHttpTaskClient workerClient = createWorkerClient(taskId);
        HttpNativeExecutionTaskResultFetcher taskResultFetcher = createResultFetcher(workerClient, lock);
        taskResultFetcher.start();
        try {
            synchronized (lock) {
                while (!taskResultFetcher.hasPage()) {
                    lock.wait();
                }
            }
            assertTrue(taskResultFetcher.hasPage());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testInfoFetcher()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);

        Duration fetchInterval = new Duration(1, TimeUnit.SECONDS);
        HttpNativeExecutionTaskInfoFetcher taskInfoFetcher = createTaskInfoFetcher(taskId, new TestingResponseManager(taskId.toString()));
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
    public void testInfoFetcherWithRetry()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);

        Duration fetchInterval = new Duration(1, TimeUnit.SECONDS);
        HttpNativeExecutionTaskInfoFetcher taskInfoFetcher = createTaskInfoFetcher(
                taskId,
                new TestingResponseManager(taskId.toString(), new FailureTaskInfoRetryResponseManager(1)),
                new Duration(5, TimeUnit.SECONDS),
                new Object());
        assertFalse(taskInfoFetcher.getTaskInfo().isPresent());
        taskInfoFetcher.start();
        try {
            Thread.sleep(3 * fetchInterval.toMillis());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }

        // First fetch is expected to succeed.
        assertTrue(taskInfoFetcher.getTaskInfo().isPresent());

        try {
            Thread.sleep(10 * fetchInterval.toMillis());
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
        Exception exception = expectThrows(RuntimeException.class, taskInfoFetcher::getTaskInfo);
        assertTrue(exception.getMessage().contains("TaskInfoFetcher encountered too many errors talking to native process."));
    }

    @Test
    public void testInfoFetcherWaitOnSignal()
    {
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);
        Object lock = new Object();

        HttpNativeExecutionTaskInfoFetcher taskInfoFetcher = createTaskInfoFetcher(taskId, new TestingResponseManager(taskId.toString(), TaskState.FINISHED), lock);
        assertFalse(taskInfoFetcher.getTaskInfo().isPresent());
        taskInfoFetcher.start();
        try {
            synchronized (lock) {
                while (!isTaskDone(taskInfoFetcher.getTaskInfo())) {
                    lock.wait();
                }
            }
        }
        catch (InterruptedException e) {
            fail();
        }
        assertTrue(isTaskDone(taskInfoFetcher.getTaskInfo()));
    }

    private boolean isTaskDone(Optional<TaskInfo> taskInfo)
    {
        return taskInfo.isPresent() && taskInfo.get().getTaskStatus().getState().isDone();
    }

    @Test
    public void testNativeExecutionTask()
    {
        // We need multi-thread scheduler to increase scheduling concurrency.
        // Otherwise, async execution assumption is not going to hold with a
        // single thread.
        ScheduledExecutorService scheduler = newScheduledThreadPool(4);
        TaskId taskId = new TaskId("testid", 0, 0, 0, 0);
        TaskManagerConfig taskConfig = new TaskManagerConfig();
        QueryManagerConfig queryConfig = new QueryManagerConfig();
        taskConfig.setInfoRefreshMaxWait(new Duration(5, TimeUnit.SECONDS));
        taskConfig.setInfoUpdateInterval(new Duration(200, TimeUnit.MILLISECONDS));
        queryConfig.setRemoteTaskMaxErrorDuration(new Duration(1, TimeUnit.MINUTES));
        List<TaskSource> sources = new ArrayList<>();
        try {
            NativeExecutionTaskFactory taskFactory = new NativeExecutionTaskFactory(
                    new TestingHttpClient(new TestingResponseManager(taskId.toString(), new TimeoutResponseManager(0, 10, 0))),
                    newSingleThreadExecutor(),
                    scheduler,
                    scheduler,
                    TASK_INFO_JSON_CODEC,
                    PLAN_FRAGMENT_JSON_CODEC,
                    TASK_UPDATE_REQUEST_JSON_CODEC,
                    taskConfig,
                    queryConfig);
            NativeExecutionTask task = taskFactory.createNativeExecutionTask(
                    testSessionBuilder().build(),
                    BASE_URI,
                    taskId,
                    createPlanFragment(),
                    sources,
                    new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()),
                    Optional.empty(),
                    Optional.empty());
            assertNotNull(task);
            assertFalse(task.getTaskInfo().isPresent());
            assertFalse(task.pollResult().isPresent());

            // Start task
            TaskInfo taskInfo = task.start();
            assertFalse(taskInfo.getTaskStatus().getState().isDone());

            List<SerializedPage> resultPages = new ArrayList<>();
            for (int i = 0; i < 100 && resultPages.size() < 10; ++i) {
                Optional<SerializedPage> page = task.pollResult();
                page.ifPresent(resultPages::add);
            }
            assertFalse(task.pollResult().isPresent());
            assertEquals(10, resultPages.size());
            assertTrue(task.getTaskInfo().isPresent());

            task.stop(true);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            fail();
        }
    }

    private NativeExecutionProcess createNativeExecutionProcess(
            Duration maxErrorDuration,
            TestingResponseManager responseManager,
            TaskManagerConfig config)
    {
        ScheduledExecutorService errorScheduler = newScheduledThreadPool(4);
        PrestoSparkWorkerProperty workerProperty = new PrestoSparkWorkerProperty(
                new NativeExecutionConnectorConfig(),
                new NativeExecutionNodeConfig(),
                new NativeExecutionSystemConfig(),
                new NativeExecutionVeloxConfig());
        NativeExecutionProcessFactory factory = new NativeExecutionProcessFactory(
                new TestingHttpClient(responseManager),
                newSingleThreadExecutor(),
                errorScheduler,
                SERVER_INFO_JSON_CODEC,
                config,
                workerProperty);
        return factory.createNativeExecutionProcess(
                testSessionBuilder().build(),
                BASE_URI,
                maxErrorDuration);
    }

    private HttpNativeExecutionTaskInfoFetcher createTaskInfoFetcher(TaskId taskId, TestingResponseManager testingResponseManager)
    {
        return createTaskInfoFetcher(taskId, testingResponseManager, new Duration(1, TimeUnit.MINUTES), new Object());
    }

    private HttpNativeExecutionTaskInfoFetcher createTaskInfoFetcher(TaskId taskId, TestingResponseManager testingResponseManager, Object lock)
    {
        return createTaskInfoFetcher(taskId, testingResponseManager, new Duration(1, TimeUnit.MINUTES), lock);
    }

    private HttpNativeExecutionTaskInfoFetcher createTaskInfoFetcher(TaskId taskId, TestingResponseManager testingResponseManager, Duration maxErrorDuration, Object lock)
    {
        PrestoSparkHttpTaskClient workerClient = createWorkerClient(taskId, new TestingHttpClient(testingResponseManager));
        return new HttpNativeExecutionTaskInfoFetcher(
                newScheduledThreadPool(1),
                newScheduledThreadPool(1),
                workerClient,
                newSingleThreadExecutor(),
                new Duration(1, TimeUnit.SECONDS),
                maxErrorDuration,
                lock);
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

    public static class TestingHttpClient
            implements com.facebook.airlift.http.client.HttpClient
    {
        private static final String TASK_ID_REGEX = "/v1/task/[a-zA-Z0-9]+.[0-9]+.[0-9]+.[0-9]+.[0-9]+";
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
            TestingHttpResponseFuture<T> future = new TestingHttpResponseFuture<>();
            executor.schedule(
                    () ->
                    {
                        URI uri = request.getUri();
                        String method = request.getMethod();
                        String path = uri.getPath();
                        try {
                            if (method.equalsIgnoreCase("GET")) {
                                // GET /v1/task/{taskId}
                                if (Pattern.compile(TASK_ID_REGEX + "\\z").matcher(path).find()) {
                                    future.complete(responseHandler.handle(request, responseManager.createTaskInfoResponse(HttpStatus.OK)));
                                }
                                // GET /v1/task/{taskId}/results/{bufferId}/{token}/acknowledge
                                else if (Pattern.compile(".*/results/[0-9]+/[0-9]+/acknowledge\\z").matcher(path).find()) {
                                    future.complete(responseHandler.handle(request, responseManager.createDummyResultResponse()));
                                }
                                // GET /v1/task/{taskId}/results/{bufferId}/{token}
                                else if (Pattern.compile(".*/results/[0-9]+/[0-9]+\\z").matcher(path).find()) {
                                    future.complete(responseHandler.handle(
                                            request,
                                            responseManager.createResultResponse()));
                                }
                                // GET /v1/info
                                else if (Pattern.compile("/v1/info").matcher(path).find()) {
                                    future.complete(responseHandler.handle(
                                            request,
                                            responseManager.createServerInfoResponse()));
                                }
                            }
                            else if (method.equalsIgnoreCase("POST")) {
                                // POST /v1/task/{taskId}/batch
                                if (Pattern.compile(format("%s\\/batch\\z", TASK_ID_REGEX)).matcher(path).find()) {
                                    future.complete(responseHandler.handle(request, responseManager.createTaskInfoResponse(HttpStatus.OK)));
                                }
                            }
                            else if (method.equalsIgnoreCase("DELETE")) {
                                // DELETE /v1/task/{taskId}/results/{bufferId}
                                if (Pattern.compile(format("%s\\/results\\/[0-9]+\\z", TASK_ID_REGEX)).matcher(path).find()) {
                                    future.complete(responseHandler.handle(request, responseManager.createDummyResultResponse()));
                                }
                                // DELETE /v1/task/{taskId}
                                else if (Pattern.compile(TASK_ID_REGEX + "\\z").matcher(path).find()) {
                                    future.complete(responseHandler.handle(request, responseManager.createDummyResultResponse()));
                                }
                            }
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                            future.completeExceptionally(e);
                        }

                        if (!future.isDone()) {
                            future.completeExceptionally(new Exception(format("Unsupported request: %s %s", method, path)));
                        }
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
        private final TestingTaskInfoResponseManager taskInfoResponseManager;
        private final String taskId;

        public TestingResponseManager(String taskId)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = new TestingResultResponseManager();
            this.serverResponseManager = new TestingServerResponseManager();
            this.taskInfoResponseManager = new TestingTaskInfoResponseManager();
        }

        public TestingResponseManager(String taskId, TaskState taskState)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = new TestingResultResponseManager();
            this.serverResponseManager = new TestingServerResponseManager();
            this.taskInfoResponseManager = new TestingTaskInfoResponseManager(taskState);
        }

        public TestingResponseManager(String taskId, TestingResultResponseManager resultResponseManager)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = requireNonNull(resultResponseManager, "resultResponseManager is null.");
            this.serverResponseManager = new TestingServerResponseManager();
            this.taskInfoResponseManager = new TestingTaskInfoResponseManager();
        }

        public TestingResponseManager(String taskId, TestingServerResponseManager serverResponseManager)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = new TestingResultResponseManager();
            this.taskInfoResponseManager = new TestingTaskInfoResponseManager();
            this.serverResponseManager = requireNonNull(serverResponseManager, "serverResponseManager is null");
        }

        public TestingResponseManager(String taskId, TestingTaskInfoResponseManager taskInfoResponseManager)
        {
            this.taskId = requireNonNull(taskId, "taskId is null");
            this.resultResponseManager = new TestingResultResponseManager();
            this.serverResponseManager = new TestingServerResponseManager();
            this.taskInfoResponseManager = requireNonNull(taskInfoResponseManager, "taskInfoResponseManager is null");
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
                throws PrestoException
        {
            return taskInfoResponseManager.createTaskInfoResponse(httpStatus, taskId);
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

        /**
         * Manager for taskInfo fetching related endpoints. It maintains any stateful information inside itself. Callers can extend this class to create their own response handling
         * logic.
         */
        public static class TestingTaskInfoResponseManager
        {
            private final TaskState taskState;

            public TestingTaskInfoResponseManager()
            {
                taskState = TaskState.PLANNED;
            }

            public TestingTaskInfoResponseManager(TaskState taskState)
            {
                this.taskState = taskState;
            }

            public Response createTaskInfoResponse(HttpStatus httpStatus, String taskId)
                    throws PrestoException
            {
                URI location = uriBuilderFrom(BASE_URI).appendPath(TASK_ROOT_PATH).build();
                ListMultimap<HeaderName, String> headers = ArrayListMultimap.create();
                headers.put(HeaderName.of(CONTENT_TYPE), String.valueOf(MediaType.create("application", "json")));
                TaskInfo taskInfo = TaskInfo.createInitialTask(
                        TaskId.valueOf(taskId),
                        location,
                        new ArrayList<>(),
                        new TaskStats(DateTime.now(), null),
                        "dummy-node").withTaskStatus(createTaskStatusDone(location));
                return new TestingResponse(
                        httpStatus.code(),
                        httpStatus.toString(),
                        headers,
                        new ByteArrayInputStream(taskInfoCodec.toBytes(taskInfo)));
            }

            private TaskStatus createTaskStatusDone(URI location)
            {
                return new TaskStatus(
                        0L,
                        0L,
                        0,
                        taskState,
                        location,
                        ImmutableSet.of(),
                        ImmutableList.of(),
                        0,
                        0,
                        0.0,
                        false,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0,
                        0L,
                        0L);
            }
        }

        public static class CrashingTaskInfoResponseManager
                extends TestingResponseManager.TestingTaskInfoResponseManager
        {
            private final int successCount;
            private int attemptCount;

            public CrashingTaskInfoResponseManager(int successCount)
            {
                super();
                this.successCount = successCount;
            }

            @Override
            public Response createTaskInfoResponse(HttpStatus httpStatus, String taskId)
                    throws PrestoException
            {
                if (attemptCount++ > successCount) {
                    return super.createTaskInfoResponse(HttpStatus.INTERNAL_SERVER_ERROR, taskId);
                }

                throw new PrestoException(new TestErrorCode(), "Server refused connection");
            }
        }

        private static class TestErrorCode
                implements ErrorCodeSupplier
        {
            @Override
            public ErrorCode toErrorCode()
            {
                return new ErrorCode(0, "test", USER_ERROR);
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

    public static class FailureRetryResponseManager
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

    private static class FailureTaskInfoRetryResponseManager
            extends TestingResponseManager.TestingTaskInfoResponseManager
    {
        private final int failureCount;
        private int retryCount;

        public FailureTaskInfoRetryResponseManager(int failureCount)
        {
            super();
            this.failureCount = failureCount;
        }

        @Override
        public Response createTaskInfoResponse(HttpStatus httpStatus, String taskId)
                throws PrestoException
        {
            if (retryCount++ > failureCount) {
                return super.createTaskInfoResponse(HttpStatus.INTERNAL_SERVER_ERROR, taskId);
            }

            return super.createTaskInfoResponse(httpStatus, taskId);
        }
    }
}
