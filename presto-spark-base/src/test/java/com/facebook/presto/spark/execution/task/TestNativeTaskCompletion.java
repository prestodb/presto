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
package com.facebook.presto.spark.execution.task;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.client.ServerInfo;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spark.classloader_interface.PrestoSparkNonRetryableExecutionException;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSerializedPage;
import com.facebook.presto.spark.classloader_interface.SerializedTaskInfo;
import com.facebook.presto.spark.execution.PrestoSparkExecutionExceptionFactory;
import com.facebook.presto.spark.execution.http.BatchTaskUpdateRequest;
import com.facebook.presto.spark.execution.http.PrestoSparkHttpTaskClient;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient.TestingOkHttpClient;
import com.facebook.presto.spark.execution.http.TestPrestoSparkHttpClient.TestingResponseManager;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcess;
import com.facebook.presto.spark.execution.nativeprocess.NativeExecutionProcessFactory;
import com.facebook.presto.spark.execution.property.NativeExecutionCatalogConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionNodeConfig;
import com.facebook.presto.spark.execution.property.NativeExecutionSystemConfig;
import com.facebook.presto.spark.execution.property.PrestoSparkWorkerProperty;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.spark.util.CollectionAccumulator;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.execution.TaskState.FAILED;
import static com.facebook.presto.execution.TaskState.FINISHED;
import static com.facebook.presto.execution.TaskStatus.failWith;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.spark.util.PrestoSparkUtils.deserializeZstdCompressed;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.Failures.toFailure;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.expectThrows;

public class TestNativeTaskCompletion
{
    private static final String ROWS_PROCESSED = "llmRowsProcessed";
    private static final String ROWS_SUCCEEDED = "llmRowsSucceeded";
    private static final JsonCodec<TaskInfo> TASK_INFO_CODEC = JsonCodec.jsonCodec(TaskInfo.class);

    @Test
    public void testFinishedSnapshotSurvivesLaterFetchFailure()
    {
        try (TaskFixture fixture = new TaskFixture(terminalTaskInfo(FINISHED, ImmutableList.of()))) {
            assertFalse(fixture.iterator.hasNext());
            assertFalse(fixture.iterator.hasNext());
            assertCapturedReport(fixture, FINISHED, true);
        }
    }

    @Test
    public void testFailedSnapshotSurvivesLaterFetchFailure()
    {
        PrestoException originalFailure = new PrestoException(GENERIC_USER_ERROR, "failed native task");
        TaskInfo terminal = terminalTaskInfo(FAILED, ImmutableList.of(toFailure(originalFailure)));
        try (TaskFixture fixture = new TaskFixture(terminal)) {
            PrestoSparkNonRetryableExecutionException failure = expectThrows(PrestoSparkNonRetryableExecutionException.class, fixture.iterator::hasNext);
            ExecutionFailureInfo failureInfo = fixture.exceptionFactory.extractExecutionFailureInfo(failure).get();
            assertEquals(failureInfo.getMessage(), originalFailure.getMessage());
            assertEquals(failureInfo.getErrorCode(), originalFailure.getErrorCode());
            assertEquals(failure.getCause().getMessage(), originalFailure.getMessage());
            assertCapturedReport(fixture, FAILED, false);
        }
    }

    private static TaskInfo terminalTaskInfo(TaskState state, List<ExecutionFailureInfo> failures)
    {
        TaskInfo initial = TaskInfo.createInitialTask(new TaskId("query", 0, 0, 0, 0),
                URI.create("http://localhost/task"), ImmutableList.of(), new TaskStats(1L, 2L), "node");
        TaskInfo terminal = initial.withTaskStatus(failWith(initial.getTaskStatus(), state, failures));
        terminal.getStats().getRuntimeStats().addMetricValue(ROWS_PROCESSED, NONE, 100);
        terminal.getStats().getRuntimeStats().addMetricValue(ROWS_SUCCEEDED, NONE, 7);
        return terminal;
    }

    private static void assertCapturedReport(TaskFixture fixture, TaskState state, boolean success)
    {
        assertEquals(fixture.collector.value().size(), 1);
        TaskInfo collected = deserializeZstdCompressed(TASK_INFO_CODEC, fixture.collector.value().get(0).getBytesAndClear());
        assertEquals(collected.getTaskStatus().getState(), state);
        assertEquals(collected.getStats().getRuntimeStats().getMetric(ROWS_PROCESSED).getSum(), 100);
        assertEquals(collected.getStats().getRuntimeStats().getMetric(ROWS_SUCCEEDED).getSum(), 7);
        assertEquals(fixture.task.infoFetchCount, 1);
        assertEquals(fixture.task.stopCalls, ImmutableList.of(success));
        assertEquals(fixture.deleteTaskCount.get(), 1);
    }

    private static class TaskFixture
            implements AutoCloseable
    {
        private final ScheduledExecutorService executor = newScheduledThreadPool(2);
        private final CollectionAccumulator<SerializedTaskInfo> collector = new CollectionAccumulator<>();
        private final AtomicInteger deleteTaskCount = new AtomicInteger();
        private final PrestoSparkExecutionExceptionFactory exceptionFactory = new PrestoSparkExecutionExceptionFactory(JsonCodec.jsonCodec(ExecutionFailureInfo.class));
        private final NativeExecutionProcess process;
        private final TestingNativeTask task;
        private final PrestoSparkNativeTaskExecutorFactory.PrestoSparkNativeTaskOutputIterator<PrestoSparkSerializedPage> iterator;

        private TaskFixture(TaskInfo taskInfo)
        {
            Session session = testSessionBuilder().build();
            TestingResponseManager responseManager = new TestingResponseManager(taskInfo.getTaskId().toString())
            {
                @Override
                public Response createDummyResultResponse(Request request)
                {
                    if (request.method().equals("DELETE") && request.url().encodedPath().equals("/v1/task/" + taskInfo.getTaskId())) {
                        deleteTaskCount.incrementAndGet();
                    }
                    return super.createDummyResultResponse(request);
                }
            };
            OkHttpClient httpClient = new TestingOkHttpClient(executor, responseManager);
            PrestoSparkHttpTaskClient workerClient = new PrestoSparkHttpTaskClient(
                    httpClient, URI.create("http://localhost:8080"), TASK_INFO_CODEC,
                    JsonCodec.jsonCodec(PlanFragment.class), JsonCodec.jsonCodec(BatchTaskUpdateRequest.class),
                    new Duration(1, SECONDS), executor, executor, new Duration(1, SECONDS));
            PrestoSparkWorkerProperty workerProperty = new PrestoSparkWorkerProperty(
                    new NativeExecutionCatalogConfig(ImmutableMap.of()), new NativeExecutionNodeConfig(), new NativeExecutionSystemConfig(ImmutableMap.of()));
            process = new NativeExecutionProcessFactory(httpClient, executor, executor, JsonCodec.jsonCodec(ServerInfo.class), workerProperty, new FeaturesConfig())
                    .createNativeExecutionProcess(session, new Duration(1, SECONDS), Optional.empty());
            task = new TestingNativeTask(taskInfo, session, workerClient, executor);
            iterator = new PrestoSparkNativeTaskExecutorFactory.PrestoSparkNativeTaskOutputIterator<>(
                    0, task, PrestoSparkSerializedPage.class, collector, TASK_INFO_CODEC,
                    exceptionFactory, new PrestoSparkNativeTaskExecutorFactory.CpuTracker(),
                    process, false, new Duration(1, SECONDS));
        }

        @Override
        public void close()
        {
            try {
                process.close();
            }
            finally {
                executor.shutdownNow();
            }
        }
    }

    private static class TestingNativeTask
            extends NativeExecutionTask
    {
        private final TaskInfo taskInfo;
        private final List<Boolean> stopCalls = new ArrayList<>();
        private int infoFetchCount;

        private TestingNativeTask(TaskInfo taskInfo, Session session, PrestoSparkHttpTaskClient workerClient, ScheduledExecutorService executor)
        {
            super(taskInfo.getTaskId(), session, workerClient, createPlanFragment(), ImmutableList.of(),
                    new TableWriteInfo(Optional.empty(), Optional.empty()), Optional.of("shuffle-write"), Optional.empty(), executor, new TaskManagerConfig());
            this.taskInfo = taskInfo;
        }

        @Override
        public Optional<TaskInfo> getTaskInfo()
        {
            if (++infoFetchCount > 1) {
                throw new IllegalStateException("next info fetch failed");
            }
            return Optional.of(taskInfo);
        }

        @Override
        public boolean isTaskDone()
        {
            return taskInfo.getTaskStatus().getState().isDone();
        }

        @Override
        public void stop(boolean success)
        {
            stopCalls.add(success);
            super.stop(success);
        }
    }
}
