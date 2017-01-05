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

package com.facebook.presto.server.remotetask;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.TaskTestUtils;
import com.facebook.presto.execution.TestSqlTaskManager;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.server.HttpRemoteTaskFactory;
import com.facebook.presto.server.TaskUpdateRequest;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHttpRemoteTask
{
    // This timeout should never be reached because a daemon thread in test should fail the test and do proper cleanup.
    @Test(timeOut = 30000)
    public void testRemoteTaskMismatch()
            throws InterruptedException, ExecutionException
    {
        Duration idleTimeout = new Duration(3, SECONDS);
        Duration failTimeout = new Duration(20, SECONDS);

        JsonCodec<TaskStatus> taskStatusCodec = JsonCodec.jsonCodec(TaskStatus.class);
        JsonCodec<TaskInfo> taskInfoCodec = JsonCodec.jsonCodec(TaskInfo.class);
        TaskManagerConfig taskManagerConfig = new TaskManagerConfig();

        // Shorten status refresh wait and info update interval so that we can have a shorter test timeout
        taskManagerConfig.setStatusRefreshMaxWait(new Duration(idleTimeout.roundTo(MILLISECONDS) / 100, MILLISECONDS));
        taskManagerConfig.setInfoUpdateInterval(new Duration(idleTimeout.roundTo(MILLISECONDS) / 10, MILLISECONDS));

        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        HttpProcessor httpProcessor = new HttpProcessor(taskStatusCodec, taskInfoCodec, lastActivityNanos);
        TestingHttpClient testingHttpClient = new TestingHttpClient(httpProcessor);

        HttpRemoteTaskFactory httpRemoteTaskFactory = new HttpRemoteTaskFactory(
                new QueryManagerConfig(),
                taskManagerConfig,
                testingHttpClient,
                new TestSqlTaskManager.MockLocationFactory(),
                taskStatusCodec,
                taskInfoCodec,
                JsonCodec.jsonCodec(TaskUpdateRequest.class),
                new RemoteTaskStats());
        RemoteTask remoteTask = httpRemoteTaskFactory.createRemoteTask(
                TEST_SESSION,
                new TaskId("test", 1, 2),
                new PrestoNode("node-id", URI.create("http://192.0.1.2"), new NodeVersion("version"), false),
                TaskTestUtils.PLAN_FRAGMENT,
                ImmutableMultimap.of(),
                createInitialEmptyOutputBuffers(OutputBuffers.BufferType.BROADCAST),
                new NodeTaskMap.PartitionedSplitCountTracker(i -> { }),
                true);

        httpProcessor.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        CompletableFuture<Void> testComplete = new CompletableFuture<>();
        asyncRun(
                idleTimeout.roundTo(MILLISECONDS),
                failTimeout.roundTo(MILLISECONDS),
                lastActivityNanos,
                () -> testComplete.complete(null),
                (message, cause) -> testComplete.completeExceptionally(new AssertionError(message, cause)));
        testComplete.get();

        httpRemoteTaskFactory.stop();
        assertTrue(remoteTask.getTaskStatus().getState().isDone(), format("TaskStatus is not in a done state: %s", remoteTask.getTaskStatus()));
        assertEquals(getOnlyElement(remoteTask.getTaskStatus().getFailures()).getErrorCode(), REMOTE_TASK_MISMATCH.toErrorCode());
        assertTrue(remoteTask.getTaskInfo().getTaskStatus().getState().isDone(), format("TaskInfo is not in a done state: %s", remoteTask.getTaskInfo()));
    }

    private static void asyncRun(long idleTimeoutMillis, long failTimeoutMillis, AtomicLong lastActivityNanos, Runnable runAfterIdle, BiConsumer<String, Throwable> runAfterFail)
    {
        new Thread(() -> {
            long startTimeNanos = System.nanoTime();

            try {
                while (true) {
                    long millisSinceLastActivity = (System.nanoTime() - lastActivityNanos.get()) / 1_000_000L;
                    long millisSinceStart = (System.nanoTime() - startTimeNanos) / 1_000_000L;
                    long millisToIdleTarget = idleTimeoutMillis - millisSinceLastActivity;
                    long millisToFailTarget = failTimeoutMillis - millisSinceStart;
                    if (millisToFailTarget < millisToIdleTarget) {
                        runAfterFail.accept(format("Activity doesn't stop after %sms", failTimeoutMillis), null);
                        return;
                    }
                    if (millisToIdleTarget < 0) {
                        runAfterIdle.run();
                        return;
                    }
                    Thread.sleep(millisToIdleTarget);
                }
            }
            catch (InterruptedException e) {
                runAfterFail.accept("Idle/fail timeout monitor thread interrupted", e);
            }
        }).start();
    }

    private static class HttpProcessor implements TestingHttpClient.Processor
    {
        private static final String INITIAL_TASK_INSTANCE_ID = "task-instance-id";
        private static final String NEW_TASK_INSTANCE_ID = "task-instance-id-x";
        private final JsonCodec<TaskStatus> taskStatusCodec;
        private final JsonCodec<TaskInfo> taskInfoCodec;
        private final AtomicLong lastActivityNanos;

        private TaskInfo initialTaskInfo;
        private TaskStatus initialTaskStatus;
        private long version;
        private TaskState taskState;

        private long statusFetchCounter;
        private String taskInstanceId = INITIAL_TASK_INSTANCE_ID;

        public HttpProcessor(JsonCodec<TaskStatus> taskStatusCodec, JsonCodec<TaskInfo> taskInfoCodec, AtomicLong lastActivityNanos)
        {
            this.taskStatusCodec = taskStatusCodec;
            this.taskInfoCodec = taskInfoCodec;
            this.lastActivityNanos = lastActivityNanos;
        }

        @Override
        public synchronized Response handle(Request request)
                throws Exception
        {
            lastActivityNanos.set(System.nanoTime());

            ImmutableListMultimap.Builder<String, String> headers = ImmutableListMultimap.builder();
            headers.put(PRESTO_TASK_INSTANCE_ID, taskInstanceId);
            headers.put(CONTENT_TYPE, "application/json");

            if (request.getUri().getPath().endsWith("/status")) {
                statusFetchCounter++;
                if (statusFetchCounter >= 10) {
                    // Change the task instance id after 10th fetch to simulate worker restart
                    taskInstanceId = NEW_TASK_INSTANCE_ID;
                }
                wait(Duration.valueOf(request.getHeader(PRESTO_MAX_WAIT)).roundTo(MILLISECONDS));
                return new TestingResponse(HttpStatus.OK, headers.build(), taskStatusCodec.toJson(buildTaskStatus()).getBytes(StandardCharsets.UTF_8));
            }
            if ("DELETE".equals(request.getMethod())) {
                taskState = TaskState.ABORTED;
            }
            return new TestingResponse(HttpStatus.OK, headers.build(), taskInfoCodec.toJson(buildTaskInfo()).getBytes(StandardCharsets.UTF_8));
        }

        public void setInitialTaskInfo(TaskInfo initialTaskInfo)
        {
            this.initialTaskInfo = initialTaskInfo;
            this.initialTaskStatus = initialTaskInfo.getTaskStatus();
            this.taskState = initialTaskStatus.getState();
            this.version = initialTaskStatus.getVersion();
        }

        private TaskInfo buildTaskInfo()
        {
            return new TaskInfo(
                    buildTaskStatus(),
                    initialTaskInfo.getLastHeartbeat(),
                    initialTaskInfo.getOutputBuffers(),
                    initialTaskInfo.getNoMoreSplits(),
                    initialTaskInfo.getStats(),
                    initialTaskInfo.isNeedsPlan(),
                    initialTaskInfo.isComplete());
        }

        private TaskStatus buildTaskStatus()
        {
            return new TaskStatus(
                    initialTaskStatus.getTaskId(),
                    taskInstanceId,
                    ++version,
                    taskState,
                    initialTaskStatus.getSelf(),
                    initialTaskStatus.getFailures(),
                    initialTaskStatus.getQueuedPartitionedDrivers(),
                    initialTaskStatus.getRunningPartitionedDrivers(),
                    initialTaskStatus.getMemoryReservation());
        }
    }
}
