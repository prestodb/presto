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
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.server.HttpRemoteTaskFactory;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.type.TypeDeserializer;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.jaxrs.JsonMapper;
import io.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertTrue;

public class TestHttpRemoteTask
{
    // This 30 sec per-test timeout should never be reached because the test should fail and do proper cleanup after 20 sec.
    private static final Duration IDLE_TIMEOUT = new Duration(3, SECONDS);
    private static final Duration FAIL_TIMEOUT = new Duration(20, SECONDS);
    private static final TaskManagerConfig TASK_MANAGER_CONFIG = new TaskManagerConfig()
            // Shorten status refresh wait and info update interval so that we can have a shorter test timeout
            .setStatusRefreshMaxWait(new Duration(IDLE_TIMEOUT.roundTo(MILLISECONDS) / 100, MILLISECONDS))
            .setInfoUpdateInterval(new Duration(IDLE_TIMEOUT.roundTo(MILLISECONDS) / 10, MILLISECONDS));

    private static final boolean TRACE_HTTP = false;

    @Test(timeOut = 30000)
    public void testRemoteTaskMismatch()
            throws Exception
    {
        runTest(TestCase.TASK_MISMATCH);
    }

    @Test(timeOut = 30000)
    public void testRejectedExecutionWhenVersionIsHigh()
            throws Exception
    {
        runTest(TestCase.TASK_MISMATCH_WHEN_VERSION_IS_HIGH);
    }

    @Test(timeOut = 30000)
    public void testRejectedExecution()
            throws Exception
    {
        runTest(TestCase.REJECTED_EXECUTION);
    }

    private void runTest(TestCase testCase)
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, testCase);

        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource);

        RemoteTask remoteTask = httpRemoteTaskFactory.createRemoteTask(
                TEST_SESSION,
                new TaskId("test", 1, 2),
                new PrestoNode("node-id", URI.create("http://fake.invalid/"), new NodeVersion("version"), false),
                TaskTestUtils.PLAN_FRAGMENT,
                ImmutableMultimap.of(),
                createInitialEmptyOutputBuffers(OutputBuffers.BufferType.BROADCAST),
                new NodeTaskMap.PartitionedSplitCountTracker(i -> {}),
                true);

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        CompletableFuture<Void> testComplete = new CompletableFuture<>();
        asyncRun(
                IDLE_TIMEOUT.roundTo(MILLISECONDS),
                FAIL_TIMEOUT.roundTo(MILLISECONDS),
                lastActivityNanos,
                () -> testComplete.complete(null),
                (message, cause) -> testComplete.completeExceptionally(new AssertionError(message, cause)));
        testComplete.get();

        httpRemoteTaskFactory.stop();
        assertTrue(remoteTask.getTaskStatus().getState().isDone(), format("TaskStatus is not in a done state: %s", remoteTask.getTaskStatus()));
        assertTrue(remoteTask.getTaskInfo().getTaskStatus().getState().isDone(), format("TaskInfo is not in a done state: %s", remoteTask.getTaskInfo()));

        ErrorCode actualErrorCode = getOnlyElement(remoteTask.getTaskStatus().getFailures()).getErrorCode();
        switch (testCase) {
            case TASK_MISMATCH:
            case TASK_MISMATCH_WHEN_VERSION_IS_HIGH:
                assertEquals(actualErrorCode, REMOTE_TASK_MISMATCH.toErrorCode());
                break;
            case REJECTED_EXECUTION:
                assertEquals(actualErrorCode, REMOTE_TASK_ERROR.toErrorCode());
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static HttpRemoteTaskFactory createHttpRemoteTaskFactory(TestingTaskResource testingTaskResource)
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new HandleJsonModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(JsonMapper.class);
                        configBinder(binder).bindConfig(FeaturesConfig.class);
                        binder.bind(TypeRegistry.class).in(Scopes.SINGLETON);
                        binder.bind(TypeManager.class).to(TypeRegistry.class).in(Scopes.SINGLETON);
                        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
                        newSetBinder(binder, Type.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
                    }

                    @Provides
                    private HttpRemoteTaskFactory createHttpRemoteTaskFactory(
                            JsonMapper jsonMapper,
                            JsonCodec<TaskStatus> taskStatusCodec,
                            JsonCodec<TaskInfo> taskInfoCodec,
                            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec)
                    {
                        JaxrsTestingHttpProcessor jaxrsTestingHttpProcessor = new JaxrsTestingHttpProcessor(URI.create("http://fake.invalid/"), testingTaskResource, jsonMapper);
                        TestingHttpClient testingHttpClient = new TestingHttpClient(jaxrsTestingHttpProcessor.setTrace(TRACE_HTTP));
                        return new HttpRemoteTaskFactory(
                                new QueryManagerConfig(),
                                TASK_MANAGER_CONFIG,
                                testingHttpClient,
                                new TestSqlTaskManager.MockLocationFactory(),
                                taskStatusCodec,
                                taskInfoCodec,
                                taskUpdateRequestCodec,
                                new RemoteTaskStats());
                    }
                });
        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        handleResolver.addConnectorName("test", new TestingHandleResolver());
        return injector.getInstance(HttpRemoteTaskFactory.class);
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

    private enum TestCase
    {
        TASK_MISMATCH,
        TASK_MISMATCH_WHEN_VERSION_IS_HIGH,
        REJECTED_EXECUTION
    }

    @Path("/task/{nodeId}")
    public static class TestingTaskResource
    {
        private static final String INITIAL_TASK_INSTANCE_ID = "task-instance-id";
        private static final String NEW_TASK_INSTANCE_ID = "task-instance-id-x";

        private final AtomicLong lastActivityNanos;
        private final TestCase testCase;

        private TaskInfo initialTaskInfo;
        private TaskStatus initialTaskStatus;
        private long version;
        private TaskState taskState;
        private String taskInstanceId = INITIAL_TASK_INSTANCE_ID;

        private long statusFetchCounter;

        public TestingTaskResource(AtomicLong lastActivityNanos, TestCase testCase)
        {
            this.lastActivityNanos = requireNonNull(lastActivityNanos, "lastActivityNanos is null");
            this.testCase = requireNonNull(testCase, "testCase is null");
        }

        @GET
        @Path("{taskId}")
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized TaskInfo getTaskInfo(
                @PathParam("taskId") final TaskId taskId,
                @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
                @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
                @Context UriInfo uriInfo)
        {
            lastActivityNanos.set(System.nanoTime());
            return buildTaskInfo();
        }

        @POST
        @Path("{taskId}")
        @Consumes(MediaType.APPLICATION_JSON)
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized TaskInfo createOrUpdateTask(
                @PathParam("taskId") TaskId taskId,
                TaskUpdateRequest taskUpdateRequest,
                @Context UriInfo uriInfo)
        {
            lastActivityNanos.set(System.nanoTime());
            return buildTaskInfo();
        }

        @GET
        @Path("{taskId}/status")
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized TaskStatus getTaskStatus(
                @PathParam("taskId") TaskId taskId,
                @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
                @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
                @Context UriInfo uriInfo)
                throws InterruptedException
        {
            lastActivityNanos.set(System.nanoTime());

            wait(maxWait.roundTo(MILLISECONDS));
            return buildTaskStatus();
        }

        @DELETE
        @Path("{taskId}")
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized TaskInfo deleteTask(
                @PathParam("taskId") TaskId taskId,
                @QueryParam("abort") @DefaultValue("true") boolean abort,
                @Context UriInfo uriInfo)
        {
            lastActivityNanos.set(System.nanoTime());

            taskState = abort ? TaskState.ABORTED : TaskState.CANCELED;
            return buildTaskInfo();
        }

        public void setInitialTaskInfo(TaskInfo initialTaskInfo)
        {
            this.initialTaskInfo = initialTaskInfo;
            this.initialTaskStatus = initialTaskInfo.getTaskStatus();
            this.taskState = initialTaskStatus.getState();
            this.version = initialTaskStatus.getVersion();
            switch (testCase) {
                case TASK_MISMATCH_WHEN_VERSION_IS_HIGH:
                    // Make the initial version large enough.
                    // This way, the version number can't be reached if it is reset to 0.
                    version = 1_000_000;
                    break;
                case TASK_MISMATCH:
                case REJECTED_EXECUTION:
                    break; // do nothing
                default:
                    throw new UnsupportedOperationException();
            }
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
            statusFetchCounter++;
            // Change the task instance id after 10th fetch to simulate worker restart
            switch (testCase) {
                case TASK_MISMATCH:
                case TASK_MISMATCH_WHEN_VERSION_IS_HIGH:
                    if (statusFetchCounter == 10) {
                        taskInstanceId = NEW_TASK_INSTANCE_ID;
                        version = 0;
                    }
                    break;
                case REJECTED_EXECUTION:
                    if (statusFetchCounter >= 10) {
                        throw new RejectedExecutionException();
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            return new TaskStatus(
                    initialTaskStatus.getTaskId(),
                    taskInstanceId,
                    ++version,
                    taskState,
                    initialTaskStatus.getSelf(),
                    "fake",
                    initialTaskStatus.getFailures(),
                    initialTaskStatus.getQueuedPartitionedDrivers(),
                    initialTaskStatus.getRunningPartitionedDrivers(),
                    initialTaskStatus.getMemoryReservation());
        }
    }
}
