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
package com.facebook.presto.native_execution;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.jaxrs.JsonMapper;
import com.facebook.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import com.facebook.airlift.jaxrs.thrift.ThriftMapper;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.json.smile.SmileModule;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.drift.codec.utils.DataSizeToBytesThriftCodec;
import com.facebook.drift.codec.utils.DurationToMillisThriftCodec;
import com.facebook.drift.codec.utils.JodaDateTimeToEpochMillisThriftCodec;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.remotetask.RemoteTaskStats;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.json.smile.SmileCodecBinder.smileCodecBinder;
import static com.facebook.drift.codec.guice.ThriftCodecBinder.thriftCodecBinder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class TestNativeExecutionTask
{
    private static final boolean TRACE_HTTP = false;

    @Test(timeOut = 30000)
    public void testRegular()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, FailureScenario.NO_FAILURE);

        NativeExecutionTaskFactory nativeExecutionTaskFactory = createNativeExecutionTaskFactory(testingTaskResource);
        NativeExecutionTask nativeExecutionTask = createNativeExecutionTask(nativeExecutionTaskFactory);

        testingTaskResource.setInitialTaskInfo(nativeExecutionTask.getTaskInfo());
        nativeExecutionTask.start();
    }

    @Test(timeOut = 30000)
    public void testNoFailure()
            throws Exception
    {
        runTest(FailureScenario.NO_FAILURE);
    }

    private void runTest(FailureScenario failureScenario)
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, failureScenario);

        NativeExecutionTaskFactory nativeExecutionTaskFactory = createNativeExecutionTaskFactory(testingTaskResource);
        NativeExecutionTask nativeExecutionTask = createNativeExecutionTask(nativeExecutionTaskFactory);

        testingTaskResource.setInitialTaskInfo(nativeExecutionTask.getTaskInfo());
        nativeExecutionTask.start();

        Thread.sleep(10000);
        nativeExecutionTask.stop();
    }

    private NativeExecutionTask createNativeExecutionTask(NativeExecutionTaskFactory nativeExecutionTaskFactory)
    {
        InternalNode node = new InternalNode("node-id", URI.create("http://fake.invalid/"), new NodeVersion("version"), false);
        TaskId taskId = new TaskId("test", 1, 0, 2);
        return nativeExecutionTaskFactory.createNativeExecutionTask(
                TEST_SESSION,
                taskId,
                createPlanFragment(),
                ImmutableList.of(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()));
    }

    private static NativeExecutionTaskFactory createNativeExecutionTaskFactory(TestingTaskResource testingTaskResource)
    {
        Bootstrap app = new Bootstrap(
                new JsonModule(),
                new SmileModule(),
                new ThriftCodecModule(),
                new HandleJsonModule(),
                new Module()
                {
                    @Override
                    public void configure(Binder binder)
                    {
                        binder.bind(JsonMapper.class);
                        binder.bind(ThriftMapper.class);
                        configBinder(binder).bindConfig(FeaturesConfig.class);
                        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
                        binder.bind(TypeManager.class).toInstance(functionAndTypeManager);
                        jsonBinder(binder).addDeserializerBinding(Type.class).to(TypeDeserializer.class);
                        newSetBinder(binder, Type.class);
                        smileCodecBinder(binder).bindSmileCodec(TaskStatus.class);
                        smileCodecBinder(binder).bindSmileCodec(TaskInfo.class);
                        smileCodecBinder(binder).bindSmileCodec(TaskUpdateRequest.class);
                        smileCodecBinder(binder).bindSmileCodec(PlanFragment.class);
                        smileCodecBinder(binder).bindSmileCodec(MetadataUpdates.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
                        jsonCodecBinder(binder).bindJsonCodec(PlanFragment.class);
                        jsonCodecBinder(binder).bindJsonCodec(MetadataUpdates.class);
                        jsonBinder(binder).addKeySerializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionSerializer.class);
                        jsonBinder(binder).addKeyDeserializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionDeserializer.class);
                        thriftCodecBinder(binder).bindThriftCodec(TaskStatus.class);
                        thriftCodecBinder(binder).bindThriftCodec(TaskInfo.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(JodaDateTimeToEpochMillisThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(DurationToMillisThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(DataSizeToBytesThriftCodec.class);
                    }

                    @Provides
                    private NativeExecutionTaskFactory createNativeExecutionTaskFactory(
                            JsonMapper jsonMapper,
                            ThriftMapper thriftMapper,
                            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
                            JsonCodec<PlanFragment> planFragmentCodec,
                            JsonCodec<TaskInfo> taskInfoCodec)
                    {
                        JaxrsTestingHttpProcessor jaxrsTestingHttpProcessor = new JaxrsTestingHttpProcessor(URI.create("http://fake.invalid/"), testingTaskResource, jsonMapper, thriftMapper);
                        TestingHttpClient testingHttpClient = new TestingHttpClient(jaxrsTestingHttpProcessor.setTrace(TRACE_HTTP));
                        testingTaskResource.setHttpClient(testingHttpClient);
                        return new NativeExecutionTaskFactory(
                                testingHttpClient,
                                new RemoteTaskStats(),
                                taskUpdateRequestCodec,
                                planFragmentCodec,
                                taskInfoCodec,
                                newCachedThreadPool(),
                                newSingleThreadScheduledExecutor());
                    }
                });
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        handleResolver.addConnectorName("test", new TestingHandleResolver());
        return injector.getInstance(NativeExecutionTaskFactory.class);
    }

    private enum FailureScenario
    {
        NO_FAILURE,
        TASK_MISMATCH,
        TASK_MISMATCH_WHEN_VERSION_IS_HIGH,
        REJECTED_EXECUTION,
    }

    @Path("/task/{nodeId}")
    public static class TestingTaskResource
    {
        private static final UUID INITIAL_TASK_INSTANCE_ID = UUID.randomUUID();
        private static final UUID NEW_TASK_INSTANCE_ID = UUID.randomUUID();

        private final AtomicLong lastActivityNanos;
        private final FailureScenario failureScenario;

        private final AtomicReference<TestingHttpClient> httpClient = new AtomicReference<>();

        private TaskInfo initialTaskInfo;
        private TaskStatus initialTaskStatus;
        private long version;
        private TaskState taskState;
        private long taskInstanceIdLeastSignificantBits = INITIAL_TASK_INSTANCE_ID.getLeastSignificantBits();
        private long taskInstanceIdMostSignificantBits = INITIAL_TASK_INSTANCE_ID.getMostSignificantBits();

        private long statusFetchCounter;

        public TestingTaskResource(AtomicLong lastActivityNanos, FailureScenario failureScenario)
        {
            this.lastActivityNanos = requireNonNull(lastActivityNanos, "lastActivityNanos is null");
            this.failureScenario = requireNonNull(failureScenario, "failureScenario is null");
        }

        public void setHttpClient(TestingHttpClient newValue)
        {
            httpClient.set(newValue);
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

        Map<PlanNodeId, TaskSource> taskSourceMap = new HashMap<>();

        @POST
        @Path("{taskId}")
        @Consumes(MediaType.APPLICATION_JSON)
        @Produces(MediaType.APPLICATION_JSON)
        public synchronized TaskInfo createOrUpdateTask(
                @PathParam("taskId") TaskId taskId,
                TaskUpdateRequest taskUpdateRequest,
                @Context UriInfo uriInfo)
        {
            for (TaskSource source : taskUpdateRequest.getSources()) {
                taskSourceMap.compute(source.getPlanNodeId(), (planNodeId, taskSource) -> taskSource == null ? source : taskSource.update(source));
            }
            lastActivityNanos.set(System.nanoTime());
            return buildTaskInfo();
        }

        public void setInitialTaskInfo(TaskInfo initialTaskInfo)
        {
            this.initialTaskInfo = initialTaskInfo;
            this.initialTaskStatus = initialTaskInfo.getTaskStatus();
            this.taskState = initialTaskStatus.getState();
            this.version = initialTaskStatus.getVersion();
            switch (failureScenario) {
                case TASK_MISMATCH_WHEN_VERSION_IS_HIGH:
                    // Make the initial version large enough.
                    // This way, the version number can't be reached if it is reset to 0.
                    version = 1_000_000;
                    break;
                case TASK_MISMATCH:
                case REJECTED_EXECUTION:
                case NO_FAILURE:
                    break; // do nothing
                default:
                    throw new UnsupportedOperationException();
            }
        }

        private TaskInfo buildTaskInfo()
        {
            return new TaskInfo(
                    initialTaskInfo.getTaskId(),
                    buildTaskStatus(),
                    initialTaskInfo.getLastHeartbeat(),
                    initialTaskInfo.getOutputBuffers(),
                    initialTaskInfo.getNoMoreSplits(),
                    initialTaskInfo.getStats(),
                    initialTaskInfo.isNeedsPlan(),
                    initialTaskInfo.getMetadataUpdates(),
                    initialTaskInfo.getNodeId());
        }

        private TaskStatus buildTaskStatus()
        {
            statusFetchCounter++;
            // Change the task instance id after 10th fetch to simulate worker restart
            switch (failureScenario) {
                case TASK_MISMATCH:
                case TASK_MISMATCH_WHEN_VERSION_IS_HIGH:
                    if (statusFetchCounter == 10) {
                        taskInstanceIdLeastSignificantBits = NEW_TASK_INSTANCE_ID.getLeastSignificantBits();
                        taskInstanceIdMostSignificantBits = NEW_TASK_INSTANCE_ID.getMostSignificantBits();
                        version = 0;
                    }
                    break;
                case REJECTED_EXECUTION:
                    if (statusFetchCounter >= 10) {
                        httpClient.get().close();
                        throw new RejectedExecutionException();
                    }
                    break;
                case NO_FAILURE:
                    break;
                default:
                    throw new UnsupportedOperationException();
            }

            return new TaskStatus(
                    taskInstanceIdLeastSignificantBits,
                    taskInstanceIdMostSignificantBits,
                    ++version,
                    taskState,
                    initialTaskStatus.getSelf(),
                    ImmutableSet.of(),
                    initialTaskStatus.getFailures(),
                    initialTaskStatus.getQueuedPartitionedDrivers(),
                    initialTaskStatus.getRunningPartitionedDrivers(),
                    initialTaskStatus.getOutputBufferUtilization(),
                    initialTaskStatus.isOutputBufferOverutilized(),
                    initialTaskStatus.getPhysicalWrittenDataSizeInBytes(),
                    initialTaskStatus.getMemoryReservationInBytes(),
                    initialTaskStatus.getSystemMemoryReservationInBytes(),
                    initialTaskStatus.getPeakNodeTotalMemoryReservationInBytes(),
                    initialTaskStatus.getFullGcCount(),
                    initialTaskStatus.getFullGcTimeInMillis(),
                    initialTaskStatus.getTotalCpuTimeInNanos(),
                    initialTaskStatus.getTaskAgeInMillis(),
                    initialTaskStatus.getQueuedPartitionedSplitsWeight(),
                    initialTaskStatus.getRunningPartitionedSplitsWeight());
        }
    }
}
