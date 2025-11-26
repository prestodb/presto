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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.http.client.testing.TestingHttpClient;
import com.facebook.airlift.jaxrs.JsonMapper;
import com.facebook.airlift.jaxrs.testing.JaxrsTestingHttpProcessor;
import com.facebook.airlift.jaxrs.thrift.ThriftMapper;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.json.JsonModule;
import com.facebook.airlift.json.smile.SmileCodec;
import com.facebook.airlift.json.smile.SmileModule;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.drift.codec.ThriftCodec;
import com.facebook.drift.codec.guice.ThriftCodecModule;
import com.facebook.drift.codec.utils.DataSizeToBytesThriftCodec;
import com.facebook.drift.codec.utils.DurationToMillisThriftCodec;
import com.facebook.drift.codec.utils.JodaDateTimeToEpochMillisThriftCodec;
import com.facebook.drift.codec.utils.LocaleToLanguageTagCodec;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.connector.ConnectorCodecManager;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SchedulerStatsTracker;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.TestQueryManager;
import com.facebook.presto.execution.TestSqlTaskManager;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.HandleJsonModule;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.server.InternalCommunicationConfig;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.thrift.ConnectorSplitThriftCodec;
import com.facebook.presto.server.thrift.DeleteTableHandleThriftCodec;
import com.facebook.presto.server.thrift.InsertTableHandleThriftCodec;
import com.facebook.presto.server.thrift.MergeTableHandleThriftCodec;
import com.facebook.presto.server.thrift.OutputTableHandleThriftCodec;
import com.facebook.presto.server.thrift.TableHandleThriftCodec;
import com.facebook.presto.server.thrift.TableLayoutHandleThriftCodec;
import com.facebook.presto.server.thrift.TransactionHandleThriftCodec;
import com.facebook.presto.spi.ConnectorDeleteTableHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorMergeTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.Serialization;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.type.TypeDeserializer;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.UriInfo;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_BINARY;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_COMPACT;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_FB_COMPACT;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.json.smile.SmileCodecBinder.smileCodecBinder;
import static com.facebook.drift.codec.guice.ThriftCodecBinder.thriftCodecBinder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.spi.SplitContext.NON_CACHEABLE;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.REMOTE_TASK_MISMATCH;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

public class TestHttpRemoteTaskWithEventLoop
{
    // This 30 sec per-test timeout should never be reached because the test should fail and do proper cleanup after 20 sec.
    private static final Duration POLL_TIMEOUT = new Duration(100, MILLISECONDS);
    private static final Duration IDLE_TIMEOUT = new Duration(3, SECONDS);
    private static final Duration FAIL_TIMEOUT = new Duration(40, SECONDS);
    private static final TaskManagerConfig TASK_MANAGER_CONFIG = new TaskManagerConfig()
            // Shorten status refresh wait and info update interval so that we can have a shorter test timeout
            .setStatusRefreshMaxWait(new Duration(IDLE_TIMEOUT.roundTo(MILLISECONDS) / 100, MILLISECONDS))
            .setInfoUpdateInterval(new Duration(IDLE_TIMEOUT.roundTo(MILLISECONDS) / 10, MILLISECONDS))
            .setEventLoopEnabled(true);

    private static final boolean TRACE_HTTP = false;

    @DataProvider
    public Object[][] thriftEncodingToggle()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(timeOut = 50000, dataProvider = "thriftEncodingToggle")
    public void testRemoteTaskMismatch(boolean useThriftEncoding)
            throws Exception
    {
        runTest(FailureScenario.TASK_MISMATCH, useThriftEncoding);
    }

    @Test(timeOut = 50000, dataProvider = "thriftEncodingToggle")
    public void testRejectedExecutionWhenVersionIsHigh(boolean useThriftEncoding)
            throws Exception
    {
        runTest(FailureScenario.TASK_MISMATCH_WHEN_VERSION_IS_HIGH, useThriftEncoding);
    }

    @Test(timeOut = 40000, dataProvider = "thriftEncodingToggle")
    public void testRejectedExecution(boolean useThriftEncoding)
            throws Exception
    {
        runTest(FailureScenario.REJECTED_EXECUTION, useThriftEncoding);
    }

    @Test(timeOut = 60000, dataProvider = "thriftEncodingToggle")
    public void testRegular(boolean useThriftEncoding)
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, FailureScenario.NO_FAILURE);

        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource, useThriftEncoding);

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory);

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        Lifespan lifespan = Lifespan.driverGroup(3);
        remoteTask.addSplits(ImmutableMultimap.of(TABLE_SCAN_NODE_ID, new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit(), lifespan, NON_CACHEABLE)));
        poll(() -> testingTaskResource.getTaskSource(TABLE_SCAN_NODE_ID) != null);
        poll(() -> testingTaskResource.getTaskSource(TABLE_SCAN_NODE_ID).getSplits().size() == 1);

        remoteTask.noMoreSplits(TABLE_SCAN_NODE_ID, lifespan);
        poll(() -> testingTaskResource.getTaskSource(TABLE_SCAN_NODE_ID).getNoMoreSplitsForLifespan().size() == 1);

        remoteTask.noMoreSplits(TABLE_SCAN_NODE_ID);
        poll(() -> testingTaskResource.getTaskSource(TABLE_SCAN_NODE_ID).isNoMoreSplits());

        remoteTask.cancel();
        poll(() -> remoteTask.getTaskStatus().getState().isDone());
        poll(() -> remoteTask.getTaskInfo().getTaskStatus().getState().isDone());

        httpRemoteTaskFactory.stop();
    }

    @Test(timeOut = 50000)
    public void testHTTPRemoteTaskSize()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, FailureScenario.NO_FAILURE);

        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource, false);

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory);

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();
        // just need to run a TaskUpdateRequest to increment the decay counter
        remoteTask.cancel();

        waitUntilTaskFinish(remoteTask);

        httpRemoteTaskFactory.stop();

        assertTrue(httpRemoteTaskFactory.getTaskUpdateRequestSize() > 0);
    }

    @Test(timeOut = 50000)
    public void testHTTPRemoteBadTaskSize()
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, FailureScenario.NO_FAILURE);
        boolean useThriftEncoding = false;
        DataSize maxDataSize = DataSize.succinctBytes(1024);
        InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig()
                .setThriftTransportEnabled(useThriftEncoding)
                .setMaxTaskUpdateSize(maxDataSize);

        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource, useThriftEncoding, internalCommunicationConfig);

        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory);
        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        waitUntilTaskFinish(remoteTask);

        httpRemoteTaskFactory.stop();

        assertTrue(remoteTask.getTaskStatus().getState().isDone(), format("TaskStatus is not in a done state: %s", remoteTask.getTaskStatus()));
        assertThat(getOnlyElement(remoteTask.getTaskStatus().getFailures()).getMessage())
                .matches("TaskUpdate size of .+? has exceeded the limit of 1kB");
    }

    @Test(dataProvider = "getUpdateSize")
    public void testGetExceededTaskUpdateSizeListMessage(int updateSizeInBytes, int maxDataSizeInBytes,
            String expectedMessage)
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, FailureScenario.NO_FAILURE);
        boolean useThriftEncoding = false;
        DataSize maxDataSize = DataSize.succinctBytes(maxDataSizeInBytes);
        InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig()
                .setThriftTransportEnabled(useThriftEncoding)
                .setMaxTaskUpdateSize(maxDataSize);
        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource, useThriftEncoding, internalCommunicationConfig);
        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory);

        Method targetMethod = HttpRemoteTaskWithEventLoop.class.getDeclaredMethod("getExceededTaskUpdateSizeMessage", new Class[] {byte[].class});
        targetMethod.setAccessible(true);
        byte[] taskUpdateRequestJson = new byte[updateSizeInBytes];
        String message = (String) targetMethod.invoke(remoteTask, new Object[] {taskUpdateRequestJson});
        assertEquals(message, expectedMessage);
    }

    @DataProvider(name = "getUpdateSize")
    protected Object[][] getUpdateSize()
    {
        return new Object[][] {
                {2000, 1000, "TaskUpdate size of 1.95kB has exceeded the limit of 1000B"},
                {2000, 1024, "TaskUpdate size of 1.95kB has exceeded the limit of 1kB"},
                {5000, 4 * 1024, "TaskUpdate size of 4.88kB has exceeded the limit of 4kB"},
                {2 * 1024, 1024, "TaskUpdate size of 2kB has exceeded the limit of 1kB"},
                {1024 * 1024, 512 * 1024, "TaskUpdate size of 1MB has exceeded the limit of 512kB"},
                {16 * 1024 * 1024, 8 * 1024 * 1024, "TaskUpdate size of 16MB has exceeded the limit of 8MB"},
                {485 * 1000 * 1000, 1024 * 1024 * 512, "TaskUpdate size of 462.53MB has exceeded the limit of 512MB"},
                {1024 * 1024 * 1024, 1024 * 1024 * 512, "TaskUpdate size of 1GB has exceeded the limit of 512MB"},
                {860492511, 524288000, "TaskUpdate size of 820.63MB has exceeded the limit of 500MB"}};
    }

    private void runTest(FailureScenario failureScenario, boolean useThriftEncoding)
            throws Exception
    {
        AtomicLong lastActivityNanos = new AtomicLong(System.nanoTime());
        TestingTaskResource testingTaskResource = new TestingTaskResource(lastActivityNanos, failureScenario);

        HttpRemoteTaskFactory httpRemoteTaskFactory = createHttpRemoteTaskFactory(testingTaskResource, useThriftEncoding);
        RemoteTask remoteTask = createRemoteTask(httpRemoteTaskFactory);

        testingTaskResource.setInitialTaskInfo(remoteTask.getTaskInfo());
        remoteTask.start();

        waitUntilTaskFinish(remoteTask);

        httpRemoteTaskFactory.stop();
        assertTrue(remoteTask.getTaskStatus().getState().isDone(), format("TaskStatus is not in a done state: %s", remoteTask.getTaskStatus()));

        ErrorCode actualErrorCode = getOnlyElement(remoteTask.getTaskStatus().getFailures()).getErrorCode();
        switch (failureScenario) {
            case TASK_MISMATCH:
            case TASK_MISMATCH_WHEN_VERSION_IS_HIGH:
                assertTrue(remoteTask.getTaskInfo().getTaskStatus().getState().isDone(), format("TaskInfo is not in a done state: %s", remoteTask.getTaskInfo()));
                assertEquals(actualErrorCode, REMOTE_TASK_MISMATCH.toErrorCode());
                break;
            case REJECTED_EXECUTION:
                // for a rejection to occur, the http client must be shutdown, which means we will not be able to ge the final task info
                assertEquals(actualErrorCode, REMOTE_TASK_ERROR.toErrorCode());
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    private RemoteTask createRemoteTask(HttpRemoteTaskFactory httpRemoteTaskFactory)
    {
        return httpRemoteTaskFactory.createRemoteTask(
                TEST_SESSION,
                new TaskId("test", 1, 0, 2, 0),
                new InternalNode("node-id", URI.create("http://fake.invalid/"), new NodeVersion("version"), false),
                createPlanFragment(),
                ImmutableMultimap.of(),
                createInitialEmptyOutputBuffers(OutputBuffers.BufferType.BROADCAST),
                new NodeTaskMap.NodeStatsTracker(i -> {}, i -> {}, (age, i) -> {}),
                true,
                new TableWriteInfo(Optional.empty(), Optional.empty()),
                SchedulerStatsTracker.NOOP);
    }

    private static HttpRemoteTaskFactory createHttpRemoteTaskFactory(TestingTaskResource testingTaskResource, boolean useThriftEncoding)
            throws Exception
    {
        InternalCommunicationConfig internalCommunicationConfig = new InternalCommunicationConfig().setThriftTransportEnabled(useThriftEncoding);
        return createHttpRemoteTaskFactory(testingTaskResource, useThriftEncoding, internalCommunicationConfig);
    }

    private static HttpRemoteTaskFactory createHttpRemoteTaskFactory(TestingTaskResource testingTaskResource, boolean useThriftEncoding, InternalCommunicationConfig internalCommunicationConfig)
            throws Exception
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
                        binder.bind(ConnectorManager.class).toProvider(() -> null).in(Scopes.SINGLETON);
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
                        jsonCodecBinder(binder).bindJsonCodec(TaskStatus.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskInfo.class);
                        jsonCodecBinder(binder).bindJsonCodec(TaskUpdateRequest.class);
                        jsonCodecBinder(binder).bindJsonCodec(PlanFragment.class);
                        jsonCodecBinder(binder).bindJsonCodec(TableWriteInfo.class);
                        jsonBinder(binder).addKeySerializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionSerializer.class);
                        jsonBinder(binder).addKeyDeserializerBinding(VariableReferenceExpression.class).to(Serialization.VariableReferenceExpressionDeserializer.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorSplit.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorTransactionHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorOutputTableHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorDeleteTableHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorInsertTableHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorMergeTableHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorTableHandle.class);
                        jsonCodecBinder(binder).bindJsonCodec(ConnectorTableLayoutHandle.class);

                        binder.bind(ConnectorCodecManager.class).in(Scopes.SINGLETON);

                        thriftCodecBinder(binder).bindCustomThriftCodec(ConnectorSplitThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(TransactionHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(OutputTableHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(InsertTableHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(DeleteTableHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(MergeTableHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(TableHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(TableLayoutHandleThriftCodec.class);
                        thriftCodecBinder(binder).bindThriftCodec(TaskStatus.class);
                        thriftCodecBinder(binder).bindThriftCodec(TaskInfo.class);
                        thriftCodecBinder(binder).bindThriftCodec(TaskUpdateRequest.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(LocaleToLanguageTagCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(JodaDateTimeToEpochMillisThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(DurationToMillisThriftCodec.class);
                        thriftCodecBinder(binder).bindCustomThriftCodec(DataSizeToBytesThriftCodec.class);
                    }

                    @Provides
                    private HttpRemoteTaskFactory createHttpRemoteTaskFactory(
                            JsonMapper jsonMapper,
                            ThriftMapper thriftMapper,
                            JsonCodec<TaskStatus> taskStatusJsonCodec,
                            SmileCodec<TaskStatus> taskStatusSmileCodec,
                            ThriftCodec<TaskStatus> taskStatusThriftCodec,
                            JsonCodec<TaskInfo> taskInfoJsonCodec,
                            ThriftCodec<TaskInfo> taskInfoThriftCodec,
                            SmileCodec<TaskInfo> taskInfoSmileCodec,
                            JsonCodec<TaskUpdateRequest> taskUpdateRequestJsonCodec,
                            SmileCodec<TaskUpdateRequest> taskUpdateRequestSmileCodec,
                            ThriftCodec<TaskUpdateRequest> taskUpdateRequestThriftCodec,
                            JsonCodec<PlanFragment> planFragmentJsonCodec,
                            SmileCodec<PlanFragment> planFragmentSmileCodec)
                    {
                        JaxrsTestingHttpProcessor jaxrsTestingHttpProcessor = new JaxrsTestingHttpProcessor(URI.create("http://fake.invalid/"), testingTaskResource, jsonMapper, thriftMapper);
                        TestingHttpClient testingHttpClient = new TestingHttpClient(jaxrsTestingHttpProcessor.setTrace(TRACE_HTTP));
                        testingTaskResource.setHttpClient(testingHttpClient);
                        return new HttpRemoteTaskFactory(
                                new QueryManagerConfig(),
                                TASK_MANAGER_CONFIG,
                                testingHttpClient,
                                new TestSqlTaskManager.MockLocationFactory(),
                                taskStatusJsonCodec,
                                taskStatusSmileCodec,
                                taskStatusThriftCodec,
                                taskInfoJsonCodec,
                                taskInfoSmileCodec,
                                taskInfoThriftCodec,
                                taskUpdateRequestJsonCodec,
                                taskUpdateRequestSmileCodec,
                                taskUpdateRequestThriftCodec,
                                planFragmentJsonCodec,
                                planFragmentSmileCodec,
                                new RemoteTaskStats(),
                                internalCommunicationConfig,
                                createTestMetadataManager(),
                                new TestQueryManager(),
                                new HandleResolver());
                    }
                });
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        handleResolver.addConnectorName("test", new TestingHandleResolver());
        return injector.getInstance(HttpRemoteTaskFactory.class);
    }

    private static void poll(BooleanSupplier success)
            throws InterruptedException
    {
        long failAt = System.nanoTime() + FAIL_TIMEOUT.roundTo(NANOSECONDS);

        while (!success.getAsBoolean()) {
            long millisUntilFail = (failAt - System.nanoTime()) / 1_000_000;
            if (millisUntilFail <= 0) {
                throw new AssertionError(format("Timeout of %s reached", FAIL_TIMEOUT));
            }
            Thread.sleep(min(POLL_TIMEOUT.toMillis(), millisUntilFail));
        }
    }

    private static void waitUntilTaskFinish(RemoteTask task)
            throws Exception
    {
        SettableFuture<?> taskFinished = SettableFuture.create();

        task.addStateChangeListener(status -> {
            if (status.getState().isDone()) {
                taskFinished.set(null);
            }
        });
        taskFinished.get();
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

        private AtomicReference<TestingHttpClient> httpClient = new AtomicReference<>();

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

        public synchronized TaskSource getTaskSource(PlanNodeId planNodeId)
        {
            TaskSource source = taskSourceMap.get(planNodeId);
            if (source == null) {
                return null;
            }
            return new TaskSource(source.getPlanNodeId(), source.getSplits(), source.getNoMoreSplitsForLifespan(), source.isNoMoreSplits());
        }

        @GET
        @Path("{taskId}/status")
        @Produces({MediaType.APPLICATION_JSON, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
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
                    initialTaskInfo.getLastHeartbeatInMillis(),
                    initialTaskInfo.getOutputBuffers(),
                    initialTaskInfo.getNoMoreSplits(),
                    initialTaskInfo.getStats(),
                    initialTaskInfo.isNeedsPlan(),
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
