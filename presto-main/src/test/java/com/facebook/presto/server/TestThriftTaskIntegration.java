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
package com.facebook.presto.server;

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.drift.client.DriftClientFactory;
import com.facebook.drift.client.address.AddressSelector;
import com.facebook.drift.client.address.SimpleAddressSelector;
import com.facebook.drift.codec.ThriftCodecManager;
import com.facebook.drift.server.DriftServer;
import com.facebook.drift.transport.netty.client.DriftNettyClientConfig;
import com.facebook.drift.transport.netty.client.DriftNettyMethodInvokerFactory;
import com.facebook.drift.transport.netty.server.DriftNettyServerModule;
import com.facebook.drift.transport.netty.server.DriftNettyServerTransport;
import com.facebook.presto.Session;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.ThriftBufferResult;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.MemoryPoolAssignmentsRequest;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.server.thrift.ThriftTaskClient;
import com.facebook.presto.server.thrift.ThriftTaskService;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.inject.Singleton;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.drift.client.ExceptionClassifier.NORMAL_RESULT;
import static com.facebook.drift.server.guice.DriftServerBinder.driftServerBinder;
import static com.facebook.drift.transport.netty.client.DriftNettyMethodInvokerFactory.createStaticDriftNettyMethodInvokerFactory;
import static com.facebook.presto.execution.buffer.BufferResult.emptyResults;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestThriftTaskIntegration
{
    private LifeCycleManager lifeCycleManager;
    private int thriftServerPort;

    @BeforeClass
    public void setup()
            throws Exception
    {
        Bootstrap app = new Bootstrap(
                new DriftNettyServerModule(),
                new TestingTaskThriftModule());

        Injector injector = app
                .doNotInitializeLogging()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);
        thriftServerPort = driftServerPort(injector.getInstance(DriftServer.class));
    }

    @AfterClass
    public void teardown()
    {
        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
    }

    @Test
    public void testServer()
    {
        AddressSelector<SimpleAddressSelector.SimpleAddress> addressSelector = new SimpleAddressSelector(
                ImmutableSet.of(HostAndPort.fromParts("localhost", thriftServerPort)),
                true);
        try (DriftNettyMethodInvokerFactory<?> invokerFactory = createStaticDriftNettyMethodInvokerFactory(new DriftNettyClientConfig())) {
            DriftClientFactory clientFactory = new DriftClientFactory(new ThriftCodecManager(), invokerFactory, addressSelector, NORMAL_RESULT);
            ThriftTaskClient client = clientFactory.createDriftClient(ThriftTaskClient.class).get();

            // get buffer result
            ListenableFuture<ThriftBufferResult> result = client.getResults(TaskId.valueOf("queryid.0.0.0"), new OutputBufferId(1), 0, 100);
            assertTrue(result.get().isBufferComplete());
            assertTrue(result.get().getSerializedPages().isEmpty());
            assertEquals(result.get().getToken(), 1);
            assertEquals(result.get().getTaskInstanceId(), "test");

            // ack buffer result
            client.acknowledgeResults(TaskId.valueOf("queryid.0.0.0"), new OutputBufferId(1), 42).get();    // sync
            client.acknowledgeResults(TaskId.valueOf("queryid.0.0.0"), new OutputBufferId(1), 42);          // fire and forget

            // abort buffer result
            client.abortResults(TaskId.valueOf("queryid.0.0.0"), new OutputBufferId(1)).get();
        }
        catch (Exception e) {
            fail();
        }
    }

    private static int driftServerPort(DriftServer server)
    {
        return ((DriftNettyServerTransport) server.getServerTransport()).getPort();
    }

    public static class TestingTaskThriftModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(ThriftTaskService.class).in(Scopes.SINGLETON);

            driftServerBinder(binder).bindService(ThriftTaskService.class);
        }

        @Provides
        @Singleton
        @ForAsyncRpc
        public static ExecutorService createAsyncHttpResponseCoreExecutor()
        {
            return newCachedThreadPool(daemonThreadsNamed("async-http-response-%s"));
        }

        @Provides
        @Singleton
        @ForAsyncRpc
        public static BoundedExecutor createAsyncHttpResponseExecutor(@ForAsyncRpc ExecutorService coreExecutor)
        {
            return new BoundedExecutor(coreExecutor, 100);
        }

        @Provides
        @Singleton
        @ForAsyncRpc
        public static ScheduledExecutorService createAsyncHttpTimeoutExecutor()
        {
            return newScheduledThreadPool(10, daemonThreadsNamed("async-http-timeout-%s"));
        }

        @Provides
        @Singleton
        public static TaskManager createTaskManager()
        {
            return new TaskManager()
            {
                @Override
                public List<TaskInfo> getAllTaskInfo()
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public TaskInfo getTaskInfo(TaskId taskId)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public TaskStatus getTaskStatus(TaskId taskId)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, TaskState currentState)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public String getTaskInstanceId(TaskId taskId)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ListenableFuture<TaskStatus> getTaskStatus(TaskId taskId, TaskState currentState)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void updateMemoryPoolAssignments(MemoryPoolAssignmentsRequest assignments)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public TaskInfo updateTask(Session session, TaskId taskId, Optional<PlanFragment> fragment, List<TaskSource> sources, OutputBuffers outputBuffers, Optional<TableWriteInfo> tableWriteInfo)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public TaskInfo cancelTask(TaskId taskId)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public TaskInfo abortTask(TaskId taskId)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public ListenableFuture<BufferResult> getTaskResults(TaskId taskId, OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
                {
                    return Futures.immediateFuture(emptyResults("test", 1, true));
                }

                @Override
                public void acknowledgeTaskResults(TaskId taskId, OutputBufferId bufferId, long sequenceId)
                {
                    assertEquals(taskId, TaskId.valueOf("queryid.0.0.0"));
                    assertEquals(bufferId, new OutputBufferId(1));
                    assertEquals(sequenceId, 42);
                }

                @Override
                public TaskInfo abortTaskResults(TaskId taskId, OutputBufferId bufferId)
                {
                    assertEquals(taskId, TaskId.valueOf("queryid.0.0.0"));
                    assertEquals(bufferId, new OutputBufferId(1));

                    // null is not going to be consumed
                    return null;
                }

                @Override
                public void addStateChangeListener(TaskId taskId, StateMachine.StateChangeListener<TaskState> stateChangeListener)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void removeRemoteSource(TaskId taskId, TaskId remoteSourceTaskId)
                {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void updateMetadataResults(TaskId taskId, MetadataUpdates metadataUpdates)
                {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }
}
