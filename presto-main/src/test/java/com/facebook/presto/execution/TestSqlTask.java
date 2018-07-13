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
package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.facebook.presto.TaskSource;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.memory.DefaultQueryContext;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.context.MemoryTrackingContext;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.TaskTestUtils.EMPTY_SOURCES;
import static com.facebook.presto.execution.TaskTestUtils.PLAN_FRAGMENT;
import static com.facebook.presto.execution.TaskTestUtils.SPLIT;
import static com.facebook.presto.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static com.facebook.presto.execution.TaskTestUtils.createTestQueryMonitor;
import static com.facebook.presto.execution.TaskTestUtils.createTestingPlanner;
import static com.facebook.presto.execution.TaskTestUtils.updateTask;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestSqlTask
{
    public static final OutputBufferId OUT = new OutputBufferId(0);
    private final TaskExecutor taskExecutor;
    private final ScheduledExecutorService taskNotificationExecutor;
    private final ScheduledExecutorService driverYieldExecutor;
    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;

    private final AtomicInteger nextTaskId = new AtomicInteger();

    public TestSqlTask()
    {
        taskExecutor = new TaskExecutor(8, 16, 3, 4, Ticker.systemTicker());
        taskExecutor.start();

        taskNotificationExecutor = newScheduledThreadPool(10, threadsNamed("task-notification-%s"));
        driverYieldExecutor = newScheduledThreadPool(2, threadsNamed("driver-yield-%s"));

        LocalExecutionPlanner planner = createTestingPlanner();

        sqlTaskExecutionFactory = new SqlTaskExecutionFactory(
                taskNotificationExecutor,
                taskExecutor,
                planner,
                createTestQueryMonitor(),
                new TaskManagerConfig());
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        taskExecutor.stop();
        taskNotificationExecutor.shutdownNow();
        driverYieldExecutor.shutdown();
    }

    @Test
    public void testEmptyQuery()
    {
        SqlTask sqlTask = createInitialTask(Optional.empty());

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withNoMoreBufferIds(),
                OptionalInt.empty());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withNoMoreBufferIds(),
                OptionalInt.empty());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
    }

    @Test
    public void testSimpleQuery()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask(Optional.empty());

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                OptionalInt.empty());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        BufferResult results = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE)).get();
        assertEquals(results.isBufferComplete(), false);
        assertEquals(results.getSerializedPages().size(), 1);
        assertEquals(results.getSerializedPages().get(0).getPositionCount(), 1);

        for (boolean moreResults = true; moreResults; moreResults = !results.isBufferComplete()) {
            results = sqlTask.getTaskResults(OUT, results.getToken() + results.getSerializedPages().size(), new DataSize(1, MEGABYTE)).get();
        }
        assertEquals(results.getSerializedPages().size(), 0);

        // complete the task by calling abort on it
        TaskInfo info = sqlTask.abortTaskResults(OUT);
        assertEquals(info.getOutputBuffers().getState(), BufferState.FINISHED);

        taskInfo = sqlTask.getTaskInfo(taskInfo.getTaskStatus().getState()).get(1, SECONDS);
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
    }

    @Test
    public void testCancel()
    {
        SqlTask sqlTask = createInitialTask(Optional.empty());

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(OUT, 0)
                        .withNoMoreBufferIds(),
                OptionalInt.empty());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
        assertNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
        assertNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTask.cancel();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);
        assertNotNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);
        assertNotNull(taskInfo.getStats().getEndTime());
    }

    @Test
    public void testTaskMemoryReleasedOnCompletion()
            throws InterruptedException, ExecutionException, TimeoutException
    {
        // waitUntilDone future gets completed when the task is done
        SettableFuture waitUntilDone = SettableFuture.create();
        SqlTask sqlTask = createInitialTask(Optional.of((input) -> waitUntilDone.set(null)));
        sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withBuffer(OUT, 0)
                        .withNoMoreBufferIds(),
                OptionalInt.empty());

        TaskContext taskContext = sqlTask.getQueryContext().getTaskContextByTaskId(sqlTask.getTaskId());
        PipelineContext pipelineContext1 = taskContext.addPipelineContext(0, false, false);
        PipelineContext pipelineContext2 = taskContext.addPipelineContext(1, false, false);

        pipelineContext1.localSystemMemoryContext().setBytes(100);
        pipelineContext2.localSystemMemoryContext().setBytes(100);
        taskContext.localSystemMemoryContext().setBytes(100);

        MemoryTrackingContext memoryTrackingContext = taskContext.getTaskMemoryContext();
        assertEquals(memoryTrackingContext.getSystemMemory(), 300);

        // canceling the task will destroy the TaskContext, which will destroy
        // the local/aggregate memory contexts tied to it including the PipelineContexts
        sqlTask.cancel();
        waitUntilDone.get(30, SECONDS);
        assertEquals(memoryTrackingContext.getSystemMemory(), 0);
    }

    @Test
    public void testAbort()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask(Optional.empty());

        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), true)),
                createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                OptionalInt.empty());
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        sqlTask.abortTaskResults(OUT);

        taskInfo = sqlTask.getTaskInfo(taskInfo.getTaskStatus().getState()).get(1, SECONDS);
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

        taskInfo = sqlTask.getTaskInfo();
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
    }

    @Test
    public void testBufferCloseOnFinish()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask(Optional.empty());

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds();
        updateTask(sqlTask, EMPTY_SOURCES, outputBuffers);

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        // close the sources (no splits will ever be added)
        updateTask(sqlTask, ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(), true)), outputBuffers);

        // finish the task by calling abort on it
        sqlTask.abortTaskResults(OUT);

        // buffer will be closed by cancel event (wait for event to fire)
        bufferResult.get(1, SECONDS);

        // verify the buffer is closed
        bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertTrue(bufferResult.isDone());
        assertTrue(bufferResult.get().isBufferComplete());
    }

    @Test
    public void testBufferCloseOnCancel()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask(Optional.empty());

        updateTask(sqlTask, EMPTY_SOURCES, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        sqlTask.cancel();
        assertEquals(sqlTask.getTaskInfo().getTaskStatus().getState(), TaskState.CANCELED);

        // buffer future will complete.. the event is async so wait a bit for event to propagate
        bufferResult.get(1, SECONDS);

        bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertTrue(bufferResult.isDone());
        assertTrue(bufferResult.get().isBufferComplete());
    }

    @Test
    public void testBufferNotCloseOnFail()
            throws Exception
    {
        SqlTask sqlTask = createInitialTask(Optional.empty());

        updateTask(sqlTask, EMPTY_SOURCES, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());

        ListenableFuture<BufferResult> bufferResult = sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE));
        assertFalse(bufferResult.isDone());

        TaskState taskState = sqlTask.getTaskInfo().getTaskStatus().getState();
        sqlTask.failed(new Exception("test"));
        assertEquals(sqlTask.getTaskInfo(taskState).get(1, SECONDS).getTaskStatus().getState(), TaskState.FAILED);

        // buffer will not be closed by fail event.  event is async so wait a bit for event to fire
        try {
            assertTrue(bufferResult.get(1, SECONDS).isBufferComplete());
            fail("expected TimeoutException");
        }
        catch (TimeoutException expected) {
            // expected
        }
        assertFalse(sqlTask.getTaskResults(OUT, 0, new DataSize(1, MEGABYTE)).isDone());
    }

    public SqlTask createInitialTask(Optional<Function<SqlTask, ?>> onDone)
    {
        TaskId taskId = new TaskId("query", 0, nextTaskId.incrementAndGet());
        URI location = URI.create("fake://task/" + taskId);

        return new SqlTask(
                taskId,
                location,
                "fake",
                new DefaultQueryContext(new QueryId("query"),
                        new DataSize(1, MEGABYTE),
                        new DataSize(2, MEGABYTE),
                        new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE)),
                        new TestingGcMonitor(),
                        taskNotificationExecutor,
                        driverYieldExecutor,
                        new DataSize(1, MEGABYTE),
                        new SpillSpaceTracker(new DataSize(1, GIGABYTE))),
                sqlTaskExecutionFactory,
                taskNotificationExecutor,
                onDone.orElseGet(() -> Functions.identity()),
                new DataSize(32, MEGABYTE));
    }
}
