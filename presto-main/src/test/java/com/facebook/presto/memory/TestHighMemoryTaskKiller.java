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
package com.facebook.presto.memory;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TestingGcMonitor;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.execution.SqlTask;
import com.facebook.presto.execution.SqlTaskExecutionFactory;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.execution.TestSqlTaskManager;
import com.facebook.presto.execution.buffer.SpoolingOutputBufferFactory;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskMemoryReservationSummary;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.google.common.base.Functions;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.SqlTask.createSqlTask;
import static com.facebook.presto.execution.TaskManagerConfig.TaskPriorityTracking.TASK_FAIR;
import static com.facebook.presto.execution.TaskTestUtils.PLAN_FRAGMENT;
import static com.facebook.presto.execution.TaskTestUtils.createTestSplitMonitor;
import static com.facebook.presto.execution.TaskTestUtils.createTestingPlanner;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableListMultimap.toImmutableListMultimap;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHighMemoryTaskKiller
{
    private final AtomicInteger nextTaskId = new AtomicInteger();

    private final TaskExecutor taskExecutor;
    private final ScheduledExecutorService taskNotificationExecutor;
    private final ScheduledExecutorService driverYieldExecutor;

    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;

    public TestHighMemoryTaskKiller()
    {
        taskExecutor = new TaskExecutor(8, 16, 3, 4, TASK_FAIR, Ticker.systemTicker());
        taskExecutor.start();

        taskNotificationExecutor = newScheduledThreadPool(10, threadsNamed("task-notification-%s"));
        driverYieldExecutor = newScheduledThreadPool(2, threadsNamed("driver-yield-%s"));

        LocalExecutionPlanner planner = createTestingPlanner();

        sqlTaskExecutionFactory = new SqlTaskExecutionFactory(
                taskNotificationExecutor,
                taskExecutor,
                planner,
                new BlockEncodingManager(),
                new OrderingCompiler(),
                createTestSplitMonitor(),
                new TaskManagerConfig());
    }

    @Test
    public void testMaxMemoryConsumingQuery()
            throws Exception
    {
        QueryId highMemoryQueryId = new QueryId("query1");
        SqlTask highMemoryTask = createInitialTask(highMemoryQueryId);
        updateTaskMemory(highMemoryTask, 200);

        QueryId lowMemoryQueryId = new QueryId("query2");
        SqlTask lowMemoryTask = createInitialTask(lowMemoryQueryId);
        updateTaskMemory(lowMemoryTask, 100);

        List<SqlTask> activeTasks = ImmutableList.of(highMemoryTask, lowMemoryTask);

        ListMultimap<QueryId, SqlTask> activeQueriesToTasksMap = activeTasks.stream()
                .collect(toImmutableListMultimap(task -> task.getQueryContext().getQueryId(), Function.identity()));

        Optional<QueryId> optionalQueryId = HighMemoryTaskKiller.getMaxMemoryConsumingQuery(activeQueriesToTasksMap);

        assertTrue(optionalQueryId.isPresent());
        assertEquals(optionalQueryId.get(), highMemoryQueryId);
    }

    public void updateTaskMemory(SqlTask sqlTask, long systemMemory)
    {
        TaskInfo taskInfo = sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                createInitialEmptyOutputBuffers(PARTITIONED)
                        .withNoMoreBufferIds(),
                Optional.of(new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty())));
        assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

        TaskContext taskContext = sqlTask.getTaskContext().get();
        PipelineContext pipelineContext = taskContext.addPipelineContext(0, true, true, false);

        pipelineContext.localSystemMemoryContext().setBytes(systemMemory);
    }

    public SqlTask createInitialTask(QueryId queryId)
    {
        TaskId taskId = new TaskId(queryId.getId(), 0, 0, nextTaskId.incrementAndGet(), 0);
        URI location = URI.create("fake://task/" + taskId);

        QueryContext queryContext = new QueryContext(queryId,
                new DataSize(1, MEGABYTE),
                new DataSize(2, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, GIGABYTE),
                new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE)),
                new TestingGcMonitor(),
                taskNotificationExecutor,
                driverYieldExecutor,
                new DataSize(1, MEGABYTE),
                new SpillSpaceTracker(new DataSize(1, GIGABYTE)),
                listJsonCodec(TaskMemoryReservationSummary.class));

        TaskContext taskContext = queryContext.addTaskContext(
                                    new TaskStateMachine(taskId, taskNotificationExecutor),
                                    testSessionBuilder().build(),
                                    Optional.of(PLAN_FRAGMENT.getRoot()),
                                    false,
                                    false,
                                    false,
                                    false,
                                    false);

        return createSqlTask(
                taskId,
                location,
                "fake",
                queryContext,
                sqlTaskExecutionFactory,
                new TestSqlTaskManager.MockExchangeClientSupplier(),
                taskNotificationExecutor,
                Functions.identity(),
                new DataSize(32, MEGABYTE),
                new CounterStat(),
                new SpoolingOutputBufferFactory(new FeaturesConfig()));
    }
}
