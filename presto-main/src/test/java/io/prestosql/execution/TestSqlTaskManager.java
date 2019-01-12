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
package io.prestosql.execution;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.node.NodeInfo;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.prestosql.OutputBuffers;
import io.prestosql.OutputBuffers.OutputBufferId;
import io.prestosql.ScheduledSplit;
import io.prestosql.TaskSource;
import io.prestosql.execution.buffer.BufferResult;
import io.prestosql.execution.buffer.BufferState;
import io.prestosql.execution.executor.TaskExecutor;
import io.prestosql.memory.LocalMemoryManager;
import io.prestosql.memory.NodeMemoryConfig;
import io.prestosql.memory.ReservedSystemMemoryConfig;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.ExchangeClient;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.spi.Node;
import io.prestosql.spi.QueryId;
import io.prestosql.spiller.LocalSpillManager;
import io.prestosql.spiller.NodeSpillConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.OutputBuffers.BufferType.PARTITIONED;
import static io.prestosql.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.TaskTestUtils.PLAN_FRAGMENT;
import static io.prestosql.execution.TaskTestUtils.SPLIT;
import static io.prestosql.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static io.prestosql.execution.TaskTestUtils.createTestSplitMonitor;
import static io.prestosql.execution.TaskTestUtils.createTestingPlanner;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test
public class TestSqlTaskManager
{
    private static final TaskId TASK_ID = new TaskId("query", 0, 1);
    public static final OutputBufferId OUT = new OutputBufferId(0);

    private final TaskExecutor taskExecutor;
    private final TaskManagementExecutor taskManagementExecutor;
    private final LocalMemoryManager localMemoryManager;
    private final LocalSpillManager localSpillManager;

    public TestSqlTaskManager()
    {
        localMemoryManager = new LocalMemoryManager(new NodeMemoryConfig(), new ReservedSystemMemoryConfig());
        localSpillManager = new LocalSpillManager(new NodeSpillConfig());
        taskExecutor = new TaskExecutor(8, 16, 3, 4, Ticker.systemTicker());
        taskExecutor.start();
        taskManagementExecutor = new TaskManagementExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        taskExecutor.stop();
        taskManagementExecutor.close();
    }

    @Test
    public void testEmptyQuery()
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, createInitialEmptyOutputBuffers(PARTITIONED).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = createTask(sqlTaskManager, taskId, ImmutableSet.of(), createInitialEmptyOutputBuffers(PARTITIONED).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        }
    }

    @Test
    public void testSimpleQuery()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, ImmutableSet.of(SPLIT), createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            BufferResult results = sqlTaskManager.getTaskResults(taskId, OUT, 0, new DataSize(1, Unit.MEGABYTE)).get();
            assertEquals(results.isBufferComplete(), false);
            assertEquals(results.getSerializedPages().size(), 1);
            assertEquals(results.getSerializedPages().get(0).getPositionCount(), 1);

            for (boolean moreResults = true; moreResults; moreResults = !results.isBufferComplete()) {
                results = sqlTaskManager.getTaskResults(taskId, OUT, results.getToken() + results.getSerializedPages().size(), new DataSize(1, Unit.MEGABYTE)).get();
            }
            assertEquals(results.isBufferComplete(), true);
            assertEquals(results.getSerializedPages().size(), 0);

            // complete the task by calling abort on it
            TaskInfo info = sqlTaskManager.abortTaskResults(taskId, OUT);
            assertEquals(info.getOutputBuffers().getState(), BufferState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId, taskInfo.getTaskStatus().getState()).get(1, TimeUnit.SECONDS);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        }
    }

    @Test
    public void testCancel()
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.cancelTask(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);
            assertNotNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);
            assertNotNull(taskInfo.getStats().getEndTime());
        }
    }

    @Test
    public void testAbort()
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);
            assertNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.abortTask(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.ABORTED);
            assertNotNull(taskInfo.getStats().getEndTime());

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.ABORTED);
            assertNotNull(taskInfo.getStats().getEndTime());
        }
    }

    @Test
    public void testAbortResults()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig())) {
            TaskId taskId = TASK_ID;
            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, ImmutableSet.of(SPLIT), createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            sqlTaskManager.abortTaskResults(taskId, OUT);

            taskInfo = sqlTaskManager.getTaskInfo(taskId, taskInfo.getTaskStatus().getState()).get(1, TimeUnit.SECONDS);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.FINISHED);
        }
    }

    @Test
    public void testRemoveOldTasks()
            throws Exception
    {
        try (SqlTaskManager sqlTaskManager = createSqlTaskManager(new TaskManagerConfig().setInfoMaxAge(new Duration(5, TimeUnit.MILLISECONDS)))) {
            TaskId taskId = TASK_ID;

            TaskInfo taskInfo = createTask(sqlTaskManager, taskId, createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.RUNNING);

            taskInfo = sqlTaskManager.cancelTask(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);

            taskInfo = sqlTaskManager.getTaskInfo(taskId);
            assertEquals(taskInfo.getTaskStatus().getState(), TaskState.CANCELED);

            Thread.sleep(100);
            sqlTaskManager.removeOldTasks();

            for (TaskInfo info : sqlTaskManager.getAllTaskInfo()) {
                assertNotEquals(info.getTaskStatus().getTaskId(), taskId);
            }
        }
    }

    public SqlTaskManager createSqlTaskManager(TaskManagerConfig config)
    {
        return new SqlTaskManager(
                createTestingPlanner(),
                new MockLocationFactory(),
                taskExecutor,
                createTestSplitMonitor(),
                new NodeInfo("test"),
                localMemoryManager,
                taskManagementExecutor,
                config,
                new NodeMemoryConfig(),
                localSpillManager,
                new NodeSpillConfig(),
                new TestingGcMonitor());
    }

    private TaskInfo createTask(SqlTaskManager sqlTaskManager, TaskId taskId, ImmutableSet<ScheduledSplit> splits, OutputBuffers outputBuffers)
    {
        return sqlTaskManager.updateTask(TEST_SESSION,
                taskId,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, splits, true)),
                outputBuffers,
                OptionalInt.empty());
    }

    private TaskInfo createTask(SqlTaskManager sqlTaskManager, TaskId taskId, OutputBuffers outputBuffers)
    {
        sqlTaskManager.getQueryContext(taskId.getQueryId())
                .addTaskContext(new TaskStateMachine(taskId, directExecutor()), testSessionBuilder().build(), false, false, OptionalInt.empty());
        return sqlTaskManager.updateTask(TEST_SESSION,
                taskId,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(),
                outputBuffers,
                OptionalInt.empty());
    }

    public static class MockExchangeClientSupplier
            implements ExchangeClientSupplier
    {
        @Override
        public ExchangeClient get(LocalMemoryContext systemMemoryContext)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class MockLocationFactory
            implements LocationFactory
    {
        @Override
        public URI createQueryLocation(QueryId queryId)
        {
            return URI.create("http://fake.invalid/query/" + queryId);
        }

        @Override
        public URI createStageLocation(StageId stageId)
        {
            return URI.create("http://fake.invalid/stage/" + stageId);
        }

        @Override
        public URI createLocalTaskLocation(TaskId taskId)
        {
            return URI.create("http://fake.invalid/task/" + taskId);
        }

        @Override
        public URI createTaskLocation(Node node, TaskId taskId)
        {
            return URI.create("http://fake.invalid/task/" + node.getNodeIdentifier() + "/" + taskId);
        }

        @Override
        public URI createMemoryInfoLocation(Node node)
        {
            return URI.create("http://fake.invalid/" + node.getNodeIdentifier() + "/memory");
        }
    }
}
