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
package com.facebook.presto.operator;

import com.facebook.airlift.stats.TestingGcMonitor;
import com.facebook.presto.Session;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.google.common.util.concurrent.MoreExecutors;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.airlift.units.DataSize.succinctBytes;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestTaskContextRuntimeStats
{
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testTaskStatsIncludesCreateTimeAndEndTime()
    {
        Session session = testSessionBuilder().build();
        QueryContext queryContext = createQueryContext(session);

        TaskStateMachine taskStateMachine = new TaskStateMachine(
                new TaskId(new StageExecutionId(new StageId(new QueryId("test_query"), 0), 0), 0, 0),
                MoreExecutors.directExecutor());

        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                Optional.empty(),
                false,
                false,
                false,
                false,
                false);

        long createTimeBeforeStats = taskStateMachine.getCreatedTimeInMillis();

        // Get task stats
        TaskStats taskStats = taskContext.getTaskStats();

        // Verify RuntimeStats contains createTime
        RuntimeStats runtimeStats = taskStats.getRuntimeStats();
        assertNotNull(runtimeStats, "RuntimeStats should not be null");
        assertTrue(runtimeStats.getMetrics().containsKey("createTime"), "RuntimeStats should contain createTime metric");

        // Verify createTime value is reasonable
        long createTimeFromStats = (long) runtimeStats.getMetric("createTime").getSum();
        assertEquals(createTimeFromStats, createTimeBeforeStats, "createTime should match task creation time");

        // Mark task as finished to trigger endTime
        taskStateMachine.finished();
        TaskStats finalTaskStats = taskContext.getTaskStats();
        RuntimeStats finalRuntimeStats = finalTaskStats.getRuntimeStats();

        // Verify endTime is now present
        assertTrue(finalRuntimeStats.getMetrics().containsKey("endTime"), "RuntimeStats should contain endTime metric after task finishes");
        long endTimeFromStats = (long) finalRuntimeStats.getMetric("endTime").getSum();
        assertTrue(endTimeFromStats > 0, "endTime should be greater than 0");
        assertTrue(endTimeFromStats >= createTimeFromStats, "endTime should be >= createTime");
    }

    @Test
    public void testTaskStatsRuntimeStatsNotNullBeforeTaskFinish()
    {
        Session session = testSessionBuilder().build();
        QueryContext queryContext = createQueryContext(session);

        TaskStateMachine taskStateMachine = new TaskStateMachine(
                new TaskId(new StageExecutionId(new StageId(new QueryId("test_query_2"), 0), 0), 0, 0),
                MoreExecutors.directExecutor());

        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                Optional.empty(),
                false,
                false,
                false,
                false,
                false);

        // Get stats before task finishes
        TaskStats taskStats = taskContext.getTaskStats();
        RuntimeStats runtimeStats = taskStats.getRuntimeStats();

        // Verify RuntimeStats is not null and contains createTime even before task finishes
        assertNotNull(runtimeStats, "RuntimeStats should not be null");
        assertTrue(runtimeStats.getMetrics().containsKey("createTime"), "RuntimeStats should contain createTime even before task finishes");

        // endTime should not be present yet (or be 0)
        if (runtimeStats.getMetrics().containsKey("endTime")) {
            long endTime = (long) runtimeStats.getMetric("endTime").getSum();
            assertEquals(endTime, 0L, "endTime should be 0 before task finishes");
        }
    }

    private QueryContext createQueryContext(Session session)
    {
        return new QueryContext(
                session.getQueryId(),
                succinctBytes(1 * 1024 * 1024),
                succinctBytes(1 * 1024 * 1024 * 1024),
                succinctBytes(1 * 1024 * 1024 * 1024),
                succinctBytes(1 * 1024 * 1024 * 1024),
                new TestingMemoryPool(succinctBytes(1 * 1024 * 1024 * 1024)),
                new TestingGcMonitor(),
                MoreExecutors.directExecutor(),
                scheduledExecutor,
                succinctBytes(1 * 1024 * 1024 * 1024),
                new SpillSpaceTracker(succinctBytes(1 * 1024 * 1024 * 1024)),
                listJsonCodec(TaskMemoryReservationSummary.class));
    }

    private static class TestingMemoryPool
            extends com.facebook.presto.memory.MemoryPool
    {
        public TestingMemoryPool(com.facebook.airlift.units.DataSize maxMemory)
        {
            super(new MemoryPoolId("test"), maxMemory);
        }
    }
}
