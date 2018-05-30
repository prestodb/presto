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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.SYSTEM_POOL;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestQueryContext
{
    private final ScheduledExecutorService taskNotificationExecutor = newScheduledThreadPool(1, threadsNamed("task-notification-%s"));

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        taskNotificationExecutor.shutdownNow();
    }

    @DataProvider
    public Object[][] testSetMemoryPoolOptions()
    {
        return new Object[][] {
                {false},
                {true},
        };
    }

    @Test(dataProvider = "testSetMemoryPoolOptions")
    public void testSetMemoryPool(boolean useReservedPool)
    {
        QueryId secondQuery = new QueryId("second");
        MemoryPool reservedPool = new MemoryPool(RESERVED_POOL, new DataSize(10, BYTE));
        long secondQueryMemory = reservedPool.getMaxBytes() - 1;
        if (useReservedPool) {
            assertTrue(reservedPool.reserve(secondQuery, secondQueryMemory).isDone());
        }

        try (LocalQueryRunner localQueryRunner = new LocalQueryRunner(TEST_SESSION)) {
            DefaultQueryContext queryContext = new DefaultQueryContext(
                    new QueryId("query"),
                    new DataSize(10, BYTE),
                    new DataSize(20, BYTE),
                    new MemoryPool(GENERAL_POOL, new DataSize(10, BYTE)),
                    new TestingGcMonitor(),
                    localQueryRunner.getExecutor(),
                    localQueryRunner.getScheduler(),
                    new DataSize(0, BYTE),
                    new SpillSpaceTracker(new DataSize(0, BYTE)));

            // Use memory
            LocalMemoryContext userMemoryContext = queryContext.getQueryMemoryContext().localUserMemoryContext();
            LocalMemoryContext revocableMemoryContext = queryContext.getQueryMemoryContext().localRevocableMemoryContext();
            assertTrue(userMemoryContext.setBytes(3).isDone());
            assertTrue(revocableMemoryContext.setBytes(5).isDone());

            queryContext.setMemoryPool(reservedPool);

            if (useReservedPool) {
                reservedPool.free(secondQuery, secondQueryMemory);
            }

            // Free memory
            userMemoryContext.close();
            revocableMemoryContext.close();
        }
    }

    @Test
    public void testLegacyQueryContext()
    {
        MemoryPool generalPool = new MemoryPool(GENERAL_POOL, new DataSize(10_000, BYTE));
        MemoryPool systemPool = new MemoryPool(SYSTEM_POOL, new DataSize(10_000, BYTE));
        try (LocalQueryRunner localQueryRunner = new LocalQueryRunner(TEST_SESSION)) {
            LegacyQueryContext queryContext = new LegacyQueryContext(
                    new QueryId("query"),
                    new DataSize(10, BYTE),
                    generalPool,
                    systemPool,
                    new TestingGcMonitor(),
                    localQueryRunner.getExecutor(),
                    localQueryRunner.getScheduler(),
                    new DataSize(0, BYTE),
                    new SpillSpaceTracker(new DataSize(0, BYTE)));
            TaskStateMachine taskStateMachine = new TaskStateMachine(TaskId.valueOf("task-id"), taskNotificationExecutor);
            TaskContext taskContext = queryContext.addTaskContext(taskStateMachine, TEST_SESSION, false, false);
            LocalMemoryContext systemContext = taskContext.localSystemMemoryContext();
            ListenableFuture<?> blocked = systemContext.setBytes(10_000);

            // even if the system pool is full, we don't block system allocations for LegacyQueryContext
            assertTrue(blocked.isDone());
            assertEquals(systemPool.getReservedBytes(), 10_000);
            assertEquals(generalPool.getReservedBytes(), 0);

            systemContext.close();

            assertEquals(systemPool.getReservedBytes(), 0);
            assertEquals(generalPool.getReservedBytes(), 0);
        }
    }
}
