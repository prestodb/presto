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

import com.facebook.airlift.stats.TestingGcMonitor;
import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskMemoryReservationSummary;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.TaskTestUtils.PLAN_FRAGMENT;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.RESERVED_POOL;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestQueryContext
{
    private static final ScheduledExecutorService TEST_EXECUTOR = newScheduledThreadPool(1, threadsNamed("test-executor-%s"));

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        TEST_EXECUTOR.shutdownNow();
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
            assertTrue(reservedPool.reserve(secondQuery, "test", secondQueryMemory).isDone());
        }

        try (LocalQueryRunner localQueryRunner = new LocalQueryRunner(TEST_SESSION)) {
            QueryContext queryContext = new QueryContext(
                    new QueryId("query"),
                    new DataSize(10, BYTE),
                    new DataSize(20, BYTE),
                    new DataSize(10, BYTE),
                    new DataSize(1, GIGABYTE),
                    new MemoryPool(GENERAL_POOL, new DataSize(10, BYTE)),
                    new TestingGcMonitor(),
                    localQueryRunner.getExecutor(),
                    localQueryRunner.getScheduler(),
                    new DataSize(0, BYTE),
                    new SpillSpaceTracker(new DataSize(0, BYTE)),
                    listJsonCodec(TaskMemoryReservationSummary.class));

            // Use memory
            queryContext.getQueryMemoryContext().initializeLocalMemoryContexts("test");
            LocalMemoryContext userMemoryContext = queryContext.getQueryMemoryContext().localUserMemoryContext();
            LocalMemoryContext revocableMemoryContext = queryContext.getQueryMemoryContext().localRevocableMemoryContext();
            assertTrue(userMemoryContext.setBytes(3).isDone());
            assertTrue(revocableMemoryContext.setBytes(5).isDone());

            queryContext.setMemoryPool(reservedPool);

            if (useReservedPool) {
                reservedPool.free(secondQuery, "test", secondQueryMemory);
            }

            // Free memory
            userMemoryContext.close();
            revocableMemoryContext.close();
        }
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = ".*Query exceeded per-node total memory limit of 20B.*")
    public void testChecksTotalMemoryOnUserMemoryAllocation()
    {
        try (LocalQueryRunner localQueryRunner = new LocalQueryRunner(TEST_SESSION)) {
            QueryContext queryContext = new QueryContext(
                    new QueryId("query"),
                    new DataSize(10, BYTE), // user memory limit
                    new DataSize(20, BYTE), // total memory limit
                    new DataSize(10, BYTE),
                    new DataSize(1, GIGABYTE),
                    new MemoryPool(GENERAL_POOL, new DataSize(10, BYTE)),
                    new TestingGcMonitor(),
                    localQueryRunner.getExecutor(),
                    localQueryRunner.getScheduler(),
                    new DataSize(0, BYTE),
                    new SpillSpaceTracker(new DataSize(0, BYTE)),
                    listJsonCodec(TaskMemoryReservationSummary.class));

            queryContext.getQueryMemoryContext().initializeLocalMemoryContexts("test");
            LocalMemoryContext systemMemoryContext = queryContext.getQueryMemoryContext().localSystemMemoryContext();
            LocalMemoryContext userMemoryContext = queryContext.getQueryMemoryContext().localUserMemoryContext();
            systemMemoryContext.setBytes(15);
            userMemoryContext.setBytes(6);
        }
    }

    @Test
    public void testMoveTaggedAllocations()
    {
        MemoryPool generalPool = new MemoryPool(GENERAL_POOL, new DataSize(10_000, BYTE));
        MemoryPool reservedPool = new MemoryPool(RESERVED_POOL, new DataSize(10_000, BYTE));
        QueryId queryId = new QueryId("query");
        QueryContext queryContext = createQueryContext(queryId, generalPool);
        TaskStateMachine taskStateMachine = new TaskStateMachine(TaskId.valueOf("queryid.0.0.0"), TEST_EXECUTOR);
        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                TEST_SESSION,
                Optional.of(PLAN_FRAGMENT.getRoot()),
                false,
                false,
                false,
                false,
                false);
        DriverContext driverContext = taskContext.addPipelineContext(0, false, false, false).addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), "test");

        // allocate some memory in the general pool
        LocalMemoryContext memoryContext = operatorContext.aggregateUserMemoryContext().newLocalMemoryContext("test_context");
        memoryContext.setBytes(1_000);

        Map<String, Long> allocations = generalPool.getTaggedMemoryAllocations(queryId);
        assertEquals(allocations, ImmutableMap.of("test_context", 1_000L));

        queryContext.setMemoryPool(reservedPool);

        assertNull(generalPool.getTaggedMemoryAllocations(queryId));
        allocations = reservedPool.getTaggedMemoryAllocations(queryId);
        assertEquals(allocations, ImmutableMap.of("test_context", 1_000L));

        assertEquals(generalPool.getFreeBytes(), 10_000);
        assertEquals(reservedPool.getFreeBytes(), 9_000);

        memoryContext.close();

        assertEquals(generalPool.getFreeBytes(), 10_000);
        assertEquals(reservedPool.getFreeBytes(), 10_000);
    }

    private static QueryContext createQueryContext(QueryId queryId, MemoryPool generalPool)
    {
        return new QueryContext(queryId,
                new DataSize(10_000, BYTE),
                new DataSize(10_000, BYTE),
                new DataSize(10_000, BYTE),
                new DataSize(1, GIGABYTE),
                generalPool,
                new TestingGcMonitor(),
                TEST_EXECUTOR,
                TEST_EXECUTOR,
                new DataSize(0, BYTE),
                new SpillSpaceTracker(new DataSize(0, BYTE)),
                listJsonCodec(TaskMemoryReservationSummary.class));
    }
}
