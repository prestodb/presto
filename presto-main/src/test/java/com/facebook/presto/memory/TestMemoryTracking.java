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
import com.facebook.presto.memory.context.MemoryTrackingContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverStats;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskMemoryReservationSummary;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.execution.TaskTestUtils.PLAN_FRAGMENT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestMemoryTracking
{
    private static final DataSize queryMaxMemory = new DataSize(1, GIGABYTE);
    private static final DataSize queryMaxTotalMemory = new DataSize(1, GIGABYTE);
    private static final DataSize queryMaxRevocableMemory = new DataSize(2, GIGABYTE);
    private static final DataSize memoryPoolSize = new DataSize(1, GIGABYTE);
    private static final DataSize maxSpillSize = new DataSize(1, GIGABYTE);
    private static final DataSize queryMaxSpillSize = new DataSize(1, GIGABYTE);
    private static final SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(maxSpillSize);

    private QueryContext queryContext;
    private TaskContext taskContext;
    private PipelineContext pipelineContext;
    private DriverContext driverContext;
    private OperatorContext operatorContext;
    private MemoryPool memoryPool;
    private ExecutorService notificationExecutor;
    private ScheduledExecutorService yieldExecutor;

    @BeforeClass
    public void setUp()
    {
        notificationExecutor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-executor-%s"));
        yieldExecutor = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-scheduler-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        notificationExecutor.shutdownNow();
        yieldExecutor.shutdownNow();
        queryContext = null;
        taskContext = null;
        pipelineContext = null;
        driverContext = null;
        operatorContext = null;
        memoryPool = null;
    }

    @BeforeMethod
    public void setUpTest()
    {
        memoryPool = new MemoryPool(new MemoryPoolId("test"), memoryPoolSize);
        queryContext = new QueryContext(
                new QueryId("test_query"),
                queryMaxMemory,
                queryMaxTotalMemory,
                queryMaxMemory,
                queryMaxRevocableMemory,
                memoryPool,
                new TestingGcMonitor(),
                notificationExecutor,
                yieldExecutor,
                queryMaxSpillSize,
                spillSpaceTracker,
                listJsonCodec(TaskMemoryReservationSummary.class));
        taskContext = queryContext.addTaskContext(
                new TaskStateMachine(new TaskId("query", 0, 0, 0), notificationExecutor),
                testSessionBuilder().build(),
                Optional.of(PLAN_FRAGMENT.getRoot()),
                true,
                true,
                true,
                true,
                false);
        pipelineContext = taskContext.addPipelineContext(0, true, true, false);
        driverContext = pipelineContext.addDriverContext();
        operatorContext = driverContext.addOperatorContext(1, new PlanNodeId("a"), "test-operator");
    }

    @Test
    public void testOperatorAllocations()
    {
        MemoryTrackingContext operatorMemoryContext = operatorContext.getOperatorMemoryContext();
        LocalMemoryContext systemMemory = operatorContext.newLocalSystemMemoryContext("test");
        LocalMemoryContext userMemory = operatorContext.localUserMemoryContext();
        LocalMemoryContext revocableMemory = operatorContext.localRevocableMemoryContext();
        userMemory.setBytes(100);
        assertOperatorMemoryAllocations(operatorMemoryContext, 100, 0, 0);
        systemMemory.setBytes(1_000_000);
        assertOperatorMemoryAllocations(operatorMemoryContext, 100, 1_000_000, 0);
        systemMemory.setBytes(2_000_000);
        assertOperatorMemoryAllocations(operatorMemoryContext, 100, 2_000_000, 0);
        userMemory.setBytes(500);
        assertOperatorMemoryAllocations(operatorMemoryContext, 500, 2_000_000, 0);
        userMemory.setBytes(userMemory.getBytes() - 500);
        assertOperatorMemoryAllocations(operatorMemoryContext, 0, 2_000_000, 0);
        revocableMemory.setBytes(300);
        assertOperatorMemoryAllocations(operatorMemoryContext, 0, 2_000_000, 300);
        assertAllocationFails((ignored) -> userMemory.setBytes(userMemory.getBytes() - 500), "bytes cannot be negative");
        operatorContext.destroy();
        assertOperatorMemoryAllocations(operatorMemoryContext, 0, 0, 0);
    }

    @Test
    public void testLocalTotalMemoryLimitExceeded()
    {
        LocalMemoryContext systemMemoryContext = operatorContext.newLocalSystemMemoryContext("test");
        systemMemoryContext.setBytes(100);
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 0, 100, 0);
        systemMemoryContext.setBytes(queryMaxTotalMemory.toBytes());
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 0, queryMaxTotalMemory.toBytes(), 0);
        try {
            systemMemoryContext.setBytes(queryMaxTotalMemory.toBytes() + 1);
            fail("allocation should hit the per-node total memory limit");
        }
        catch (ExceededMemoryLimitException e) {
            assertEquals(e.getMessage(), format("Query exceeded per-node total memory limit of %1$s [Allocated: %1$s, Delta: 1B, Top Consumers: {test=%1$s}]", queryMaxTotalMemory));
        }
    }

    @Test
    public void testLocalRevocableMemoryLimitExceeded()
    {
        LocalMemoryContext revocableMemoryContext = operatorContext.localRevocableMemoryContext();
        revocableMemoryContext.setBytes(100);
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 0, 0, 100);
        revocableMemoryContext.setBytes(queryMaxRevocableMemory.toBytes());
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 0, 0, queryMaxRevocableMemory.toBytes());
        try {
            revocableMemoryContext.setBytes(queryMaxRevocableMemory.toBytes() + 1);
            fail("allocation should hit the per-node revocable memory limit");
        }
        catch (ExceededMemoryLimitException e) {
            assertEquals(e.getMessage(), format("Query exceeded per-node revocable memory limit of %1$s [Allocated: %1$s, Delta: 1B]", queryMaxRevocableMemory));
        }
    }

    @Test
    public void testLocalSystemAllocations()
    {
        long pipelineLocalAllocation = 1_000_000;
        long taskLocalAllocation = 10_000_000;
        LocalMemoryContext pipelineLocalSystemMemoryContext = pipelineContext.localSystemMemoryContext();
        pipelineLocalSystemMemoryContext.setBytes(pipelineLocalAllocation);
        assertLocalMemoryAllocations(pipelineContext.getPipelineMemoryContext(),
                pipelineLocalAllocation,
                0,
                pipelineLocalAllocation);
        LocalMemoryContext taskLocalSystemMemoryContext = taskContext.localSystemMemoryContext();
        taskLocalSystemMemoryContext.setBytes(taskLocalAllocation);
        assertLocalMemoryAllocations(
                taskContext.getTaskMemoryContext(),
                pipelineLocalAllocation + taskLocalAllocation,
                0,
                taskLocalAllocation);
        assertEquals(pipelineContext.getPipelineStats().getSystemMemoryReservationInBytes(),
                pipelineLocalAllocation,
                "task level allocations should not be visible at the pipeline level");
        pipelineLocalSystemMemoryContext.setBytes(pipelineLocalSystemMemoryContext.getBytes() - pipelineLocalAllocation);
        assertLocalMemoryAllocations(
                pipelineContext.getPipelineMemoryContext(),
                taskLocalAllocation,
                0,
                0);
        taskLocalSystemMemoryContext.setBytes(taskLocalSystemMemoryContext.getBytes() - taskLocalAllocation);
        assertLocalMemoryAllocations(
                taskContext.getTaskMemoryContext(),
                0,
                0,
                0);
    }

    @Test
    public void testStats()
    {
        LocalMemoryContext systemMemory = operatorContext.newLocalSystemMemoryContext("test");
        LocalMemoryContext userMemory = operatorContext.localUserMemoryContext();
        userMemory.setBytes(100_000_000);
        systemMemory.setBytes(200_000_000);

        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0,
                200_000_000);

        // allocate more and check peak memory reservation
        userMemory.setBytes(600_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                600_000_000,
                0,
                200_000_000);

        userMemory.setBytes(userMemory.getBytes() - 300_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                300_000_000,
                0,
                200_000_000);

        userMemory.setBytes(userMemory.getBytes() - 300_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                0,
                200_000_000);

        operatorContext.destroy();

        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                0,
                0);
    }

    @Test
    public void testRevocableMemoryAllocations()
    {
        LocalMemoryContext systemMemory = operatorContext.newLocalSystemMemoryContext("test");
        LocalMemoryContext userMemory = operatorContext.localUserMemoryContext();
        LocalMemoryContext revocableMemory = operatorContext.localRevocableMemoryContext();
        revocableMemory.setBytes(100_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                100_000_000,
                0);
        userMemory.setBytes(100_000_000);
        systemMemory.setBytes(100_000_000);
        revocableMemory.setBytes(200_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                200_000_000,
                100_000_000);
    }

    @Test
    public void testTrySetBytes()
    {
        LocalMemoryContext localMemoryContext = operatorContext.localUserMemoryContext();
        assertTrue(localMemoryContext.trySetBytes(100_000_000));
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0,
                0);

        assertTrue(localMemoryContext.trySetBytes(200_000_000));
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                200_000_000,
                0,
                0);

        assertTrue(localMemoryContext.trySetBytes(100_000_000));
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0,
                0);

        // allocating more than the pool size should fail and we should have the same stats as before
        assertFalse(localMemoryContext.trySetBytes(memoryPool.getMaxBytes() + 1));
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0,
                0);
    }

    @Test
    public void testTrySetZeroBytesFullPool()
    {
        LocalMemoryContext localMemoryContext = operatorContext.localUserMemoryContext();
        // fill up the pool
        memoryPool.reserve(new QueryId("test_query"), "test", memoryPool.getFreeBytes());
        // try to reserve 0 bytes in the full pool
        assertTrue(localMemoryContext.trySetBytes(localMemoryContext.getBytes()));
    }

    @Test
    public void testDestroy()
    {
        LocalMemoryContext newLocalSystemMemoryContext = operatorContext.newLocalSystemMemoryContext("test");
        LocalMemoryContext newLocalUserMemoryContext = operatorContext.localUserMemoryContext();
        LocalMemoryContext newLocalRevocableMemoryContext = operatorContext.localRevocableMemoryContext();
        newLocalSystemMemoryContext.setBytes(100_000);
        newLocalRevocableMemoryContext.setBytes(200_000);
        newLocalUserMemoryContext.setBytes(400_000);
        assertEquals(operatorContext.getOperatorMemoryContext().getSystemMemory(), 100_000);
        assertEquals(operatorContext.getOperatorMemoryContext().getUserMemory(), 400_000);
        operatorContext.destroy();
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 0, 0, 0);
    }

    @Test
    public void testCumulativeUserMemoryEstimation()
    {
        LocalMemoryContext userMemory = operatorContext.localUserMemoryContext();
        long userMemoryBytes = 100_000_000;
        userMemory.setBytes(userMemoryBytes);
        long startTime = System.nanoTime();
        double cumulativeUserMemory = taskContext.getTaskStats().getCumulativeUserMemory();
        long endTime = System.nanoTime();

        double elapsedTimeInMillis = (endTime - startTime) / 1_000_000.0;
        long averageMemoryForLastPeriod = userMemoryBytes / 2;

        assertTrue(cumulativeUserMemory < elapsedTimeInMillis * averageMemoryForLastPeriod);
    }

    @Test
    public void testCumulativeTotalMemoryEstimation()
    {
        LocalMemoryContext userMemory = operatorContext.localUserMemoryContext();
        LocalMemoryContext systemMemory = operatorContext.localSystemMemoryContext();
        long userMemoryBytes = 100_000_000;
        long systemMemoryBytes = 40_000_000;
        userMemory.setBytes(userMemoryBytes);
        systemMemory.setBytes(systemMemoryBytes);
        long startTime = System.nanoTime();
        double cumulativeTotalMemory = taskContext.getTaskStats().getCumulativeTotalMemory();
        long endTime = System.nanoTime();

        double elapsedTimeInMillis = (endTime - startTime) / 1_000_000.0;
        long averageMemoryForLastPeriod = (userMemoryBytes + systemMemoryBytes) / 2;

        assertTrue(cumulativeTotalMemory < elapsedTimeInMillis * averageMemoryForLastPeriod);
    }

    private void assertStats(
            OperatorStats operatorStats,
            DriverStats driverStats,
            PipelineStats pipelineStats,
            TaskStats taskStats,
            long expectedUserMemory,
            long expectedRevocableMemory,
            long expectedSystemMemory)
    {
        assertEquals(operatorStats.getUserMemoryReservation().toBytes(), expectedUserMemory);
        assertEquals(driverStats.getUserMemoryReservation().toBytes(), expectedUserMemory);
        assertEquals(pipelineStats.getUserMemoryReservationInBytes(), expectedUserMemory);
        assertEquals(taskStats.getUserMemoryReservationInBytes(), expectedUserMemory);

        assertEquals(operatorStats.getSystemMemoryReservation().toBytes(), expectedSystemMemory);
        assertEquals(driverStats.getSystemMemoryReservation().toBytes(), expectedSystemMemory);
        assertEquals(pipelineStats.getSystemMemoryReservationInBytes(), expectedSystemMemory);
        assertEquals(taskStats.getSystemMemoryReservationInBytes(), expectedSystemMemory);

        assertEquals(operatorStats.getRevocableMemoryReservation().toBytes(), expectedRevocableMemory);
        assertEquals(driverStats.getRevocableMemoryReservation().toBytes(), expectedRevocableMemory);
        assertEquals(pipelineStats.getRevocableMemoryReservationInBytes(), expectedRevocableMemory);
        assertEquals(taskStats.getRevocableMemoryReservationInBytes(), expectedRevocableMemory);
    }

    private void assertAllocationFails(Consumer<Void> allocationFunction, String expectedPattern)
    {
        try {
            allocationFunction.accept(null);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) {
            assertTrue(Pattern.matches(expectedPattern, e.getMessage()),
                    "\nExpected (re) :" + expectedPattern + "\nActual :" + e.getMessage());
        }
    }

    // the allocations that are done at the operator level are reflected at that level and all the way up to the pools
    private void assertOperatorMemoryAllocations(
            MemoryTrackingContext memoryTrackingContext,
            long expectedUserMemory,
            long expectedSystemMemory,
            long expectedRevocableMemory)
    {
        assertEquals(memoryTrackingContext.getUserMemory(), expectedUserMemory, "User memory verification failed");
        // both user and system memory are allocated from the same memoryPool
        assertEquals(memoryPool.getReservedBytes(), expectedUserMemory + expectedSystemMemory, "Memory pool verification failed");
        assertEquals(memoryTrackingContext.getSystemMemory(), expectedSystemMemory, "System memory verification failed");
        assertEquals(memoryTrackingContext.getRevocableMemory(), expectedRevocableMemory, "Revocable memory verification failed");
    }

    // the local allocations are reflected only at that level and all the way up to the pools
    private void assertLocalMemoryAllocations(
            MemoryTrackingContext memoryTrackingContext,
            long expectedPoolMemory,
            long expectedContextUserMemory,
            long expectedContextSystemMemory)
    {
        assertEquals(memoryTrackingContext.getUserMemory(), expectedContextUserMemory, "User memory verification failed");
        assertEquals(memoryPool.getReservedBytes(), expectedPoolMemory, "Memory pool verification failed");
        assertEquals(memoryTrackingContext.localSystemMemoryContext().getBytes(), expectedContextSystemMemory, "Local system memory verification failed");
    }
}
