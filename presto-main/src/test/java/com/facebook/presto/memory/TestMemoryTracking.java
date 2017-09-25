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
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverStats;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.PipelineStats;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.LocalMemoryContext;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestMemoryTracking
{
    private QueryContext queryContext;
    private TaskContext taskContext;
    private PipelineContext pipelineContext;
    private DriverContext driverContext;
    private OperatorContext operatorContext;
    private MemoryPool userPool;
    private MemoryPool systemPool;
    private ExecutorService notificationExecutor;
    private ScheduledExecutorService yieldExecutor;

    @BeforeMethod
    public void setUp()
    {
        DataSize queryMaxMemory = new DataSize(1, GIGABYTE);
        DataSize memoryPoolSize = new DataSize(1, GIGABYTE);
        DataSize systemMemoryPoolSize = new DataSize(1, GIGABYTE);
        DataSize maxSpillSize = new DataSize(1, GIGABYTE);
        DataSize queryMaxSpillSize = new DataSize(1, GIGABYTE);
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(maxSpillSize);
        notificationExecutor = newCachedThreadPool(daemonThreadsNamed("local-query-runner-executor-%s"));
        yieldExecutor = newScheduledThreadPool(2, daemonThreadsNamed("local-query-runner-scheduler-%s"));
        userPool = new MemoryPool(new MemoryPoolId("test"), memoryPoolSize);
        systemPool = new MemoryPool(new MemoryPoolId("testSystem"), systemMemoryPoolSize);
        queryContext = new QueryContext(
                new QueryId("test_query"),
                queryMaxMemory,
                userPool,
                systemPool,
                notificationExecutor,
                yieldExecutor,
                queryMaxSpillSize,
                spillSpaceTracker);
        taskContext = queryContext.addTaskContext(
                new TaskStateMachine(new TaskId("query", 0, 0), notificationExecutor),
                testSessionBuilder().build(),
                true,
                true);
        pipelineContext = taskContext.addPipelineContext(0, true, true);
        driverContext = pipelineContext.addDriverContext();
        operatorContext = new OperatorContext(1,
                new PlanNodeId("a"),
                "test",
                driverContext,
                newCachedThreadPool(daemonThreadsNamed("test-%s")),
                driverContext.getDriverMemoryContext().newMemoryTrackingContext());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryContext = null;
        taskContext = null;
        pipelineContext = null;
        driverContext = null;
        operatorContext = null;
        userPool = null;
        systemPool = null;
        notificationExecutor.shutdownNow();
        yieldExecutor.shutdownNow();
    }

    @Test
    public void testOperatorAllocations()
    {
        operatorContext.reserveMemory(100);
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 100, 0);
        operatorContext.setSystemMemory(1_000_000);
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 100, 1_000_000);
        operatorContext.setSystemMemory(2_000_000);
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 100, 2_000_000);
        operatorContext.reserveMemory(400);
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 500, 2_000_000);
        operatorContext.freeMemory(500);
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 0, 2_000_000);
        assertAllocationFails((ignored) -> operatorContext.freeMemory(500), "\\Qcannot free more memory than reserved\\E");
        operatorContext.freeSystemMemory();
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 0, 0);
        assertAllocationFails((ignored) -> operatorContext.setSystemMemory(-1), ".*is negative");
    }

    @Test
    public void testLocalSystemAllocations()
    {
        long pipelineLocalAllocation = 1_000_000;
        long taskLocalAllocation = 10_000_000;
        LocalMemoryContext pipelineLocalSystemMemoryContext = pipelineContext.localSystemMemoryContext();
        pipelineLocalSystemMemoryContext.addBytes(pipelineLocalAllocation);
        assertLocalMemoryAllocations(pipelineContext.getPipelineMemoryContext(),
                0,
                0,
                pipelineLocalAllocation,
                pipelineLocalAllocation);
        LocalMemoryContext taskLocalSystemMemoryContext = taskContext.localSystemMemoryContext();
        taskLocalSystemMemoryContext.addBytes(taskLocalAllocation);
        assertLocalMemoryAllocations(
                taskContext.getTaskMemoryContext(),
                0,
                0,
                taskLocalAllocation + pipelineLocalAllocation, // at the pool level we should observe both
                taskLocalAllocation);
        assertEquals(pipelineContext.getPipelineStats().getSystemMemoryReservation().toBytes(),
                pipelineLocalAllocation,
                "task level allocations should not be visible at the pipeline level");
        pipelineLocalSystemMemoryContext.addBytes(-pipelineLocalAllocation);
        assertLocalMemoryAllocations(
                pipelineContext.getPipelineMemoryContext(),
                0,
                0,
                taskLocalAllocation,
                0);
        taskLocalSystemMemoryContext.addBytes(-taskLocalAllocation);
        assertLocalMemoryAllocations(
                taskContext.getTaskMemoryContext(),
                0,
                0,
                0,
                0);
    }

    @Test
    public void testStats()
    {
        operatorContext.reserveMemory(100_000_000);
        operatorContext.setSystemMemory(200_000_000);

        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0,
                200_000_000,
                100_000_000);

        // allocate more and check peak memory reservation
        operatorContext.reserveMemory(500_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                600_000_000,
                0,
                200_000_000,
                500_000_000);

        operatorContext.freeMemory(300_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                300_000_000,
                0,
                200_000_000,
                500_000_000);

        operatorContext.freeMemory(300_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                0,
                200_000_000,
                500_000_000);

        operatorContext.freeSystemMemory();
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                0,
                0,
                500_000_000);
    }

    @Test
    public void testRevocableMemoryAllocations()
    {
        operatorContext.reserveRevocableMemory(100_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                100_000_000,
                0,
                0);
        operatorContext.reserveMemory(100_000_000);
        operatorContext.setSystemMemory(100_000_000);
        operatorContext.reserveRevocableMemory(100_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                200_000_000,
                100_000_000,
                100_000_000);
    }

    @Test
    public void testTryReserveMemory()
    {
        assertTrue(operatorContext.tryReserveMemory(100_000_000));
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0,
                0,
                100_000_000); // tryReserveMemory should update peak usage

        assertFalse(operatorContext.tryReserveMemory(userPool.getMaxBytes() + 1));
        // the allocation should fail and we should have the same stats as before
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0,
                0,
                100_000_000); // tryReserveMemory should update peak usage
    }

    @Test
    public void testTransferMemoryToTaskContext()
    {
        operatorContext.reserveMemory(300_000_000);
        operatorContext.transferMemoryToTaskContext(300_000_000);
        assertEquals(operatorContext.getOperatorMemoryContext().reservedLocalUserMemory(), 0);
        assertLocalMemoryAllocations(taskContext.getTaskMemoryContext(), 300_000_000, 300_000_000, 0, 0);
    }

    @Test
    public void testFreeSystemMemory()
    {
        LocalMemoryContext newLocalMemoryContext = operatorContext.newLocalSystemMemoryContext();
        newLocalMemoryContext.setBytes(100_000);
        assertEquals(operatorContext.getOperatorMemoryContext().reservedSystemMemory(), 100_000);
        operatorContext.freeSystemMemory();
        assertOperatorMemoryAllocations(operatorContext.getOperatorMemoryContext(), 0, 0);
    }

    private void assertStats(
            OperatorStats operatorStats,
            DriverStats driverStats,
            PipelineStats pipelineStats,
            TaskStats taskStats,
            long expectedUserMemory,
            long expectedRevocableMemory,
            long expectedSystemMemory,
            long expectedPeakDriverUserMemory)
    {
        assertEquals(operatorStats.getMemoryReservation().toBytes(), expectedUserMemory);
        assertEquals(driverStats.getMemoryReservation().toBytes(), expectedUserMemory);
        assertEquals(pipelineStats.getMemoryReservation().toBytes(), expectedUserMemory);
        assertEquals(taskStats.getMemoryReservation().toBytes(), expectedUserMemory);

        assertEquals(operatorStats.getSystemMemoryReservation().toBytes(), expectedSystemMemory);
        assertEquals(driverStats.getSystemMemoryReservation().toBytes(), expectedSystemMemory);
        assertEquals(pipelineStats.getSystemMemoryReservation().toBytes(), expectedSystemMemory);
        assertEquals(taskStats.getSystemMemoryReservation().toBytes(), expectedSystemMemory);

        assertEquals(operatorStats.getRevocableMemoryReservation().toBytes(), expectedRevocableMemory);
        assertEquals(driverStats.getRevocableMemoryReservation().toBytes(), expectedRevocableMemory);
        assertEquals(pipelineStats.getRevocableMemoryReservation().toBytes(), expectedRevocableMemory);
        assertEquals(taskStats.getRevocableMemoryReservation().toBytes(), expectedRevocableMemory);

        assertEquals(driverStats.getPeakMemoryReservation().toBytes(), expectedPeakDriverUserMemory);
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
            long expectedSystemMemory)
    {
        assertEquals(memoryTrackingContext.reservedUserMemory(), expectedUserMemory, "User memory verification failed");
        assertEquals(userPool.getReservedBytes(), expectedUserMemory, "User pool memory verification failed");
        assertEquals(memoryTrackingContext.reservedSystemMemory(), expectedSystemMemory, "System memory verification failed");
        assertEquals(systemPool.getReservedBytes(), expectedSystemMemory, "System pool memory verification failed");
    }

    // the local allocations are reflected only at that level and all the way up to the pools
    private void assertLocalMemoryAllocations(
            MemoryTrackingContext memoryTrackingContext,
            long expectedUserPoolMemory,
            long expectedContextUserMemory,
            long expectedSystemPoolMemory,
            long expectedContextSystemMemory)
    {
        assertEquals(memoryTrackingContext.reservedUserMemory(), expectedContextUserMemory, "User memory verification failed");
        assertEquals(userPool.getReservedBytes(), expectedUserPoolMemory, "User pool memory verification failed");
        assertEquals(memoryTrackingContext.reservedLocalSystemMemory(), expectedContextSystemMemory, "Local system memory verification failed");
        assertEquals(systemPool.getReservedBytes(), expectedSystemPoolMemory, "System pool memory verification failed");
    }
}
