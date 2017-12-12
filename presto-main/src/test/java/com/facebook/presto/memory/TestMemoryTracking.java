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
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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
    private static final DataSize queryMaxMemory = new DataSize(1, GIGABYTE);
    private static final DataSize memoryPoolSize = new DataSize(1, GIGABYTE);
    private static final DataSize systemMemoryPoolSize = new DataSize(1, GIGABYTE);
    private static final DataSize maxSpillSize = new DataSize(1, GIGABYTE);
    private static final DataSize queryMaxSpillSize = new DataSize(1, GIGABYTE);
    private static final SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(maxSpillSize);

    private QueryContext queryContext;
    private TaskContext taskContext;
    private PipelineContext pipelineContext;
    private DriverContext driverContext;
    private OperatorContext operatorContext;
    private MemoryPool userPool;
    private MemoryPool systemPool;
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
        userPool = null;
        systemPool = null;
    }

    @BeforeMethod
    public void setUpTest()
    {
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
        operatorContext = driverContext.addOperatorContext(1, new PlanNodeId("a"), "test-operator");
    }

    @Test
    public void testOperatorAllocations()
    {
        MemoryTrackingContext operatorMemoryContext = operatorContext.getOperatorMemoryContext();
        LocalMemoryContext systemMemory = operatorContext.newLocalSystemMemoryContext();
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
        assertAllocationFails((ignored) -> userMemory.setBytes(userMemory.getBytes() - 500), "tried to free more memory than is reserved");
        operatorContext.destroy();
        assertOperatorMemoryAllocations(operatorMemoryContext, 0, 0, 0);
    }

    @Test
    public void testLocalSystemAllocations()
    {
        long pipelineLocalAllocation = 1_000_000;
        long taskLocalAllocation = 10_000_000;
        LocalMemoryContext pipelineLocalSystemMemoryContext = pipelineContext.localSystemMemoryContext();
        pipelineLocalSystemMemoryContext.setBytes(pipelineLocalAllocation);
        assertLocalMemoryAllocations(pipelineContext.getPipelineMemoryContext(),
                0,
                0,
                pipelineLocalAllocation,
                pipelineLocalAllocation);
        LocalMemoryContext taskLocalSystemMemoryContext = taskContext.localSystemMemoryContext();
        taskLocalSystemMemoryContext.setBytes(taskLocalAllocation);
        assertLocalMemoryAllocations(
                taskContext.getTaskMemoryContext(),
                0,
                0,
                taskLocalAllocation + pipelineLocalAllocation, // at the pool level we should observe both
                taskLocalAllocation);
        assertEquals(pipelineContext.getPipelineStats().getSystemMemoryReservation().toBytes(),
                pipelineLocalAllocation,
                "task level allocations should not be visible at the pipeline level");
        pipelineLocalSystemMemoryContext.setBytes(pipelineLocalSystemMemoryContext.getBytes() - pipelineLocalAllocation);
        assertLocalMemoryAllocations(
                pipelineContext.getPipelineMemoryContext(),
                0,
                0,
                taskLocalAllocation,
                0);
        taskLocalSystemMemoryContext.setBytes(taskLocalSystemMemoryContext.getBytes() - taskLocalAllocation);
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
        MemoryTrackingContext operatorMemoryContext = operatorContext.getOperatorMemoryContext();
        LocalMemoryContext systemMemory = operatorContext.newLocalSystemMemoryContext();
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
                200_000_000,
                100_000_000);

        // allocate more and check peak memory reservation
        userMemory.setBytes(600_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                600_000_000,
                0,
                200_000_000,
                600_000_000);

        userMemory.setBytes(userMemory.getBytes() - 300_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                300_000_000,
                0,
                200_000_000,
                600_000_000);

        userMemory.setBytes(userMemory.getBytes() - 300_000_000);
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                0,
                200_000_000,
                600_000_000);

        operatorContext.destroy();

        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                0,
                0,
                0,
                600_000_000);
    }

    @Test
    public void testRevocableMemoryAllocations()
    {
        LocalMemoryContext systemMemory = operatorContext.newLocalSystemMemoryContext();
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
                0,
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
                100_000_000,
                100_000_000);
    }

    @Test
    public void testPeakMemoryReservation()
    {
        LocalMemoryContext userMemory = operatorContext.localUserMemoryContext();
        userMemory.setBytes(1000);
        assertEquals(driverContext.getDriverStats().getPeakMemoryReservation().toBytes(), 1000);
        userMemory.setBytes(3000);
        assertEquals(driverContext.getDriverStats().getPeakMemoryReservation().toBytes(), 3000);
        userMemory.setBytes(4500);
        assertEquals(driverContext.getDriverStats().getPeakMemoryReservation().toBytes(), 4500);
        assertTrue(operatorContext.trySetMemoryReservation(3500));
        assertEquals(driverContext.getDriverStats().getPeakMemoryReservation().toBytes(), 4500,
                "setting reservation to a lower value shouldn't change peak memory reservation");
    }

    @Test
    public void testTryReserveMemory()
    {
        assertTrue(operatorContext.trySetMemoryReservation(100_000_000));
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                100_000_000,
                0,
                0,
                100_000_000); // trySetMemoryReservation should update peak usage

        assertTrue(operatorContext.trySetMemoryReservation(200_000_000));
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                200_000_000,
                0,
                0,
                200_000_000); // trySetMemoryReservation should update peak usage

        assertFalse(operatorContext.trySetMemoryReservation(userPool.getMaxBytes() + 1));
        // the allocation should fail and we should have the same stats as before
        assertStats(
                operatorContext.getOperatorStats(),
                driverContext.getDriverStats(),
                pipelineContext.getPipelineStats(),
                taskContext.getTaskStats(),
                200_000_000,
                0,
                0,
                200_000_000);
    }

    @Test
    public void testTransferMemoryToTaskContext()
    {
        LocalMemoryContext userMemory = operatorContext.localUserMemoryContext();
        userMemory.setBytes(300_000_000);
        assertEquals(operatorContext.getOperatorMemoryContext().getUserMemory(), 300_000_000);
        assertEquals(driverContext.getDriverMemoryContext().getUserMemory(), 300_000_000);
        assertEquals(pipelineContext.getPipelineMemoryContext().getUserMemory(), 300_000_000);
        assertEquals(taskContext.getTaskMemoryContext().getUserMemory(), 300_000_000);

        operatorContext.transferMemoryToTaskContext(500_000_000);
        assertEquals(operatorContext.getOperatorMemoryContext().getUserMemory(), 0);
        assertEquals(driverContext.getDriverMemoryContext().getUserMemory(), 0);
        assertEquals(pipelineContext.getPipelineMemoryContext().getUserMemory(), 0);
        assertEquals(taskContext.getTaskMemoryContext().getUserMemory(), 500_000_000);
        assertLocalMemoryAllocations(taskContext.getTaskMemoryContext(), 500_000_000, 500_000_000, 0, 0);
    }

    @Test
    public void testDestroy()
    {
        LocalMemoryContext newLocalSystemMemoryContext = operatorContext.newLocalSystemMemoryContext();
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
            long expectedSystemMemory,
            long expectedRevocableMemory)
    {
        assertEquals(memoryTrackingContext.getUserMemory(), expectedUserMemory, "User memory verification failed");
        assertEquals(userPool.getReservedBytes(), expectedUserMemory, "User pool memory verification failed");
        assertEquals(memoryTrackingContext.getSystemMemory(), expectedSystemMemory, "System memory verification failed");
        assertEquals(systemPool.getReservedBytes(), expectedSystemMemory, "System pool memory verification failed");
        assertEquals(memoryTrackingContext.getRevocableMemory(), expectedRevocableMemory, "Revocable memory verification failed");
    }

    // the local allocations are reflected only at that level and all the way up to the pools
    private void assertLocalMemoryAllocations(
            MemoryTrackingContext memoryTrackingContext,
            long expectedUserPoolMemory,
            long expectedContextUserMemory,
            long expectedSystemPoolMemory,
            long expectedContextSystemMemory)
    {
        assertEquals(memoryTrackingContext.getUserMemory(), expectedContextUserMemory, "User memory verification failed");
        assertEquals(userPool.getReservedBytes(), expectedUserPoolMemory, "User pool memory verification failed");
        assertEquals(memoryTrackingContext.localSystemMemoryContext().getBytes(), expectedContextSystemMemory, "Local system memory verification failed");
        assertEquals(systemPool.getReservedBytes(), expectedSystemPoolMemory, "System pool memory verification failed");
    }
}
