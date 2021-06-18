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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TestingGcMonitor;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.execution.TestSqlTaskManager.MockExchangeClientSupplier;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.SpoolingOutputBufferFactory;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.memory.context.MemoryTrackingContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskMemoryReservationSummary;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.google.common.base.Functions;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.execution.SqlTask.createSqlTask;
import static com.facebook.presto.execution.TaskManagerConfig.TaskPriorityTracking.TASK_FAIR;
import static com.facebook.presto.execution.TaskTestUtils.PLAN_FRAGMENT;
import static com.facebook.presto.execution.TaskTestUtils.SPLIT;
import static com.facebook.presto.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static com.facebook.presto.execution.TaskTestUtils.createTestSplitMonitor;
import static com.facebook.presto.execution.TaskTestUtils.createTestingPlanner;
import static com.facebook.presto.execution.TaskTestUtils.updateTask;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.TaskSpillingStrategy.ORDER_BY_CREATE_TIME;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.TaskSpillingStrategy.ORDER_BY_REVOCABLE_BYTES;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMemoryRevokingScheduler
{
    public static final OutputBuffers.OutputBufferId OUT = new OutputBuffers.OutputBufferId(0);
    private final AtomicInteger idGeneator = new AtomicInteger();
    private final SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(new DataSize(10, GIGABYTE));

    private final Map<QueryId, QueryContext> queryContexts = new HashMap<>();

    private ExecutorService singleThreadedExecutor;
    private ScheduledExecutorService singleThreadedScheduledExecutor;
    private ScheduledExecutorService scheduledExecutor;
    private SqlTaskExecutionFactory sqlTaskExecutionFactory;
    private MemoryPool memoryPool;

    private Set<OperatorContext> allOperatorContexts;

    @BeforeMethod
    public void setUp()
    {
        memoryPool = new MemoryPool(GENERAL_POOL, new DataSize(10, BYTE));

        TaskExecutor taskExecutor = new TaskExecutor(8, 16, 3, 4, TASK_FAIR, Ticker.systemTicker());
        taskExecutor.start();

        // Must be single threaded
        singleThreadedExecutor = newSingleThreadExecutor(threadsNamed("task-notification-%s"));
        singleThreadedScheduledExecutor = newScheduledThreadPool(1, threadsNamed("task-notification-%s"));
        scheduledExecutor = newScheduledThreadPool(2, threadsNamed("task-notification-%s"));

        LocalExecutionPlanner planner = createTestingPlanner();

        sqlTaskExecutionFactory = new SqlTaskExecutionFactory(
                singleThreadedExecutor,
                taskExecutor,
                planner,
                new BlockEncodingManager(),
                new OrderingCompiler(),
                createTestSplitMonitor(),
                new TaskManagerConfig()
                        .setPerOperatorAllocationTrackingEnabled(true)
                        .setTaskCpuTimerEnabled(true)
                        .setPerOperatorAllocationTrackingEnabled(true)
                        .setTaskAllocationTrackingEnabled(true));

        allOperatorContexts = null;
        TestOperatorContext.firstOperator = null;
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        queryContexts.clear();
        memoryPool = null;
        singleThreadedExecutor.shutdownNow();
        singleThreadedScheduledExecutor.shutdown();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testMemoryPoolRevoking()
            throws Exception
    {
        QueryContext q1 = getOrCreateQueryContext(new QueryId("q1"), memoryPool);
        QueryContext q2 = getOrCreateQueryContext(new QueryId("q2"), memoryPool);

        SqlTask sqlTask1 = newSqlTask(q1.getQueryId(), memoryPool);
        SqlTask sqlTask2 = newSqlTask(q2.getQueryId(), memoryPool);

        TaskContext taskContext1 = getOrCreateTaskContext(sqlTask1);
        PipelineContext pipelineContext11 = taskContext1.addPipelineContext(0, false, false, false);
        DriverContext driverContext111 = pipelineContext11.addDriverContext();
        OperatorContext operatorContext1 = driverContext111.addOperatorContext(1, new PlanNodeId("na"), "na");
        OperatorContext operatorContext2 = driverContext111.addOperatorContext(2, new PlanNodeId("na"), "na");
        DriverContext driverContext112 = pipelineContext11.addDriverContext();
        OperatorContext operatorContext3 = driverContext112.addOperatorContext(3, new PlanNodeId("na"), "na");

        TaskContext taskContext2 = getOrCreateTaskContext(sqlTask2);
        PipelineContext pipelineContext21 = taskContext2.addPipelineContext(1, false, false, false);
        DriverContext driverContext211 = pipelineContext21.addDriverContext();
        OperatorContext operatorContext4 = driverContext211.addOperatorContext(4, new PlanNodeId("na"), "na");
        OperatorContext operatorContext5 = driverContext211.addOperatorContext(5, new PlanNodeId("na"), "na");

        List<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(
                singletonList(memoryPool),
                () -> tasks,
                queryContexts::get,
                1.0,
                1.0,
                ORDER_BY_CREATE_TIME,
                false);
        try {
            scheduler.start();
            allOperatorContexts = ImmutableSet.of(operatorContext1, operatorContext2, operatorContext3, operatorContext4, operatorContext5);
            assertMemoryRevokingNotRequested();

            assertEquals(10, memoryPool.getFreeBytes());
            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingNotRequested();

            LocalMemoryContext revocableMemory1 = operatorContext1.localRevocableMemoryContext();
            LocalMemoryContext revocableMemory3 = operatorContext3.localRevocableMemoryContext();
            LocalMemoryContext revocableMemory4 = operatorContext4.localRevocableMemoryContext();
            LocalMemoryContext revocableMemory5 = operatorContext5.localRevocableMemoryContext();

            revocableMemory1.setBytes(3);
            revocableMemory3.setBytes(6);
            assertEquals(1, memoryPool.getFreeBytes());
            scheduler.awaitAsynchronousCallbacksRun();
            // we are still good - no revoking needed
            assertMemoryRevokingNotRequested();

            revocableMemory4.setBytes(7);
            assertEquals(-6, memoryPool.getFreeBytes());
            scheduler.awaitAsynchronousCallbacksRun();
            // we need to revoke 3 and 6
            assertMemoryRevokingRequestedFor(operatorContext1, operatorContext3);

            // lets revoke some bytes
            revocableMemory1.setBytes(0);
            operatorContext1.resetMemoryRevokingRequested();
            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingRequestedFor(operatorContext3);
            assertEquals(-3, memoryPool.getFreeBytes());

            // and allocate some more
            revocableMemory5.setBytes(3);
            assertEquals(-6, memoryPool.getFreeBytes());
            scheduler.awaitAsynchronousCallbacksRun();
            // we are still good with just OC3 in process of revoking
            assertMemoryRevokingRequestedFor(operatorContext3);

            // and allocate some more
            revocableMemory5.setBytes(4);
            assertEquals(-7, memoryPool.getFreeBytes());
            scheduler.awaitAsynchronousCallbacksRun();
            // now we have to trigger revoking for OC4
            assertMemoryRevokingRequestedFor(operatorContext3, operatorContext4);
        }
        finally {
            scheduler.stop();
        }
    }

    @Test
    public void testCountAlreadyRevokedMemoryWithinAPool()
            throws Exception
    {
        // Given
        MemoryPool anotherMemoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(10, BYTE));
        SqlTask sqlTask1 = newSqlTask(new QueryId("q1"), anotherMemoryPool);
        OperatorContext operatorContext1 = createContexts(sqlTask1);

        SqlTask sqlTask2 = newSqlTask(new QueryId("q2"), memoryPool);
        OperatorContext operatorContext2 = createContexts(sqlTask2);

        List<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(
                asList(memoryPool, anotherMemoryPool),
                () -> tasks,
                queryContexts::get,
                1.0,
                1.0,
                ORDER_BY_CREATE_TIME,
                false);
        try {
            scheduler.start();
            allOperatorContexts = ImmutableSet.of(operatorContext1, operatorContext2);

            /*
             * sqlTask1 fills its pool
             */
            operatorContext1.localRevocableMemoryContext().setBytes(12);
            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingRequestedFor(operatorContext1);

            /*
             * When sqlTask2 fills its pool
             */
            operatorContext2.localRevocableMemoryContext().setBytes(12);
            scheduler.awaitAsynchronousCallbacksRun();
            /*
             * Then sqlTask2 should be asked to revoke its memory too
             */
            assertMemoryRevokingRequestedFor(operatorContext1, operatorContext2);
        }
        finally {
            scheduler.stop();
        }
    }

    /**
     * Ensures that when revoking is requested, the first task to start revoking is based on the {@link FeaturesConfig.TaskSpillingStrategy}
     */
    @Test
    public void testTaskRevokingOrderForCreateTime()
            throws Exception
    {
        SqlTask sqlTask1 = newSqlTask(new QueryId("query"), memoryPool);
        TestOperatorContext operatorContext1 = createTestingOperatorContexts(sqlTask1, "operator1");

        SqlTask sqlTask2 = newSqlTask(new QueryId("query"), memoryPool);
        TestOperatorContext operatorContext2 = createTestingOperatorContexts(sqlTask2, "operator2");

        allOperatorContexts = ImmutableSet.of(operatorContext1, operatorContext2);
        List<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(
                singletonList(memoryPool),
                () -> tasks,
                queryContexts::get,
                1.0,
                1.0,
                ORDER_BY_CREATE_TIME,
                false);
        try {
            scheduler.start(); // no periodic check initiated

            assertMemoryRevokingNotRequested();

            operatorContext1.localRevocableMemoryContext().setBytes(11);
            operatorContext2.localRevocableMemoryContext().setBytes(12);

            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingRequestedFor(operatorContext1, operatorContext2);
            assertEquals(TestOperatorContext.firstOperator, "operator1"); // operator1 should revoke first as it belongs to a task that was created earlier
        }
        finally {
            scheduler.stop();
        }
    }

    @Test
    public void testTaskRevokingOrderForRevocableBytes()
            throws Exception
    {
        SqlTask sqlTask1 = newSqlTask(new QueryId("query"), memoryPool);
        TestOperatorContext operatorContext1 = createTestingOperatorContexts(sqlTask1, "operator1");

        SqlTask sqlTask2 = newSqlTask(new QueryId("query"), memoryPool);
        TestOperatorContext operatorContext2 = createTestingOperatorContexts(sqlTask2, "operator2");

        allOperatorContexts = ImmutableSet.of(operatorContext1, operatorContext2);
        List<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(
                singletonList(memoryPool),
                () -> tasks,
                queryContexts::get,
                1.0,
                1.0,
                ORDER_BY_REVOCABLE_BYTES,
                false);
        try {
            scheduler.start();

            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingNotRequested();

            operatorContext1.localRevocableMemoryContext().setBytes(11);
            operatorContext2.localRevocableMemoryContext().setBytes(12);

            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingRequestedFor(operatorContext1, operatorContext2);
            assertEquals(TestOperatorContext.firstOperator, "operator2"); // operator2 should revoke first since it (and it's encompassing task) has allocated more bytes
        }
        finally {
            scheduler.stop();
        }
    }

    @Test
    public void testTaskThresholdRevokingScheduler()
            throws Exception
    {
        SqlTask sqlTask1 = newSqlTask(new QueryId("query"), memoryPool);
        TestOperatorContext operatorContext11 = createTestingOperatorContexts(sqlTask1, "operator11");
        TestOperatorContext operatorContext12 = createTestingOperatorContexts(sqlTask1, "operator12");

        SqlTask sqlTask2 = newSqlTask(new QueryId("query"), memoryPool);
        TestOperatorContext operatorContext2 = createTestingOperatorContexts(sqlTask2, "operator2");

        allOperatorContexts = ImmutableSet.of(operatorContext11, operatorContext12, operatorContext2);
        List<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        ImmutableMap<TaskId, SqlTask> taskMap = ImmutableMap.of(sqlTask1.getTaskId(), sqlTask1, sqlTask2.getTaskId(), sqlTask2);
        TaskThresholdMemoryRevokingScheduler scheduler = new TaskThresholdMemoryRevokingScheduler(
                singletonList(memoryPool), () -> tasks, taskMap::get, singleThreadedScheduledExecutor, 5L);

        assertMemoryRevokingNotRequested();

        operatorContext11.localRevocableMemoryContext().setBytes(3);
        operatorContext2.localRevocableMemoryContext().setBytes(2);
        // at this point, Task1 = 3 total bytes, Task2 = 2 total bytes

        requestMemoryRevoking(scheduler);
        assertMemoryRevokingNotRequested();

        operatorContext12.localRevocableMemoryContext().setBytes(3);
        // at this point, Task1 = 6 total bytes, Task2 = 2 total bytes

        requestMemoryRevoking(scheduler);
        // only operator11 should revoke since we need to revoke only 1 byte
        // threshold - (operator11 + operator12) => 5 - (3 + 3) = 1 bytes to revoke
        assertMemoryRevokingRequestedFor(operatorContext11);

        // revoke 2 bytes in operator11
        operatorContext11.localRevocableMemoryContext().setBytes(1);
        // at this point, Task1 = 3 total bytes, Task2 = 2 total bytes
        operatorContext11.resetMemoryRevokingRequested();
        requestMemoryRevoking(scheduler);
        assertMemoryRevokingNotRequested();

        operatorContext12.localRevocableMemoryContext().setBytes(6); // operator12 fills up
        // at this point, Task1 = 7 total bytes, Task2 = 2 total bytes
        requestMemoryRevoking(scheduler);
        // both operator11 and operator 12 are revoking since we revoke in order of operator creation within the task until we are below the memory revoking threshold
        assertMemoryRevokingRequestedFor(operatorContext11, operatorContext12);

        operatorContext11.localRevocableMemoryContext().setBytes(2);
        operatorContext11.resetMemoryRevokingRequested();
        operatorContext12.localRevocableMemoryContext().setBytes(2);
        operatorContext12.resetMemoryRevokingRequested();
        // at this point, Task1 = 4 total bytes, Task2 = 2 total bytes

        requestMemoryRevoking(scheduler);
        assertMemoryRevokingNotRequested(); // no need to revoke

        operatorContext2.localRevocableMemoryContext().setBytes(6);
        // at this point, Task1 = 4 total bytes, Task2 = 6 total bytes, operators in Task2 must be revoked
        requestMemoryRevoking(scheduler);
        assertMemoryRevokingRequestedFor(operatorContext2);
    }

    @Test
    public void testTaskThresholdRevokingSchedulerImmediate()
            throws Exception
    {
        SqlTask sqlTask1 = newSqlTask(new QueryId("query"), memoryPool);
        TestOperatorContext operatorContext11 = createTestingOperatorContexts(sqlTask1, "operator11");
        TestOperatorContext operatorContext12 = createTestingOperatorContexts(sqlTask1, "operator12");

        SqlTask sqlTask2 = newSqlTask(new QueryId("query"), memoryPool);
        TestOperatorContext operatorContext2 = createTestingOperatorContexts(sqlTask2, "operator2");

        allOperatorContexts = ImmutableSet.of(operatorContext11, operatorContext12, operatorContext2);
        List<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        ImmutableMap<TaskId, SqlTask> taskMap = ImmutableMap.of(sqlTask1.getTaskId(), sqlTask1, sqlTask2.getTaskId(), sqlTask2);
        TaskThresholdMemoryRevokingScheduler scheduler = new TaskThresholdMemoryRevokingScheduler(
                singletonList(memoryPool), () -> tasks, taskMap::get, singleThreadedScheduledExecutor, 5L);
        scheduler.registerPoolListeners(); // no periodic check initiated

        assertMemoryRevokingNotRequested();

        operatorContext11.localRevocableMemoryContext().setBytes(3);
        operatorContext2.localRevocableMemoryContext().setBytes(2);
        // at this point, Task1 = 3 total bytes, Task2 = 2 total bytes

        // this ensures that we are waiting for the memory revocation listener and not using polling-based revoking
        awaitTaskThresholdAsynchronousCallbacksRun();
        assertMemoryRevokingNotRequested();

        operatorContext12.localRevocableMemoryContext().setBytes(3);
        // at this point, Task1 = 6 total bytes, Task2 = 2 total bytes

        awaitTaskThresholdAsynchronousCallbacksRun();
        // only operator11 should revoke since we need to revoke only 1 byte
        // threshold - (operator11 + operator12) => 5 - (3 + 3) = 1 bytes to revoke
        assertMemoryRevokingRequestedFor(operatorContext11);

        // revoke 2 bytes in operator11
        operatorContext11.localRevocableMemoryContext().setBytes(1);
        // at this point, Task1 = 3 total bytes, Task2 = 2 total bytes
        operatorContext11.resetMemoryRevokingRequested();
        awaitTaskThresholdAsynchronousCallbacksRun();
        assertMemoryRevokingNotRequested();

        operatorContext12.localRevocableMemoryContext().setBytes(6); // operator12 fills up
        // at this point, Task1 = 7 total bytes, Task2 = 2 total bytes
        awaitTaskThresholdAsynchronousCallbacksRun();
        // both operator11 and operator 12 are revoking since we revoke in order of operator creation within the task until we are below the memory revoking threshold
        assertMemoryRevokingRequestedFor(operatorContext11, operatorContext12);

        operatorContext11.localRevocableMemoryContext().setBytes(2);
        operatorContext11.resetMemoryRevokingRequested();
        operatorContext12.localRevocableMemoryContext().setBytes(2);
        operatorContext12.resetMemoryRevokingRequested();
        // at this point, Task1 = 4 total bytes, Task2 = 2 total bytes

        awaitTaskThresholdAsynchronousCallbacksRun();
        assertMemoryRevokingNotRequested(); // no need to revoke

        operatorContext2.localRevocableMemoryContext().setBytes(6);
        // at this point, Task1 = 4 total bytes, Task2 = 6 total bytes, operators in Task2 must be revoked
        awaitTaskThresholdAsynchronousCallbacksRun();
        assertMemoryRevokingRequestedFor(operatorContext2);
    }

    @Test
    public void testQueryMemoryRevoking()
            throws Exception
    {
        // The various tasks created here use a small amount of system memory independent of what's set explicitly
        // in this test. Triggering spilling based on differences of thousands of bytes rather than hundreds
        // makes the test resilient to any noise that creates.

        // There can still be a race condition where some of these allocations are made when the total memory is above
        // the spill threshold, but in revokeMemory() some memory is reduced between when we get the total memory usage
        // and when we get the task memory usage.  This can cause some extra spilling.
        // To prevent flakiness in the test, we reset revoke memory requested for all operators, even if only one spilled.

        QueryId queryId = new QueryId("query");
        // use a larger memory pool so that we don't trigger spilling due to filling the memory pool
        MemoryPool queryLimitMemoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(100, GIGABYTE));
        SqlTask sqlTask1 = newSqlTask(queryId, queryLimitMemoryPool);
        TestOperatorContext operatorContext11 = createTestingOperatorContexts(sqlTask1, "operator11");
        TestOperatorContext operatorContext12 = createTestingOperatorContexts(sqlTask1, "operator12");

        SqlTask sqlTask2 = newSqlTask(queryId, queryLimitMemoryPool);
        TestOperatorContext operatorContext2 = createTestingOperatorContexts(sqlTask2, "operator2");

        allOperatorContexts = ImmutableSet.of(operatorContext11, operatorContext12, operatorContext2);
        List<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(
                singletonList(queryLimitMemoryPool),
                () -> tasks,
                queryContexts::get,
                1.0,
                1.0,
                ORDER_BY_REVOCABLE_BYTES,
                true);
        try {
            scheduler.start();

            assertMemoryRevokingNotRequested();

            operatorContext11.localRevocableMemoryContext().setBytes(150_000);
            operatorContext2.localRevocableMemoryContext().setBytes(100_000);
            // at this point, Task1 = 150k total bytes, Task2 = 100k total bytes

            // this ensures that we are waiting for the memory revocation listener and not using polling-based revoking
            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingNotRequested();

            operatorContext12.localRevocableMemoryContext().setBytes(300_000);
            // at this point, Task1 =  450k total bytes, Task2 = 100k total bytes

            scheduler.awaitAsynchronousCallbacksRun();
            // only operator11 should revoke since we need to revoke only 50k bytes
            // limit - (task1 + task2) => 500k - (450k + 100k) = 50k byte to revoke
            assertMemoryRevokingRequestedFor(operatorContext11);

            // revoke all bytes in operator11
            operatorContext11.localRevocableMemoryContext().setBytes(0);
            // at this point, Task1 = 300k total bytes, Task2 = 100k total bytes
            scheduler.awaitAsynchronousCallbacksRun();
            operatorContext11.resetMemoryRevokingRequested();
            operatorContext12.resetMemoryRevokingRequested();
            operatorContext2.resetMemoryRevokingRequested();
            assertMemoryRevokingNotRequested();

            operatorContext11.localRevocableMemoryContext().setBytes(20_000);
            // at this point, Task1 = 320,000 total bytes (oc11 - 20k, oc12 - 300k), Task2 = 100k total bytes
            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingNotRequested();

            operatorContext2.localSystemMemoryContext().setBytes(150_000);
            // at this point, Task1 = 320K total bytes, Task2 = 250K total bytes
            // both operator11 and operator 12 are revoking since we revoke in order of operator creation within the task until we are below the memory revoking threshold
            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingRequestedFor(operatorContext11, operatorContext12);

            operatorContext11.localRevocableMemoryContext().setBytes(0);
            operatorContext12.localRevocableMemoryContext().setBytes(0);
            scheduler.awaitAsynchronousCallbacksRun();
            operatorContext11.resetMemoryRevokingRequested();
            operatorContext12.resetMemoryRevokingRequested();
            operatorContext2.resetMemoryRevokingRequested();
            assertMemoryRevokingNotRequested();

            operatorContext11.localRevocableMemoryContext().setBytes(50_000);
            operatorContext12.localRevocableMemoryContext().setBytes(50_000);
            operatorContext2.localSystemMemoryContext().setBytes(150_000);
            operatorContext2.localRevocableMemoryContext().setBytes(150_000);
            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingNotRequested(); // no need to revoke
            // at this point, Task1 = 75k total bytes, Task2 = 300k total bytes (150k revocable, 150k system)

            operatorContext12.localUserMemoryContext().setBytes(300_000);
            // at this point, Task1 = 400K total bytes (100k revocable, 300k user), Task2 = 300k total bytes (150k revocable, 150k system)
            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingRequestedFor(operatorContext2, operatorContext11);
        }
        finally {
            scheduler.stop();
        }
    }

    @Test
    public void testRevokesPoolWhenFullBeforeQueryLimit()
            throws Exception
    {
        QueryContext q1 = getOrCreateQueryContext(new QueryId("q1"), memoryPool);
        QueryContext q2 = getOrCreateQueryContext(new QueryId("q2"), memoryPool);

        SqlTask sqlTask1 = newSqlTask(q1.getQueryId(), memoryPool);
        SqlTask sqlTask2 = newSqlTask(q2.getQueryId(), memoryPool);

        TaskContext taskContext1 = getOrCreateTaskContext(sqlTask1);
        PipelineContext pipelineContext11 = taskContext1.addPipelineContext(0, false, false, false);
        DriverContext driverContext111 = pipelineContext11.addDriverContext();
        OperatorContext operatorContext1 = driverContext111.addOperatorContext(1, new PlanNodeId("na"), "na");
        OperatorContext operatorContext2 = driverContext111.addOperatorContext(2, new PlanNodeId("na"), "na");
        DriverContext driverContext112 = pipelineContext11.addDriverContext();
        OperatorContext operatorContext3 = driverContext112.addOperatorContext(3, new PlanNodeId("na"), "na");

        TaskContext taskContext2 = getOrCreateTaskContext(sqlTask2);
        PipelineContext pipelineContext21 = taskContext2.addPipelineContext(1, false, false, false);
        DriverContext driverContext211 = pipelineContext21.addDriverContext();
        OperatorContext operatorContext4 = driverContext211.addOperatorContext(4, new PlanNodeId("na"), "na");

        List<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(
                singletonList(memoryPool),
                () -> tasks,
                queryContexts::get,
                1.0,
                1.0,
                ORDER_BY_CREATE_TIME,
                true);
        try {
            scheduler.start();

            allOperatorContexts = ImmutableSet.of(operatorContext1, operatorContext2, operatorContext3, operatorContext4);
            assertMemoryRevokingNotRequested();

            assertEquals(10, memoryPool.getFreeBytes());
            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingNotRequested();

            LocalMemoryContext revocableMemory1 = operatorContext1.localRevocableMemoryContext();
            LocalMemoryContext revocableMemory3 = operatorContext3.localRevocableMemoryContext();
            LocalMemoryContext revocableMemory4 = operatorContext4.localRevocableMemoryContext();

            revocableMemory1.setBytes(3);
            revocableMemory3.setBytes(6);
            assertEquals(1, memoryPool.getFreeBytes());
            scheduler.awaitAsynchronousCallbacksRun();
            // we are still good - no revoking needed
            assertMemoryRevokingNotRequested();

            revocableMemory4.setBytes(7);
            assertEquals(-6, memoryPool.getFreeBytes());
            scheduler.awaitAsynchronousCallbacksRun();
            // we need to revoke 3 and 6
            assertMemoryRevokingRequestedFor(operatorContext1, operatorContext3);
        }
        finally {
            scheduler.stop();
        }
    }

    @Test
    public void testQueryMemoryNotRevokedWhenNotEnabled()
            throws Exception
    {
        // The various tasks created here use a small amount of system memory independent of what's set explicitly
        // in this test. Triggering spilling based on differences of thousands of bytes rather than hundreds
        // makes the test resilient to any noise that creates.

        QueryId queryId = new QueryId("query");
        // use a larger memory pool so that we don't trigger spilling due to filling the memory pool
        MemoryPool queryLimitMemoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(100, GIGABYTE));
        SqlTask sqlTask1 = newSqlTask(queryId, queryLimitMemoryPool);
        TestOperatorContext operatorContext11 = createTestingOperatorContexts(sqlTask1, "operator11");

        allOperatorContexts = ImmutableSet.of(operatorContext11);
        List<SqlTask> tasks = ImmutableList.of(sqlTask1);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(
                singletonList(queryLimitMemoryPool),
                () -> tasks,
                queryContexts::get,
                1.0,
                1.0,
                ORDER_BY_REVOCABLE_BYTES,
                false);
        try {
            scheduler.start();

            assertMemoryRevokingNotRequested();

            // exceed the query memory limit of 500KB
            operatorContext11.localRevocableMemoryContext().setBytes(600_000);
            scheduler.awaitAsynchronousCallbacksRun();
            assertMemoryRevokingNotRequested();
        }
        finally {
            scheduler.stop();
        }
    }

    private OperatorContext createContexts(SqlTask sqlTask)
    {
        TaskContext taskContext = getOrCreateTaskContext(sqlTask);
        PipelineContext pipelineContext = taskContext.addPipelineContext(0, false, false, false);
        DriverContext driverContext = pipelineContext.addDriverContext();
        return driverContext.addOperatorContext(1, new PlanNodeId("na"), "na");
    }

    private TestOperatorContext createTestingOperatorContexts(SqlTask sqlTask, String operatorName)
    {
        // update task to update underlying taskHolderReference with taskExecution + create a new taskContext
        sqlTask.updateTask(TEST_SESSION,
                Optional.of(PLAN_FRAGMENT),
                ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, ImmutableSet.of(SPLIT), false)),
                createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds(),
                Optional.of(new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty())));

        // use implicitly created task context from updateTask. It should be the only task in this QueryContext's tasks
        TaskContext taskContext = sqlTask.getQueryContext().getTaskContextByTaskId(sqlTask.getTaskId());
        PipelineContext pipelineContext = taskContext.addPipelineContext(0, false, false, false);
        DriverContext driverContext = pipelineContext.addDriverContext();
        TestOperatorContext testOperatorContext = new TestOperatorContext(
                1,
                new PlanNodeId("na"),
                "na",
                driverContext,
                singleThreadedExecutor,
                driverContext.getDriverMemoryContext().newMemoryTrackingContext(),
                operatorName);
        driverContext.addOperatorContext(testOperatorContext);
        return testOperatorContext;
    }

    private static class TestOperatorContext
            extends OperatorContext
    {
        public static String firstOperator;
        private final String operatorName;

        public TestOperatorContext(
                int operatorId,
                PlanNodeId planNodeId,
                String operatorType,
                DriverContext driverContext,
                Executor executor,
                MemoryTrackingContext operatorMemoryContext,
                String operatorName)
        {
            super(operatorId, planNodeId, operatorType, driverContext, executor, operatorMemoryContext);
            this.operatorName = operatorName;
        }

        @Override
        public long requestMemoryRevoking()
        {
            if (firstOperator == null) {
                // Due to the way MemoryRevokingScheduler works, revoking tasks one by one, simultaneous revoke of two tasks is impossible
                // This is why updating this static member is safe
                firstOperator = operatorName;
            }
            return super.requestMemoryRevoking();
        }
    }

    private void requestMemoryRevoking(TaskThresholdMemoryRevokingScheduler scheduler)
            throws Exception
    {
        scheduler.revokeHighMemoryTasksIfNeeded();
        awaitTaskThresholdAsynchronousCallbacksRun();
    }

    private void awaitTaskThresholdAsynchronousCallbacksRun()
            throws Exception
    {
        // Make sure asynchronous callback got called (executor is single-threaded).
        singleThreadedScheduledExecutor.invokeAll(singletonList((Callable<?>) () -> null));
    }

    private void assertMemoryRevokingRequestedFor(OperatorContext... operatorContexts)
    {
        ImmutableSet<OperatorContext> operatorContextsSet = ImmutableSet.copyOf(operatorContexts);
        operatorContextsSet.forEach(
                operatorContext -> assertTrue(operatorContext.isMemoryRevokingRequested(), "expected memory requested for operator " + operatorContext.getOperatorId()));
        Sets.difference(allOperatorContexts, operatorContextsSet).forEach(
                operatorContext -> assertFalse(operatorContext.isMemoryRevokingRequested(), "expected memory  not requested for operator " + operatorContext.getOperatorId()));
    }

    private void assertMemoryRevokingNotRequested()
    {
        assertMemoryRevokingRequestedFor();
    }

    private SqlTask newSqlTask(QueryId queryId, MemoryPool memoryPool)
    {
        QueryContext queryContext = getOrCreateQueryContext(queryId, memoryPool);

        TaskId taskId = new TaskId(queryId.getId(), 0, 0, idGeneator.incrementAndGet());
        URI location = URI.create("fake://task/" + taskId);

        return createSqlTask(
                taskId,
                location,
                "fake",
                queryContext,
                sqlTaskExecutionFactory,
                new MockExchangeClientSupplier(),
                singleThreadedExecutor,
                Functions.identity(),
                new DataSize(32, MEGABYTE),
                new CounterStat(),
                new SpoolingOutputBufferFactory(new FeaturesConfig()));
    }

    private QueryContext getOrCreateQueryContext(QueryId queryId, MemoryPool memoryPool)
    {
        return queryContexts.computeIfAbsent(queryId, id -> new QueryContext(id,
                new DataSize(500, KILOBYTE),
                new DataSize(500, KILOBYTE),
                new DataSize(500, KILOBYTE),
                new DataSize(1, GIGABYTE),
                memoryPool,
                new TestingGcMonitor(),
                singleThreadedExecutor,
                scheduledExecutor,
                new DataSize(1, GIGABYTE),
                spillSpaceTracker,
                listJsonCodec(TaskMemoryReservationSummary.class)));
    }

    private TaskContext getOrCreateTaskContext(SqlTask sqlTask)
    {
        if (!sqlTask.getTaskContext().isPresent()) {
            // update task to update underlying taskHolderReference with taskExecution + create a new taskContext
            updateTask(sqlTask, ImmutableList.of(), createInitialEmptyOutputBuffers(PARTITIONED).withBuffer(OUT, 0).withNoMoreBufferIds());
        }
        return sqlTask.getTaskContext().orElseThrow(() -> new IllegalStateException("TaskContext not present"));
    }
}
