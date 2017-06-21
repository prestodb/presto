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

import com.facebook.presto.Session;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.event.query.QueryMonitorConfig;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.memory.QueryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PipelineContext;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.TestingSession;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.execution.TaskTestUtils.createTestingPlanner;
import static com.facebook.presto.memory.LocalMemoryManager.GENERAL_POOL;
import static com.facebook.presto.memory.LocalMemoryManager.SYSTEM_POOL;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMemoryRevokingScheduler
{
    private final AtomicInteger idGeneator = new AtomicInteger();
    private final Session session = TestingSession.testSessionBuilder().build();
    private final SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(new DataSize(10, GIGABYTE));

    private ScheduledExecutorService executor;
    private SqlTaskExecutionFactory sqlTaskExecutionFactory;
    private MemoryPool memoryPool;

    private Set<OperatorContext> allOperatorContexts;

    @BeforeMethod
    public void setUp()
    {
        memoryPool = new MemoryPool(GENERAL_POOL, new DataSize(10, BYTE));

        TaskExecutor taskExecutor = new TaskExecutor(8, 16);
        taskExecutor.start();
        executor = newScheduledThreadPool(10, threadsNamed("task-notification-%s"));

        LocalExecutionPlanner planner = createTestingPlanner();

        sqlTaskExecutionFactory = new SqlTaskExecutionFactory(
                executor,
                taskExecutor,
                planner,
                new QueryMonitor(new ObjectMapperProvider().get(), jsonCodec(StageInfo.class), new EventListenerManager(), new NodeInfo("test"), new NodeVersion("testVersion"), new QueryMonitorConfig()),
                new TaskManagerConfig());

        allOperatorContexts = null;
    }

    @AfterMethod
    public void tearDown()
    {
        memoryPool = null;
        executor.shutdownNow();
    }

    @Test
    public void testScheduleMemoryRevoking()
            throws Exception
    {
        SqlTask sqlTask1 = newSqlTask();
        SqlTask sqlTask2 = newSqlTask();

        TaskContext taskContext1 = sqlTask1.getQueryContext().addTaskContext(new TaskStateMachine(new TaskId("q1", 1, 1), executor), session, false, false);
        PipelineContext pipelineContext11 = taskContext1.addPipelineContext(0, false, false);
        DriverContext driverContext111 = pipelineContext11.addDriverContext();
        OperatorContext operatorContext1 = driverContext111.addOperatorContext(1, new PlanNodeId("na"), "na");
        OperatorContext operatorContext2 = driverContext111.addOperatorContext(2, new PlanNodeId("na"), "na");
        DriverContext driverContext112 = pipelineContext11.addDriverContext();
        OperatorContext operatorContext3 = driverContext112.addOperatorContext(3, new PlanNodeId("na"), "na");

        TaskContext taskContext2 = sqlTask2.getQueryContext().addTaskContext(new TaskStateMachine(new TaskId("q2", 1, 1), executor), session, false, false);
        PipelineContext pipelineContext21 = taskContext2.addPipelineContext(1, false, false);
        DriverContext driverContext211 = pipelineContext21.addDriverContext();
        OperatorContext operatorContext4 = driverContext211.addOperatorContext(4, new PlanNodeId("na"), "na");
        OperatorContext operatorContext5 = driverContext211.addOperatorContext(5, new PlanNodeId("na"), "na");

        Collection<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(singletonList(memoryPool), () -> tasks, executor, 1.0, 1.0);

        allOperatorContexts = ImmutableSet.of(operatorContext1, operatorContext2, operatorContext3, operatorContext4, operatorContext5);
        assertMemoryRevokingNotRequested();

        scheduler.requestMemoryRevokingIfNeeded(tasks);
        assertEquals(10, memoryPool.getFreeBytes());
        assertMemoryRevokingNotRequested();

        operatorContext1.setRevocableMemoryReservation(3);
        operatorContext3.setRevocableMemoryReservation(6);
        assertEquals(1, memoryPool.getFreeBytes());
        scheduler.requestMemoryRevokingIfNeeded(tasks);
        // we are still good - no revoking needed
        assertMemoryRevokingNotRequested();

        operatorContext4.setRevocableMemoryReservation(7);
        assertEquals(-6, memoryPool.getFreeBytes());
        scheduler.requestMemoryRevokingIfNeeded(tasks);
        // we need to revoke 3 and 6
        assertMemoryRevokingRequestedFor(operatorContext1, operatorContext3);

        // yet another revoking request should not change anything
        scheduler.requestMemoryRevokingIfNeeded(tasks);
        assertMemoryRevokingRequestedFor(operatorContext1, operatorContext3);

        // lets revoke some bytes
        operatorContext1.setRevocableMemoryReservation(0);
        operatorContext1.resetMemoryRevokingRequested();
        scheduler.requestMemoryRevokingIfNeeded(tasks);
        assertMemoryRevokingRequestedFor(operatorContext3);
        assertEquals(-3, memoryPool.getFreeBytes());

        // and allocate some more
        operatorContext5.setRevocableMemoryReservation(3);
        assertEquals(-6, memoryPool.getFreeBytes());
        scheduler.requestMemoryRevokingIfNeeded(tasks);
        // we are still good with just OC3 in process of revoking
        assertMemoryRevokingRequestedFor(operatorContext3);

        // and allocate some more
        operatorContext5.setRevocableMemoryReservation(4);
        assertEquals(-7, memoryPool.getFreeBytes());
        scheduler.requestMemoryRevokingIfNeeded(tasks);
        // no we have to trigger revoking for OC4
        assertMemoryRevokingRequestedFor(operatorContext3, operatorContext4);
    }

    @Test
    public void testCountAlreadyRevokedMemoryWithinAPool()
            throws Exception
    {
        // Given
        SqlTask sqlTask1 = newSqlTask();
        MemoryPool anotherMemoryPool = new MemoryPool(SYSTEM_POOL, new DataSize(10, BYTE));
        sqlTask1.getQueryContext().setMemoryPool(anotherMemoryPool);
        OperatorContext operatorContext1 = createContexts(sqlTask1);

        SqlTask sqlTask2 = newSqlTask();
        OperatorContext operatorContext2 = createContexts(sqlTask2);

        List<SqlTask> tasks = ImmutableList.of(sqlTask1, sqlTask2);
        MemoryRevokingScheduler scheduler = new MemoryRevokingScheduler(asList(memoryPool, anotherMemoryPool), () -> tasks, executor, 1.0, 1.0);
        allOperatorContexts = ImmutableSet.of(operatorContext1, operatorContext2);

        /*
         * sqlTask1 fills its pool
         */
        operatorContext1.setRevocableMemoryReservation(12);
        scheduler.requestMemoryRevokingIfNeeded(tasks);
        assertMemoryRevokingRequestedFor(operatorContext1);

        /*
         * When sqlTask2 fills its pool
         */
        operatorContext2.setRevocableMemoryReservation(12);
        scheduler.requestMemoryRevokingIfNeeded(tasks);

        /*
         * Then sqlTask2 should be asked to revoke its memory too
         */
        assertMemoryRevokingRequestedFor(operatorContext1, operatorContext2);
    }

    private OperatorContext createContexts(SqlTask sqlTask)
    {
        TaskContext taskContext = sqlTask.getQueryContext().addTaskContext(new TaskStateMachine(new TaskId("q", 1, 1), executor), session, false, false);
        PipelineContext pipelineContext = taskContext.addPipelineContext(0, false, false);
        DriverContext driverContext = pipelineContext.addDriverContext();
        OperatorContext operatorContext = driverContext.addOperatorContext(1, new PlanNodeId("na"), "na");

        return operatorContext;
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

    private SqlTask newSqlTask()
    {
        TaskId taskId = new TaskId("query", 0, idGeneator.incrementAndGet());
        URI location = URI.create("fake://task/" + taskId);

        return new SqlTask(
                taskId,
                location,
                new QueryContext(new QueryId("query"), new DataSize(1, MEGABYTE), memoryPool, new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE)), executor, new DataSize(1, GIGABYTE), spillSpaceTracker),
                sqlTaskExecutionFactory,
                executor,
                Functions.<SqlTask>identity(),
                new DataSize(32, MEGABYTE));
    }
}
