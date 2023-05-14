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
package com.facebook.presto.spark.task;

import com.facebook.presto.Session;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.execution.TaskTestUtils;
import com.facebook.presto.execution.executor.TaskExecutor;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spark.execution.PrestoSparkTaskExecution;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.TestingOutputBuffer;
import com.facebook.presto.sql.planner.TestingRemoteSourceFactory;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTaskContext;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.execution.TaskManagerConfig.TaskPriorityTracking.TASK_FAIR;
import static com.facebook.presto.execution.TaskTestUtils.TABLE_SCAN_NODE_ID;
import static com.facebook.presto.execution.TaskTestUtils.createPlanFragment;
import static com.facebook.presto.execution.TaskTestUtils.createTestingPlanner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestPrestoSparkTaskExecution
{
    Session nativeTestSession;
    Session nonNativeTestSession;
    LocalExecutionPlanner.LocalExecutionPlan localExecutionPlan;
    TaskExecutor taskExecutor;
    PlanFragment planFragment = createPlanFragment();
    ExecutorService taskNotificationExecutor;
    ScheduledExecutorService scheduledExecutor;
    TaskStateMachine taskStateMachine;

    Set<ScheduledSplit> splits = ImmutableSet.of(
            new ScheduledSplit(1, TABLE_SCAN_NODE_ID, new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit())),
            new ScheduledSplit(2, TABLE_SCAN_NODE_ID, new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit())),
            new ScheduledSplit(3, TABLE_SCAN_NODE_ID, new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit())));

    @BeforeMethod
    public void setUp()
    {
        taskNotificationExecutor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        taskExecutor = new TaskExecutor(8, 16, 3, 4, TASK_FAIR, Ticker.systemTicker());

        nativeTestSession = testSessionBuilder()
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();
        nonNativeTestSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        taskStateMachine = new TaskStateMachine(new TaskId("test_query_id", 4, 4, 1, 0), taskNotificationExecutor);

        localExecutionPlan = createTestingPlanner().plan(
                TestingTaskContext.createTaskContext(taskNotificationExecutor, scheduledExecutor, nativeTestSession),
                planFragment,
                new TestingOutputBuffer(),
                new TestingRemoteSourceFactory(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()),
                new ArrayList<>());
    }

    @AfterMethod
    public void tearDown()
    {
        taskStateMachine.finished();
        taskExecutor.stop();
        scheduledExecutor.shutdown();
        taskNotificationExecutor.shutdown();
    }

    @Test
    public void testNativeDriverInstanceCount()
    {
        testDriverCount(nativeTestSession, true, 1);
    }

    @Test
    public void testJavaDriverInstanceCount()
    {
        testDriverCount(nonNativeTestSession, false, 3);
    }

    private void testDriverCount(Session session, boolean isNative, int expectedDriverCount)
    {
        TaskContext taskContext = TestingTaskContext.createTaskContext(taskNotificationExecutor, scheduledExecutor, session, new DataSize(2, GIGABYTE));
        taskExecutor.start();
        PrestoSparkTaskExecution taskExecution = new PrestoSparkTaskExecution(taskStateMachine, taskContext, localExecutionPlan, taskExecutor, TaskTestUtils.createTestSplitMonitor(), taskNotificationExecutor, scheduledExecutor, isNative);
        taskExecution.start(ImmutableList.of(new TaskSource(TABLE_SCAN_NODE_ID, splits, true)));
        assertEquals(taskContext.getPipelineContexts().get(0).getPipelineStats().getDrivers().size(), expectedDriverCount);
    }
}
