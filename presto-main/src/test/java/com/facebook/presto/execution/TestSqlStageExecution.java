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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.scheduler.SplitSchedulerStats;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.failureDetector.NoOpFailureDetector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.split.MockSplitSource;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.SqlStageExecution.createSqlStageExecution;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.spi.StandardErrorCode.HOST_SHUTTING_DOWN;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestSqlStageExecution
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterClass
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test(timeOut = 3 * 60 * 1000)
    public void testFinalStageInfo()
            throws Exception
    {
        // run test a few times to catch any race conditions
        // this is not done with TestNG invocation count so there can be a global time limit on the test
        for (int iteration = 0; iteration < 10; iteration++) {
            testFinalStageInfoInternal();
        }
    }

    private void testFinalStageInfoInternal()
            throws Exception
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStageExecution stage = createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                createExchangePlanFragment(),
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()));
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        // add listener that fetches stage info when the final status is available
        SettableFuture<StageExecutionInfo> finalStageInfo = SettableFuture.create();
        stage.addFinalStageInfoListener(finalStageInfo::set);

        // in a background thread add a ton of tasks
        CountDownLatch latch = new CountDownLatch(1000);
        Future<?> addTasksTask = executor.submit(() -> {
            try {
                for (int i = 0; i < 1_000_000; i++) {
                    if (Thread.interrupted()) {
                        return;
                    }
                    InternalNode node = new InternalNode(
                            "source" + i,
                            URI.create("http://10.0.0." + (i / 10_000) + ":" + (i % 10_000)),
                            NodeVersion.UNKNOWN,
                            false);
                    stage.scheduleTask(node, i);
                    latch.countDown();
                }
            }
            finally {
                while (latch.getCount() > 0) {
                    latch.countDown();
                }
            }
        });

        // wait for some tasks to be created, and then abort the query
        latch.await(1, MINUTES);
        assertFalse(stage.getStageExecutionInfo().getTasks().isEmpty());
        stage.abort();

        // once the final stage info is available, verify that it is complete
        StageExecutionInfo stageInfo = finalStageInfo.get(1, MINUTES);
        assertFalse(stageInfo.getTasks().isEmpty());
        assertTrue(stageInfo.isFinal());
        assertSame(stage.getStageExecutionInfo(), stageInfo);

        // cancel the background thread adding tasks
        addTasksTask.cancel(true);
    }

    private static PlanFragment createExchangePlanFragment()
    {
        PlanNode planNode = new RemoteSourceNode(
                Optional.empty(),
                new PlanNodeId("exchange"),
                ImmutableList.of(new PlanFragmentId(0)),
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "column", VARCHAR)),
                false,
                Optional.empty(),
                REPARTITION);

        return new PlanFragment(
                new PlanFragmentId(0),
                planNode,
                ImmutableSet.copyOf(planNode.getOutputVariables()),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputVariables()),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());
    }

    @Test(timeOut = 3 * 60 * 1000)
    public void testNoMoreRetryOnNoFailedTask()
            throws Exception
    {
        // run test a few times to catch any race conditions
        // this is not done with TestNG invocation count so there can be a global time limit on the test
        for (int iteration = 0; iteration < 10; iteration++) {
            testNoMoreRetryOnNoFailedTaskInternal();
        }
    }

    private void testNoMoreRetryOnNoFailedTaskInternal()
            throws Exception
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStageExecution stage = createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                createExchangePlanFragment(),
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()));
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        // add listener that fetches stage info when the final status is available
        SettableFuture<StageExecutionInfo> finalStageInfo = SettableFuture.create();
        stage.addFinalStageInfoListener(finalStageInfo::set);

        stage.registerStageTaskRecoveryCallback((taskId, executionFailureInfos) -> {}, ImmutableSet.of(HOST_SHUTTING_DOWN.toErrorCode()));

        ArrayList<RemoteTask> allTasks = new ArrayList<RemoteTask>();
        for (int i = 0; i < 2; i++) {
            InternalNode node = new InternalNode(
                    "source" + i,
                    URI.create("http://10.0.0." + i + ":" + i),
                    NodeVersion.UNKNOWN,
                    false);
            Optional<RemoteTask> t = stage.scheduleTask(node, i);
            t.ifPresent(allTasks::add);
        }

        for (RemoteTask t : allTasks) {
            ((MockRemoteTaskFactory.MockRemoteTask) t).markAllSplitsRun();
        }

        assertFalse(stage.getStageExecutionInfo().getTasks().isEmpty());
        assertTrue(stage.noMoreRetry());
    }

    @Test(timeOut = 3 * 60 * 1000)
    public void testNoMoreRetryOnOneFailedTask()
            throws Exception
    {
        // run test a few times to catch any race conditions
        // this is not done with TestNG invocation count so there can be a global time limit on the test
        for (int iteration = 0; iteration < 10; iteration++) {
            testNoMoreRetryOnOneFailedTaskInternal();
        }
    }

    @SuppressWarnings("checkstyle:EmptyBlock")
    private void testNoMoreRetryOnOneFailedTaskInternal()
            throws Exception
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        PlanNode planNode = new RemoteSourceNode(
                Optional.empty(),
                new PlanNodeId("exchange"),
                ImmutableList.of(new PlanFragmentId(0)),
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "column", VARCHAR)),
                false,
                Optional.empty(),
                REPARTITION);

        PlanFragment plan = new PlanFragment(
                    new PlanFragmentId(0),
                    planNode,
                    ImmutableSet.copyOf(planNode.getOutputVariables()),
                    SOURCE_DISTRIBUTION,
                    ImmutableList.of(planNode.getId()),
                    new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputVariables()),
                    StageExecutionDescriptor.ungroupedExecution(),
                    false,
                    StatsAndCosts.empty(),
                    Optional.empty());
        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStageExecution stage = createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                plan,
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()));
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        // add listener that fetches stage info when the final status is available
        SettableFuture<StageExecutionInfo> finalStageInfo = SettableFuture.create();
        stage.addFinalStageInfoListener(finalStageInfo::set);

        ArrayList<RemoteTask> allTasks = new ArrayList<RemoteTask>();
        for (int i = 0; i < 2; i++) {
            InternalNode node = new InternalNode(
                    "source" + i,
                    URI.create("http://10.0.0." + i + ":" + i),
                    NodeVersion.UNKNOWN,
                    false);
            Optional<RemoteTask> ot = stage.scheduleTask(node, i);

            if (ot.isPresent()) {
                RemoteTask t = ot.get();
                allTasks.add(t);

                ImmutableMultimap.Builder<PlanNodeId, Split> splits = ImmutableMultimap.builder();
                splits.put(planNode.getId(), new Split(new ConnectorId("test"), new ConnectorTransactionHandle() {}, new MockSplitSource.MockConnectorSplit()));
                t.addSplits(splits.build());
            }
        }

        AtomicBoolean recoveryIsRun = new AtomicBoolean(false);
        stage.registerStageTaskRecoveryCallback((taskId, executionFailureInfos) -> {
            RemoteTask remoteTask = stage.getAllTasks().stream()
                    .filter(task -> task.getTaskId().equals(taskId))
                    .collect(onlyElement());

            ImmutableMultimap.Builder<PlanNodeId, Split> splits = ImmutableMultimap.builder();
            for (ScheduledSplit s : remoteTask.getAllSplits(planNode.getId())) {
                splits.put(planNode.getId(), s.getSplit());
            }
            remoteTask.addSplits(splits.build());
            recoveryIsRun.set(true);
        }, ImmutableSet.of(HOST_SHUTTING_DOWN.toErrorCode()));

        MockRemoteTaskFactory.MockRemoteTask firstTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(0);
        firstTask.failed();

        assertFalse(stage.getStageExecutionInfo().getTasks().isEmpty());
        assertFalse(stage.noMoreRetry());

        while (!recoveryIsRun.get());

        MockRemoteTaskFactory.MockRemoteTask secondTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(1);
        secondTask.markAllSplitsRun();

        assertTrue(stage.noMoreRetry());
    }

    @Test(timeOut = 3 * 60 * 1000)
    public void testNoMoreRetryOnExceedingFailurePercentage()
            throws Exception
    {
        // run test a few times to catch any race conditions
        // this is not done with TestNG invocation count so there can be a global time limit on the test
        for (int iteration = 0; iteration < 10; iteration++) {
            testNoMoreRetryOnExceedingFailurePercentageInternal();
        }
    }

    private void testNoMoreRetryOnExceedingFailurePercentageInternal()
            throws Exception
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(new FinalizerService());

        PlanNode planNode = new RemoteSourceNode(
                Optional.empty(),
                new PlanNodeId("exchange"),
                ImmutableList.of(new PlanFragmentId(0)),
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "column", VARCHAR)),
                false,
                Optional.empty(),
                REPARTITION);

        Session sessionWithCustomPercentage = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(SystemSessionProperties.MAX_FAILED_TASK_PERCENTAGE, "0.4")
                .build();

        PlanFragment plan = new PlanFragment(
                new PlanFragmentId(0),
                planNode,
                ImmutableSet.copyOf(planNode.getOutputVariables()),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputVariables()),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());
        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStageExecution stage = createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                plan,
                new MockRemoteTaskFactory(executor, scheduledExecutor),
                sessionWithCustomPercentage,
                true,
                nodeTaskMap,
                executor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()));
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));

        // add listener that fetches stage info when the final status is available
        SettableFuture<StageExecutionInfo> finalStageInfo = SettableFuture.create();
        stage.addFinalStageInfoListener(finalStageInfo::set);

        ArrayList<RemoteTask> allTasks = new ArrayList<RemoteTask>();
        for (int i = 0; i < 2; i++) {
            InternalNode node = new InternalNode(
                    "source" + i,
                    URI.create("http://10.0.0." + i + ":" + i),
                    NodeVersion.UNKNOWN,
                    false);
            Optional<RemoteTask> ot = stage.scheduleTask(node, i);

            if (ot.isPresent()) {
                RemoteTask t = ot.get();
                allTasks.add(t);

                ImmutableMultimap.Builder<PlanNodeId, Split> splits = ImmutableMultimap.builder();
                splits.put(planNode.getId(), new Split(new ConnectorId("test"), new ConnectorTransactionHandle() {}, new MockSplitSource.MockConnectorSplit()));
                t.addSplits(splits.build());
            }
        }

        stage.registerStageTaskRecoveryCallback((taskId, executionFailureInfos) -> {}, ImmutableSet.of(HOST_SHUTTING_DOWN.toErrorCode()));

        MockRemoteTaskFactory.MockRemoteTask firstTask = (MockRemoteTaskFactory.MockRemoteTask) allTasks.get(0);
        firstTask.failed();

        assertFalse(stage.getStageExecutionInfo().getTasks().isEmpty());
        assertTrue(stage.noMoreRetry());
    }
}
