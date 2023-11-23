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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.dispatcher.NoOpQueryManager;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.MockRemoteTaskFactory;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.PartitionedSplitsInfo;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TestSqlTaskManager;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelectorConfig;
import com.facebook.presto.failureDetector.NoOpFailureDetector;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.split.ConnectorAwareSplitSource;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingSession;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.ttl.nodettlfetchermanagers.ThrowingNodeTtlFetcherManager;
import com.facebook.presto.util.FinalizerService;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static java.lang.Integer.min;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSplitRetrySourcePartitionedScheduler
{
    public static final OutputBuffers.OutputBufferId OUT = new OutputBuffers.OutputBufferId(0);
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("connector_id");
    private static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("plan_id");

    private final ExecutorService queryExecutor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("stageScheduledExecutor-%s"));
    private final LocationFactory locationFactory = new TestSqlTaskManager.MockLocationFactory();
    private final InMemoryNodeManager nodeManager = new InMemoryNodeManager();
    private final FinalizerService finalizerService = new FinalizerService();

    public TestSplitRetrySourcePartitionedScheduler()
    {
        nodeManager.addNode(CONNECTOR_ID,
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
    }

    @BeforeClass
    public void setUp()
    {
        finalizerService.start();
    }

    @AfterClass(alwaysRun = true)
    public void destroyExecutor()
    {
        queryExecutor.shutdownNow();
        scheduledExecutor.shutdownNow();
        finalizerService.destroy();
    }

    @Test
    public void testScheduleNoSplits()
    {
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSplitRetryPartitionedScheduler(createFixedSplitSource(0, TestingSplit::createRemoteSplit), stage, nodeTaskMap, nodeManager, 1);

        ScheduleResult scheduleResult = scheduler.schedule();

        assertEquals(scheduleResult.getNewTasks().size(), 1);
        assertEffectivelyFinished(scheduleResult, scheduler);

        stage.abort();
    }

    @Test
    public void testScheduleSplitsOneAtATime()
    {
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSplitRetryPartitionedScheduler(createFixedSplitSource(60, TestingSplit::createRemoteSplit), stage, nodeTaskMap, nodeManager, 1);

        for (int i = 0; i < 60; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            assertFalse(scheduleResult.isFinished());

            // never blocks
            assertTrue(scheduleResult.getBlocked().isDone());

            // first three splits create new tasks
            assertEquals(scheduleResult.getNewTasks().size(), i < 3 ? 1 : 0);
            assertEquals(stage.getAllTasks().size(), i < 3 ? i + 1 : 3);

            assertPartitionedSplitCount(stage, min(i + 1, 60));
        }

        // One iteration to make finish the SourcePartitionedScheduler
        ScheduleResult scheduleResult = scheduler.schedule();
        assertFalse(scheduleResult.isFinished());

        stage.triggerWhenNoMoreRetryForTest();

        scheduleResult = scheduler.schedule();
        assertTrue(scheduleResult.isFinished());

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 20);
        }

        stage.abort();
    }

    @Test
    public void testScheduleSplitsBatched()
    {
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSplitRetryPartitionedScheduler(createFixedSplitSource(60, TestingSplit::createRemoteSplit), stage, nodeTaskMap, nodeManager, 7);

        for (int i = 0; i <= (60 / 7); i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            assertFalse(scheduleResult.isFinished());

            // never blocks
            assertTrue(scheduleResult.getBlocked().isDone());

            // first three splits create new tasks
            assertEquals(scheduleResult.getNewTasks().size(), i == 0 ? 3 : 0);
            assertEquals(stage.getAllTasks().size(), 3);

            assertPartitionedSplitCount(stage, min((i + 1) * 7, 60));
        }

        // One iteration to make finish the SourcePartitionedScheduler
        ScheduleResult scheduleResult = scheduler.schedule();
        assertFalse(scheduleResult.isFinished());

        stage.triggerWhenNoMoreRetryForTest();

        scheduleResult = scheduler.schedule();
        assertTrue(scheduleResult.isFinished());

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 20);
        }

        stage.abort();
    }

    @Test
    public void testScheduleSplitsBlock()
    {
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSplitRetryPartitionedScheduler(createFixedSplitSource(80, TestingSplit::createRemoteSplit), stage, nodeTaskMap, nodeManager, 1);

        // schedule first 60 splits, which will cause the scheduler to block
        for (int i = 0; i <= 60; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            assertFalse(scheduleResult.isFinished());

            // blocks at 20 per node
            assertEquals(scheduleResult.getBlocked().isDone(), i != 60);

            // first three splits create new tasks
            assertEquals(scheduleResult.getNewTasks().size(), i < 3 ? 1 : 0);
            assertEquals(stage.getAllTasks().size(), i < 3 ? i + 1 : 3);

            assertPartitionedSplitCount(stage, min(i + 1, 60));
        }

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 20);
        }

        // drop the 20 splits from one node
        ((MockRemoteTaskFactory.MockRemoteTask) stage.getAllTasks().get(0)).clearSplits();

        // schedule remaining 20 splits
        for (int i = 0; i < 20; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            assertFalse(scheduleResult.isFinished());

            // does not block again
            assertTrue(scheduleResult.getBlocked().isDone());

            // no additional tasks will be created
            assertEquals(scheduleResult.getNewTasks().size(), 0);
            assertEquals(stage.getAllTasks().size(), 3);

            // we dropped 20 splits so start at 40 and count to 60
            assertPartitionedSplitCount(stage, min(i + 41, 60));
        }

        // One iteration to make finish the SourcePartitionedScheduler
        ScheduleResult scheduleResult = scheduler.schedule();
        assertFalse(scheduleResult.isFinished());

        stage.triggerWhenNoMoreRetryForTest();

        scheduleResult = scheduler.schedule();
        assertTrue(scheduleResult.isFinished());

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            PartitionedSplitsInfo splitsInfo = remoteTask.getPartitionedSplitsInfo();
            assertEquals(splitsInfo.getCount(), 20);
        }

        stage.abort();
    }

    @Test
    public void testScheduleSlowSplitSource()
    {
        TestSourcePartitionedScheduler.QueuedSplitSource queuedSplitSource = new TestSourcePartitionedScheduler.QueuedSplitSource(TestingSplit::createRemoteSplit);
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSplitRetryPartitionedScheduler(queuedSplitSource, stage, nodeTaskMap, nodeManager, 1);

        // schedule with no splits - will block
        ScheduleResult scheduleResult = scheduler.schedule();
        assertFalse(scheduleResult.isFinished());
        assertFalse(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 0);
        assertEquals(stage.getAllTasks().size(), 0);

        queuedSplitSource.addSplits(1);
        assertTrue(scheduleResult.getBlocked().isDone());
    }

    @Test
    public void testNoNodes()
    {
        try {
            NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
            InMemoryNodeManager nodeManager = new InMemoryNodeManager();
            NodeScheduler nodeScheduler = new NodeScheduler(
                    new LegacyNetworkTopology(),
                    nodeManager,
                    new NodeSelectionStats(),
                    new NodeSchedulerConfig().setIncludeCoordinator(false),
                    nodeTaskMap,
                    new ThrowingNodeTtlFetcherManager(),
                    new NoOpQueryManager(),
                    new SimpleTtlNodeSelectorConfig());

            SubPlan plan = createPlan();
            SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

            StageScheduler scheduler = newSourcePartitionedSchedulerAsStageScheduler(
                    stage,
                    TABLE_SCAN_NODE_ID,
                    new ConnectorAwareSplitSource(CONNECTOR_ID, TestingTransactionHandle.create(), createFixedSplitSource(20, TestingSplit::createRemoteSplit)),
                    new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(TestingSession.testSessionBuilder().build(), CONNECTOR_ID), stage::getAllTasks),
                    2);
            scheduler.schedule();

            fail("expected PrestoException");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NO_NODES_AVAILABLE.toErrorCode());
        }
    }
    private static StageScheduler getSplitRetryPartitionedScheduler(
            ConnectorSplitSource connectorSplitSource,
            SqlStageExecution stage,
            NodeTaskMap nodeTaskMap,
            InternalNodeManager nodeManager,
            int splitBatchSize)
    {
        SourcePartitionedScheduler sourcePartitionedScheduler = getSourcePartitionedScheduler(connectorSplitSource, stage, nodeManager, nodeTaskMap, splitBatchSize);
        sourcePartitionedScheduler.startLifespan(Lifespan.taskWide(), NOT_PARTITIONED);
        return new SplitRetrySourcePartitionedScheduler(sourcePartitionedScheduler, stage);
    }

    private static SourcePartitionedScheduler getSourcePartitionedScheduler(
            ConnectorSplitSource connectorSplitSource,
            SqlStageExecution stage,
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            int splitBatchSize)
    {
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setIncludeCoordinator(false)
                .setMaxSplitsPerNode(20)
                .setMaxPendingSplitsPerTask(0);
        NodeScheduler nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                nodeManager,
                new NodeSelectionStats(),
                nodeSchedulerConfig,
                nodeTaskMap,
                new ThrowingNodeTtlFetcherManager(),
                new NoOpQueryManager(),
                new SimpleTtlNodeSelectorConfig());
        SplitSource splitSource = new ConnectorAwareSplitSource(CONNECTOR_ID, TestingTransactionHandle.create(), connectorSplitSource);
        SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(TestingSession.testSessionBuilder().build(), splitSource.getConnectorId()), stage::getAllTasks);
        return new SourcePartitionedScheduler(stage, TABLE_SCAN_NODE_ID, splitSource, placementPolicy, splitBatchSize, false);
    }

    private static SubPlan createPlan()
    {
        VariableReferenceExpression variable = new VariableReferenceExpression(Optional.empty(), "column", VARCHAR);

        // table scan with splitCount splits
        TableScanNode tableScan = new TableScanNode(
                Optional.empty(),
                TABLE_SCAN_NODE_ID,
                new TableHandle(CONNECTOR_ID, new TestingMetadata.TestingTableHandle(), TestingTransactionHandle.create(), Optional.empty()),
                ImmutableList.of(variable),
                ImmutableMap.of(variable, new TestingMetadata.TestingColumnHandle("column")),
                TupleDomain.all(),
                TupleDomain.all());

        RemoteSourceNode remote = new RemoteSourceNode(Optional.empty(), new PlanNodeId("remote_id"), new PlanFragmentId(0), ImmutableList.of(), false, Optional.empty(), GATHER);
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId(0),
                new JoinNode(
                        Optional.empty(),
                        new PlanNodeId("join_id"),
                        INNER,
                        tableScan,
                        remote,
                        ImmutableList.of(),
                        ImmutableList.<VariableReferenceExpression>builder()
                                .addAll(tableScan.getOutputVariables())
                                .addAll(remote.getOutputVariables())
                                .build(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableMap.of()),
                ImmutableSet.of(variable),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(TABLE_SCAN_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(variable)),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());

        return new SubPlan(testFragment, ImmutableList.of());
    }

    private SqlStageExecution createSqlStageExecution(SubPlan tableScanPlan, NodeTaskMap nodeTaskMap)
    {
        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStageExecution stage = SqlStageExecution.createSqlStageExecution(
                new StageExecutionId(stageId, 0),
                tableScanPlan.getFragment(),
                new MockRemoteTaskFactory(queryExecutor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                queryExecutor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()), true, true);

        stage.setOutputBuffers(createInitialEmptyOutputBuffers(PARTITIONED)
                .withBuffer(OUT, 0)
                .withNoMoreBufferIds());

        return stage;
    }

    private static ConnectorSplitSource createFixedSplitSource(int splitCount, Supplier<ConnectorSplit> splitFactory)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (int i = 0; i < splitCount; i++) {
            splits.add(splitFactory.get());
        }
        return new FixedSplitSource(splits.build());
    }

    private static void assertEffectivelyFinished(ScheduleResult scheduleResult, StageScheduler scheduler)
    {
        if (scheduleResult.isFinished()) {
            assertTrue(scheduleResult.getBlocked().isDone());
            return;
        }

        assertTrue(scheduleResult.getBlocked().isDone());
        ScheduleResult nextScheduleResult = scheduler.schedule();
        assertTrue(nextScheduleResult.isFinished());
        assertTrue(nextScheduleResult.getBlocked().isDone());
        assertEquals(nextScheduleResult.getNewTasks().size(), 0);
        assertEquals(nextScheduleResult.getSplitsScheduled(), 0);
    }

    private static void assertPartitionedSplitCount(SqlStageExecution stage, int expectedPartitionedSplitCount)
    {
        assertEquals(stage.getAllTasks().stream().mapToInt(remoteTask -> remoteTask.getPartitionedSplitsInfo().getCount()).sum(), expectedPartitionedSplitCount);
    }
}
