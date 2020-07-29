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
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.MockRemoteTaskFactory;
import com.facebook.presto.execution.MockRemoteTaskFactory.MockRemoteTask;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TestSqlTaskManager.MockLocationFactory;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
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
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
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
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.util.FinalizerService;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
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
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSourcePartitionedScheduler
{
    public static final OutputBufferId OUT = new OutputBufferId(0);
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("connector_id");
    private static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("plan_id");

    private final ExecutorService queryExecutor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("stageScheduledExecutor-%s"));
    private final LocationFactory locationFactory = new MockLocationFactory();
    private final InMemoryNodeManager nodeManager = new InMemoryNodeManager();
    private final FinalizerService finalizerService = new FinalizerService();

    public TestSourcePartitionedScheduler()
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

        StageScheduler scheduler = getSourcePartitionedScheduler(createFixedSplitSource(0, TestingSplit::createRemoteSplit), stage, nodeManager, nodeTaskMap, 1);

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

        StageScheduler scheduler = getSourcePartitionedScheduler(createFixedSplitSource(60, TestingSplit::createRemoteSplit), stage, nodeManager, nodeTaskMap, 1);

        for (int i = 0; i < 60; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            // only finishes when last split is fetched
            if (i == 59) {
                assertEffectivelyFinished(scheduleResult, scheduler);
            }
            else {
                assertFalse(scheduleResult.isFinished());
            }

            // never blocks
            assertTrue(scheduleResult.getBlocked().isDone());

            // first three splits create new tasks
            assertEquals(scheduleResult.getNewTasks().size(), i < 3 ? 1 : 0);
            assertEquals(stage.getAllTasks().size(), i < 3 ? i + 1 : 3);

            assertPartitionedSplitCount(stage, min(i + 1, 60));
        }

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 20);
        }

        stage.abort();
    }

    @Test
    public void testScheduleSplitsBatched()
    {
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(createFixedSplitSource(60, TestingSplit::createRemoteSplit), stage, nodeManager, nodeTaskMap, 7);

        for (int i = 0; i <= (60 / 7); i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            // finishes when last split is fetched
            if (i == (60 / 7)) {
                assertEffectivelyFinished(scheduleResult, scheduler);
            }
            else {
                assertFalse(scheduleResult.isFinished());
            }

            // never blocks
            assertTrue(scheduleResult.getBlocked().isDone());

            // first three splits create new tasks
            assertEquals(scheduleResult.getNewTasks().size(), i == 0 ? 3 : 0);
            assertEquals(stage.getAllTasks().size(), 3);

            assertPartitionedSplitCount(stage, min((i + 1) * 7, 60));
        }

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 20);
        }

        stage.abort();
    }

    @Test
    public void testScheduleSplitsBlock()
    {
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(createFixedSplitSource(80, TestingSplit::createRemoteSplit), stage, nodeManager, nodeTaskMap, 1);

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
            assertEquals(remoteTask.getPartitionedSplitCount(), 20);
        }

        // todo rewrite MockRemoteTask to fire a tate transition when splits are cleared, and then validate blocked future completes

        // drop the 20 splits from one node
        ((MockRemoteTask) stage.getAllTasks().get(0)).clearSplits();

        // schedule remaining 20 splits
        for (int i = 0; i < 20; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            // finishes when last split is fetched
            if (i == 19) {
                assertEffectivelyFinished(scheduleResult, scheduler);
            }
            else {
                assertFalse(scheduleResult.isFinished());
            }

            // does not block again
            assertTrue(scheduleResult.getBlocked().isDone());

            // no additional tasks will be created
            assertEquals(scheduleResult.getNewTasks().size(), 0);
            assertEquals(stage.getAllTasks().size(), 3);

            // we dropped 20 splits so start at 40 and count to 60
            assertPartitionedSplitCount(stage, min(i + 41, 60));
        }

        for (RemoteTask remoteTask : stage.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 20);
        }

        stage.abort();
    }

    @Test
    public void testScheduleSlowSplitSource()
    {
        QueuedSplitSource queuedSplitSource = new QueuedSplitSource(TestingSplit::createRemoteSplit);
        SubPlan plan = createPlan();
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(queuedSplitSource, stage, nodeManager, nodeTaskMap, 1);

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
                    nodeTaskMap);

            SubPlan plan = createPlan();
            SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

            StageScheduler scheduler = newSourcePartitionedSchedulerAsStageScheduler(
                    stage,
                    TABLE_SCAN_NODE_ID,
                    new ConnectorAwareSplitSource(CONNECTOR_ID, TestingTransactionHandle.create(), createFixedSplitSource(20, TestingSplit::createRemoteSplit)),
                    new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(CONNECTOR_ID), stage::getAllTasks),
                    2);
            scheduler.schedule();

            fail("expected PrestoException");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), NO_NODES_AVAILABLE.toErrorCode());
        }
    }

    @Test
    public void testBalancedSplitAssignment()
    {
        // use private node manager so we can add a node later
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(CONNECTOR_ID,
                new InternalNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new InternalNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new InternalNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);

        // Schedule 15 splits - there are 3 nodes, each node should get 5 splits
        SubPlan firstPlan = createPlan();
        SqlStageExecution firstStage = createSqlStageExecution(firstPlan, nodeTaskMap);
        StageScheduler firstScheduler = getSourcePartitionedScheduler(createFixedSplitSource(15, TestingSplit::createRemoteSplit), firstStage, nodeManager, nodeTaskMap, 200);

        ScheduleResult scheduleResult = firstScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, firstScheduler);
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(firstStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 5);
        }

        // Add new node
        InternalNode additionalNode = new InternalNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, additionalNode);

        // Schedule 5 splits in another query. Since the new node does not have any splits, all 5 splits are assigned to the new node
        SubPlan secondPlan = createPlan();
        SqlStageExecution secondStage = createSqlStageExecution(secondPlan, nodeTaskMap);
        StageScheduler secondScheduler = getSourcePartitionedScheduler(createFixedSplitSource(5, TestingSplit::createRemoteSplit), secondStage, nodeManager, nodeTaskMap, 200);

        scheduleResult = secondScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, secondScheduler);
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 1);
        assertEquals(secondStage.getAllTasks().size(), 1);
        RemoteTask task = secondStage.getAllTasks().get(0);
        assertEquals(task.getPartitionedSplitCount(), 5);

        firstStage.abort();
        secondStage.abort();
    }

    @Test
    public void testBlockCausesFullSchedule()
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);

        // Schedule 60 splits - filling up all nodes
        SubPlan firstPlan = createPlan();
        SqlStageExecution firstStage = createSqlStageExecution(firstPlan, nodeTaskMap);
        StageScheduler firstScheduler = getSourcePartitionedScheduler(createFixedSplitSource(60, TestingSplit::createRemoteSplit), firstStage, nodeManager, nodeTaskMap, 200);

        ScheduleResult scheduleResult = firstScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, firstScheduler);
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(firstStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 20);
        }

        // Schedule more splits in another query, which will block since all nodes are full
        SubPlan secondPlan = createPlan();
        SqlStageExecution secondStage = createSqlStageExecution(secondPlan, nodeTaskMap);
        StageScheduler secondScheduler = getSourcePartitionedScheduler(createFixedSplitSource(5, TestingSplit::createRemoteSplit), secondStage, nodeManager, nodeTaskMap, 200);

        scheduleResult = secondScheduler.schedule();
        assertFalse(scheduleResult.isFinished());
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(secondStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : secondStage.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 0);
        }

        firstStage.abort();
        secondStage.abort();
    }

    private static void assertPartitionedSplitCount(SqlStageExecution stage, int expectedPartitionedSplitCount)
    {
        assertEquals(stage.getAllTasks().stream().mapToInt(RemoteTask::getPartitionedSplitCount).sum(), expectedPartitionedSplitCount);
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

    private static StageScheduler getSourcePartitionedScheduler(
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
        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSelectionStats(), nodeSchedulerConfig, nodeTaskMap);
        SplitSource splitSource = new ConnectorAwareSplitSource(CONNECTOR_ID, TestingTransactionHandle.create(), connectorSplitSource);
        SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(splitSource.getConnectorId()), stage::getAllTasks);
        return newSourcePartitionedSchedulerAsStageScheduler(stage, TABLE_SCAN_NODE_ID, splitSource, placementPolicy, splitBatchSize);
    }

    private static SubPlan createPlan()
    {
        VariableReferenceExpression variable = new VariableReferenceExpression("column", VARCHAR);

        // table scan with splitCount splits
        TableScanNode tableScan = new TableScanNode(
                TABLE_SCAN_NODE_ID,
                new TableHandle(CONNECTOR_ID, new TestingTableHandle(), TestingTransactionHandle.create(), Optional.empty()),
                ImmutableList.of(variable),
                ImmutableMap.of(variable, new TestingColumnHandle("column")),
                TupleDomain.all(),
                TupleDomain.all());

        RemoteSourceNode remote = new RemoteSourceNode(new PlanNodeId("remote_id"), new PlanFragmentId(0), ImmutableList.of(), false, Optional.empty(), GATHER);
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId(0),
                new JoinNode(new PlanNodeId("join_id"),
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

    private static ConnectorSplitSource createFixedSplitSource(int splitCount, Supplier<ConnectorSplit> splitFactory)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (int i = 0; i < splitCount; i++) {
            splits.add(splitFactory.get());
        }
        return new FixedSplitSource(splits.build());
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
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()));

        stage.setOutputBuffers(createInitialEmptyOutputBuffers(PARTITIONED)
                .withBuffer(OUT, 0)
                .withNoMoreBufferIds());

        return stage;
    }

    private static class QueuedSplitSource
            implements ConnectorSplitSource
    {
        private final Supplier<ConnectorSplit> splitFactory;
        private final LinkedBlockingQueue<ConnectorSplit> queue = new LinkedBlockingQueue<>();
        private CompletableFuture<?> notEmptyFuture = new CompletableFuture<>();
        private boolean closed;

        public QueuedSplitSource(Supplier<ConnectorSplit> splitFactory)
        {
            this.splitFactory = requireNonNull(splitFactory, "splitFactory is null");
        }

        synchronized void addSplits(int count)
        {
            if (closed) {
                return;
            }
            for (int i = 0; i < count; i++) {
                queue.add(splitFactory.get());
                notEmptyFuture.complete(null);
            }
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
        {
            checkArgument(partitionHandle.equals(NOT_PARTITIONED), "partitionHandle must be NOT_PARTITIONED");
            return notEmptyFuture
                    .thenApply(x -> getBatch(maxSize))
                    .thenApply(splits -> new ConnectorSplitBatch(splits, isFinished()));
        }

        private synchronized List<ConnectorSplit> getBatch(int maxSize)
        {
            // take up to maxSize elements from the queue
            List<ConnectorSplit> elements = new ArrayList<>(maxSize);
            queue.drainTo(elements, maxSize);

            // if the queue is empty and the current future is finished, create a new one so
            // a new readers can be notified when the queue has elements to read
            if (queue.isEmpty() && !closed) {
                if (notEmptyFuture.isDone()) {
                    notEmptyFuture = new CompletableFuture<>();
                }
            }

            return ImmutableList.copyOf(elements);
        }

        @Override
        public synchronized boolean isFinished()
        {
            return closed && queue.isEmpty();
        }

        @Override
        public synchronized void close()
        {
            closed = true;
        }
    }
}
