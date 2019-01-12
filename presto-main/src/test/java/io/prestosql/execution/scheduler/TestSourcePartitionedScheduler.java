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
package io.prestosql.execution.scheduler;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.prestosql.OutputBuffers.OutputBufferId;
import io.prestosql.client.NodeVersion;
import io.prestosql.connector.ConnectorId;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.MockRemoteTaskFactory;
import io.prestosql.execution.MockRemoteTaskFactory.MockRemoteTask;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.execution.StageId;
import io.prestosql.execution.TestSqlTaskManager.MockLocationFactory;
import io.prestosql.failureDetector.NoOpFailureDetector;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.PrestoNode;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.StageExecutionStrategy;
import io.prestosql.spi.Node;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.split.ConnectorAwareSplitSource;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.StageExecutionPlan;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import io.prestosql.testing.TestingMetadata.TestingTableHandle;
import io.prestosql.testing.TestingSplit;
import io.prestosql.testing.TestingTransactionHandle;
import io.prestosql.util.FinalizerService;
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

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.OutputBuffers.BufferType.PARTITIONED;
import static io.prestosql.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
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

    private final ExecutorService queryExecutor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("stageScheduledExecutor-%s"));
    private final LocationFactory locationFactory = new MockLocationFactory();
    private final InMemoryNodeManager nodeManager = new InMemoryNodeManager();
    private final FinalizerService finalizerService = new FinalizerService();

    public TestSourcePartitionedScheduler()
    {
        nodeManager.addNode(CONNECTOR_ID,
                new PrestoNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new PrestoNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new PrestoNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
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
        StageExecutionPlan plan = createPlan(createFixedSplitSource(0, TestingSplit::createRemoteSplit));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(plan, stage, nodeManager, nodeTaskMap, 1);

        ScheduleResult scheduleResult = scheduler.schedule();

        assertEquals(scheduleResult.getNewTasks().size(), 1);
        assertEffectivelyFinished(scheduleResult, scheduler);

        stage.abort();
    }

    @Test
    public void testScheduleSplitsOneAtATime()
    {
        StageExecutionPlan plan = createPlan(createFixedSplitSource(60, TestingSplit::createRemoteSplit));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(plan, stage, nodeManager, nodeTaskMap, 1);

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
        StageExecutionPlan plan = createPlan(createFixedSplitSource(60, TestingSplit::createRemoteSplit));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(plan, stage, nodeManager, nodeTaskMap, 7);

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
        StageExecutionPlan plan = createPlan(createFixedSplitSource(80, TestingSplit::createRemoteSplit));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(plan, stage, nodeManager, nodeTaskMap, 1);

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
        StageExecutionPlan plan = createPlan(queuedSplitSource);
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        StageScheduler scheduler = getSourcePartitionedScheduler(plan, stage, nodeManager, nodeTaskMap, 1);

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
            NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSchedulerConfig().setIncludeCoordinator(false), nodeTaskMap);

            StageExecutionPlan plan = createPlan(createFixedSplitSource(20, TestingSplit::createRemoteSplit));
            SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

            StageScheduler scheduler = newSourcePartitionedSchedulerAsStageScheduler(
                    stage,
                    Iterables.getOnlyElement(plan.getSplitSources().keySet()),
                    Iterables.getOnlyElement(plan.getSplitSources().values()),
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
                new PrestoNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false),
                new PrestoNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN, false),
                new PrestoNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN, false));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);

        // Schedule 15 splits - there are 3 nodes, each node should get 5 splits
        StageExecutionPlan firstPlan = createPlan(createFixedSplitSource(15, TestingSplit::createRemoteSplit));
        SqlStageExecution firstStage = createSqlStageExecution(firstPlan, nodeTaskMap);
        StageScheduler firstScheduler = getSourcePartitionedScheduler(firstPlan, firstStage, nodeManager, nodeTaskMap, 200);

        ScheduleResult scheduleResult = firstScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, firstScheduler);
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(firstStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 5);
        }

        // Add new node
        Node additionalNode = new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN, false);
        nodeManager.addNode(CONNECTOR_ID, additionalNode);

        // Schedule 5 splits in another query. Since the new node does not have any splits, all 5 splits are assigned to the new node
        StageExecutionPlan secondPlan = createPlan(createFixedSplitSource(5, TestingSplit::createRemoteSplit));
        SqlStageExecution secondStage = createSqlStageExecution(secondPlan, nodeTaskMap);
        StageScheduler secondScheduler = getSourcePartitionedScheduler(secondPlan, secondStage, nodeManager, nodeTaskMap, 200);

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
        StageExecutionPlan firstPlan = createPlan(createFixedSplitSource(60, TestingSplit::createRemoteSplit));
        SqlStageExecution firstStage = createSqlStageExecution(firstPlan, nodeTaskMap);
        StageScheduler firstScheduler = getSourcePartitionedScheduler(firstPlan, firstStage, nodeManager, nodeTaskMap, 200);

        ScheduleResult scheduleResult = firstScheduler.schedule();
        assertEffectivelyFinished(scheduleResult, firstScheduler);
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(firstStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 20);
        }

        // Schedule more splits in another query, which will block since all nodes are full
        StageExecutionPlan secondPlan = createPlan(createFixedSplitSource(5, TestingSplit::createRemoteSplit));
        SqlStageExecution secondStage = createSqlStageExecution(secondPlan, nodeTaskMap);
        StageScheduler secondScheduler = getSourcePartitionedScheduler(secondPlan, secondStage, nodeManager, nodeTaskMap, 200);

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
            StageExecutionPlan plan,
            SqlStageExecution stage,
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            int splitBatchSize)
    {
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setIncludeCoordinator(false)
                .setMaxSplitsPerNode(20)
                .setMaxPendingSplitsPerTask(0);
        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, nodeSchedulerConfig, nodeTaskMap);

        PlanNodeId sourceNode = Iterables.getOnlyElement(plan.getSplitSources().keySet());
        SplitSource splitSource = Iterables.getOnlyElement(plan.getSplitSources().values());
        SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(splitSource.getConnectorId()), stage::getAllTasks);
        return newSourcePartitionedSchedulerAsStageScheduler(stage, sourceNode, splitSource, placementPolicy, splitBatchSize);
    }

    private static StageExecutionPlan createPlan(ConnectorSplitSource splitSource)
    {
        Symbol symbol = new Symbol("column");

        // table scan with splitCount splits
        PlanNodeId tableScanNodeId = new PlanNodeId("plan_id");
        TableScanNode tableScan = new TableScanNode(
                tableScanNodeId,
                new TableHandle(CONNECTOR_ID, new TestingTableHandle()),
                ImmutableList.of(symbol),
                ImmutableMap.of(symbol, new TestingColumnHandle("column")));

        RemoteSourceNode remote = new RemoteSourceNode(new PlanNodeId("remote_id"), new PlanFragmentId("plan_fragment_id"), ImmutableList.of(), Optional.empty(), GATHER);
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId("plan_id"),
                new JoinNode(new PlanNodeId("join_id"),
                        INNER,
                        tableScan,
                        remote,
                        ImmutableList.of(),
                        ImmutableList.<Symbol>builder()
                                .addAll(tableScan.getOutputSymbols())
                                .addAll(remote.getOutputSymbols())
                                .build(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                ImmutableMap.of(symbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(tableScanNodeId),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(symbol)),
                StageExecutionStrategy.ungroupedExecution(),
                StatsAndCosts.empty());

        return new StageExecutionPlan(
                testFragment,
                ImmutableMap.of(tableScanNodeId, new ConnectorAwareSplitSource(CONNECTOR_ID, TestingTransactionHandle.create(), splitSource)),
                ImmutableList.of());
    }

    private static ConnectorSplitSource createFixedSplitSource(int splitCount, Supplier<ConnectorSplit> splitFactory)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (int i = 0; i < splitCount; i++) {
            splits.add(splitFactory.get());
        }
        return new FixedSplitSource(splits.build());
    }

    private SqlStageExecution createSqlStageExecution(StageExecutionPlan tableScanPlan, NodeTaskMap nodeTaskMap)
    {
        StageId stageId = new StageId(new QueryId("query"), 0);
        SqlStageExecution stage = SqlStageExecution.createSqlStageExecution(stageId,
                locationFactory.createStageLocation(stageId),
                tableScanPlan.getFragment(),
                new MockRemoteTaskFactory(queryExecutor, scheduledExecutor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                queryExecutor,
                new NoOpFailureDetector(),
                new SplitSchedulerStats());

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
