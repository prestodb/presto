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
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.MockRemoteTaskFactory;
import com.facebook.presto.execution.MockRemoteTaskFactory.MockRemoteTask;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TestSqlTaskManager.MockLocationFactory;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.ConnectorAwareSplitSource;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.PartitionFunctionBinding;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.util.FinalizerService;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.Integer.min;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestSourcePartitionedScheduler
{
    public static final TaskId OUT = new TaskId("query", "stage", "out");
    private static final String CONNECTOR_ID = "test";

    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));
    private final LocationFactory locationFactory = new MockLocationFactory();
    private final InMemoryNodeManager nodeManager = new InMemoryNodeManager();
    private final FinalizerService finalizerService = new FinalizerService();

    public TestSourcePartitionedScheduler()
    {
        nodeManager.addNode(CONNECTOR_ID,
                new PrestoNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN),
                new PrestoNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN),
                new PrestoNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN));
    }

    @BeforeClass
    public void setUp()
    {
        finalizerService.start();
    }

    @AfterClass
    public void destroyExecutor()
    {
        executor.shutdownNow();
        finalizerService.destroy();
    }

    @Test
    public void testScheduleNoSplits()
            throws Exception
    {
        StageExecutionPlan plan = createPlan(createFixedSplitSource(0, TestingSplit::createRemoteSplit));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        SourcePartitionedScheduler scheduler = getSourcePartitionedScheduler(plan, stage, nodeManager, nodeTaskMap, 1);

        ScheduleResult scheduleResult = scheduler.schedule();

        assertTrue(scheduleResult.isFinished());
        assertTrue(scheduleResult.getBlocked().isDone());
        assertTrue(scheduleResult.getNewTasks().isEmpty());

        stage.abort();
    }

    @Test
    public void testScheduleSplitsOneAtATime()
            throws Exception
    {
        StageExecutionPlan plan = createPlan(createFixedSplitSource(60, TestingSplit::createRemoteSplit));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        SourcePartitionedScheduler scheduler = getSourcePartitionedScheduler(plan, stage, nodeManager, nodeTaskMap, 1);

        for (int i = 0; i < 60; i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            // only finishes when last split is fetched
            assertEquals(scheduleResult.isFinished(), i == 59);

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
            throws Exception
    {
        StageExecutionPlan plan = createPlan(createFixedSplitSource(60, TestingSplit::createRemoteSplit));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        SourcePartitionedScheduler scheduler = getSourcePartitionedScheduler(plan, stage, nodeManager, nodeTaskMap, 7);

        for (int i = 0; i <= (60 / 7); i++) {
            ScheduleResult scheduleResult = scheduler.schedule();

            // finishes when last split is fetched
            assertEquals(scheduleResult.isFinished(), i == (60 / 7));

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
            throws Exception
    {
        StageExecutionPlan plan = createPlan(createFixedSplitSource(80, TestingSplit::createRemoteSplit));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        SourcePartitionedScheduler scheduler = getSourcePartitionedScheduler(plan, stage, nodeManager, nodeTaskMap, 1);

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
            assertEquals(scheduleResult.isFinished(), i == 19);

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
            throws Exception
    {
        QueuedSplitSource queuedSplitSource = new QueuedSplitSource(TestingSplit::createRemoteSplit);
        StageExecutionPlan plan = createPlan(queuedSplitSource);
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
        SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

        SourcePartitionedScheduler scheduler = getSourcePartitionedScheduler(plan, stage, nodeManager, nodeTaskMap, 1);

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
            throws Exception
    {
        try {
            NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);
            InMemoryNodeManager nodeManager = new InMemoryNodeManager();
            NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, new NodeSchedulerConfig().setIncludeCoordinator(false), nodeTaskMap);

            StageExecutionPlan plan = createPlan(createFixedSplitSource(20, TestingSplit::createRemoteSplit));
            SqlStageExecution stage = createSqlStageExecution(plan, nodeTaskMap);

            SourcePartitionedScheduler scheduler = new SourcePartitionedScheduler(
                    stage,
                    Iterables.getOnlyElement(plan.getSplitSources().keySet()),
                    Iterables.getOnlyElement(plan.getSplitSources().values()),
                    new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector("test"), stage::getAllTasks),
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
            throws Exception
    {
        // use private node manager so we can add a node later
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(CONNECTOR_ID,
                new PrestoNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN),
                new PrestoNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN),
                new PrestoNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN));
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);

        // Schedule 15 splits - there are 3 nodes, each node should get 5 splits
        StageExecutionPlan firstPlan = createPlan(createFixedSplitSource(15, TestingSplit::createRemoteSplit));
        SqlStageExecution firstStage = createSqlStageExecution(firstPlan, nodeTaskMap);
        SourcePartitionedScheduler firstScheduler = getSourcePartitionedScheduler(firstPlan, firstStage, nodeManager, nodeTaskMap, 200);

        ScheduleResult scheduleResult = firstScheduler.schedule();
        assertTrue(scheduleResult.isFinished());
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(firstStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 5);
        }

        // Add new node
        Node additionalNode = new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN);
        nodeManager.addNode(CONNECTOR_ID, additionalNode);

        // Schedule 5 splits in another query. Since the new node does not have any splits, all 5 splits are assigned to the new node
        StageExecutionPlan secondPlan = createPlan(createFixedSplitSource(5, TestingSplit::createRemoteSplit));
        SqlStageExecution secondStage = createSqlStageExecution(secondPlan, nodeTaskMap);
        SourcePartitionedScheduler secondScheduler = getSourcePartitionedScheduler(secondPlan, secondStage, nodeManager, nodeTaskMap, 200);

        scheduleResult = secondScheduler.schedule();
        assertTrue(scheduleResult.isFinished());
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
            throws Exception
    {
        NodeTaskMap nodeTaskMap = new NodeTaskMap(finalizerService);

        // Schedule 60 splits - filling up all nodes
        StageExecutionPlan firstPlan = createPlan(createFixedSplitSource(60, TestingSplit::createRemoteSplit));
        SqlStageExecution firstStage = createSqlStageExecution(firstPlan, nodeTaskMap);
        SourcePartitionedScheduler firstScheduler = getSourcePartitionedScheduler(firstPlan, firstStage, nodeManager, nodeTaskMap, 200);

        ScheduleResult scheduleResult = firstScheduler.schedule();
        assertTrue(scheduleResult.isFinished());
        assertTrue(scheduleResult.getBlocked().isDone());
        assertEquals(scheduleResult.getNewTasks().size(), 3);
        assertEquals(firstStage.getAllTasks().size(), 3);
        for (RemoteTask remoteTask : firstStage.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 20);
        }

        // Schedule more splits in another query, which will block since all nodes are full
        StageExecutionPlan secondPlan = createPlan(createFixedSplitSource(5, TestingSplit::createRemoteSplit));
        SqlStageExecution secondStage = createSqlStageExecution(secondPlan, nodeTaskMap);
        SourcePartitionedScheduler secondScheduler = getSourcePartitionedScheduler(secondPlan, secondStage, nodeManager, nodeTaskMap, 200);

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

    private static SourcePartitionedScheduler getSourcePartitionedScheduler(
            StageExecutionPlan plan,
            SqlStageExecution stage, NodeManager nodeManager, NodeTaskMap nodeTaskMap,
            int splitBatchSize)
    {
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setIncludeCoordinator(false)
                .setMaxSplitsPerNode(20)
                .setMaxPendingSplitsPerNodePerTask(0);
        NodeScheduler nodeScheduler = new NodeScheduler(new LegacyNetworkTopology(), nodeManager, nodeSchedulerConfig, nodeTaskMap);

        PlanNodeId sourceNode = Iterables.getOnlyElement(plan.getSplitSources().keySet());
        SplitSource splitSource = Iterables.getOnlyElement(plan.getSplitSources().values());
        SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeScheduler.createNodeSelector(splitSource.getDataSourceName()), stage::getAllTasks);
        return new SourcePartitionedScheduler(stage, sourceNode, splitSource, placementPolicy, splitBatchSize);
    }

    private static StageExecutionPlan createPlan(ConnectorSplitSource splitSource)
    {
        Symbol symbol = new Symbol("column");

        // table scan with splitCount splits
        PlanNodeId tableScanNodeId = new PlanNodeId("plan_id");
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId("plan_id"),
                new JoinNode(new PlanNodeId("join_id"),
                        INNER,
                        new TableScanNode(
                                tableScanNodeId,
                                new TableHandle(CONNECTOR_ID, new TestingTableHandle()),
                                ImmutableList.of(symbol),
                                ImmutableMap.of(symbol, new TestingColumnHandle("column")),
                                Optional.empty(),
                                TupleDomain.all(),
                                null),
                        new RemoteSourceNode(new PlanNodeId("remote_id"), new PlanFragmentId("plan_fragment_id"), ImmutableList.of()),
                        ImmutableList.of(),
                        Optional.<Symbol>empty(),
                        Optional.<Symbol>empty()),
                ImmutableMap.<Symbol, Type>of(symbol, VARCHAR),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(tableScanNodeId),
                new PartitionFunctionBinding(SINGLE_DISTRIBUTION, ImmutableList.of(symbol), ImmutableList.of()));

        return new StageExecutionPlan(
                testFragment,
                ImmutableMap.of(tableScanNodeId, new ConnectorAwareSplitSource(CONNECTOR_ID, TestingTransactionHandle.create(CONNECTOR_ID), splitSource)),
                ImmutableList.of());
    }

    private static ConnectorSplitSource createFixedSplitSource(int splitCount, Supplier<ConnectorSplit> splitFactory)
    {
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (int i = 0; i < splitCount; i++) {
            splits.add(splitFactory.get());
        }
        return new FixedSplitSource(CONNECTOR_ID, splits.build());
    }

    private SqlStageExecution createSqlStageExecution(StageExecutionPlan tableScanPlan, NodeTaskMap nodeTaskMap)
    {
        StageId stageId = new StageId(new QueryId("query"), "stage");
        SqlStageExecution stage = new SqlStageExecution(stageId,
                locationFactory.createStageLocation(stageId),
                tableScanPlan.getFragment(),
                new MockRemoteTaskFactory(executor),
                TEST_SESSION,
                true,
                nodeTaskMap,
                executor);

        stage.setOutputBuffers(INITIAL_EMPTY_OUTPUT_BUFFERS
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
        public String getDataSourceName()
        {
            return CONNECTOR_ID;
        }

        @Override
        public synchronized CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
        {
            return notEmptyFuture.thenApply(x -> getBatch(maxSize));
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
