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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.Session;
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.execution.SharedBuffer.BufferState;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TestSqlTaskManager.MockLocationFactory;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.ConnectorAwareSplitSource;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestSqlStageExecution
{
    public static final TaskId OUT = new TaskId("query", "stage", "out");
    private NodeTaskMap nodeTaskMap;
    private InMemoryNodeManager nodeManager;
    private NodeScheduler nodeScheduler;
    private LocationFactory locationFactory;
    private Supplier<ConnectorSplit> splitFactory;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        nodeManager = new InMemoryNodeManager();
        ImmutableList.Builder<Node> nodeBuilder = ImmutableList.builder();
        nodeBuilder.add(new PrestoNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN));
        nodeBuilder.add(new PrestoNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN));
        nodeBuilder.add(new PrestoNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN));
        ImmutableList<Node> nodes = nodeBuilder.build();

        nodeManager.addNode("foo", nodes);
        NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig()
                .setMaxSplitsPerNode(20)
                .setIncludeCoordinator(false)
                .setMaxPendingSplitsPerNodePerTask(10);

        nodeTaskMap = new NodeTaskMap();
        nodeScheduler = new NodeScheduler(nodeManager, nodeSchedulerConfig, nodeTaskMap);
        locationFactory = new MockLocationFactory();
        splitFactory = new Supplier<ConnectorSplit>() {
            @Override
            public ConnectorSplit get()
            {
                return TestingSplit.createLocalSplit();
            }
        };
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*No nodes available to run query")
    public void testExcludeCoordinator()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        NodeScheduler nodeScheduler = new NodeScheduler(nodeManager, new NodeSchedulerConfig().setIncludeCoordinator(false), nodeTaskMap);

        // Start sql stage execution
        StageExecutionPlan tableScanPlan = createTableScanPlan("test", 20, new Supplier<ConnectorSplit>() {
            @Override
            public ConnectorSplit get()
            {
                return TestingSplit.createEmptySplit();
            }
        });
        SqlStageExecution sqlStageExecution = createSqlStageExecution(nodeScheduler, 2, tableScanPlan);
        Future future = sqlStageExecution.start();
        future.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testSplitAssignment()
            throws Exception
    {
        // Start sql stage execution (schedule 15 splits in batches of 2), there are 3 nodes, each node should get 5 splits
        StageExecutionPlan tableScanPlan = createTableScanPlan("test", 15, splitFactory);
        SqlStageExecution sqlStageExecution1 = createSqlStageExecution(nodeScheduler, 2, tableScanPlan);
        Future future1 = sqlStageExecution1.start();
        future1.get(1, TimeUnit.SECONDS);
        for (RemoteTask remoteTask : sqlStageExecution1.getAllTasks()) {
            assertEquals(remoteTask.getPartitionedSplitCount(), 5);
        }

        // Add new node
        Node additionalNode = new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN);
        nodeManager.addNode("foo", additionalNode);

        // Schedule next query with 5 splits. Since the new node does not have any splits, all 5 splits are assigned to the new node
        StageExecutionPlan tableScanPlan2 = createTableScanPlan("test", 5, splitFactory);
        SqlStageExecution sqlStageExecution2 = createSqlStageExecution(nodeScheduler, 5, tableScanPlan2);
        Future future2 = sqlStageExecution2.start();
        future2.get(1, TimeUnit.SECONDS);
        List<RemoteTask> tasks2 = sqlStageExecution2.getTasks(additionalNode);

        RemoteTask task = Iterables.getFirst(tasks2, null);
        assertNotNull(task);
        assertEquals(task.getPartitionedSplitCount(), 5);
    }

    @Test
    public void testSplitAssignmentBatchSizeGreaterThanMaxPending()
            throws Exception
    {
        // Start sql stage execution with 100 splits. Only 20 will be scheduled on each node as that is the maxSplitsPerNode
        StageExecutionPlan tableScanPlan = createTableScanPlan("test", 100, splitFactory);
        SqlStageExecution sqlStageExecution1 = createSqlStageExecution(nodeScheduler, 100, tableScanPlan);
        Future future1 = sqlStageExecution1.start();

        // The stage scheduler will block and this will cause a timeout exception
        try {
            future1.get(1, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
        }

        for (RemoteTask task : sqlStageExecution1.getAllTasks()) {
            assertEquals(task.getPartitionedSplitCount(), 20);
        }
    }

    private SqlStageExecution createSqlStageExecution(NodeScheduler nodeScheduler, int splitBatchSize, StageExecutionPlan tableScanPlan)
    {
        ExecutorService remoteTaskExecutor = newCachedThreadPool(daemonThreadsNamed("remoteTaskExecutor-%s"));
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));

        OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS
                .withBuffer(OUT, new UnpartitionedPagePartitionFunction())
                .withNoMoreBufferIds();

        return new SqlStageExecution(new QueryId("query"),
                locationFactory,
                tableScanPlan,
                nodeScheduler,
                remoteTaskFactory,
                TEST_SESSION,
                splitBatchSize,
                8,      // initialHashPartitions
                executor,
                nodeTaskMap,
                outputBuffers);
    }

    @Test(enabled = false)
    public void testYieldCausesFullSchedule()
            throws Exception
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
        SqlStageExecution stageExecution = null;
        try {
            StageExecutionPlan joinPlan = createJoinPlan("A");

            InMemoryNodeManager nodeManager = new InMemoryNodeManager();
            nodeManager.addNode("foo", new PrestoNode("other", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN));

            OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS
                    .withBuffer(OUT, new UnpartitionedPagePartitionFunction())
                    .withNoMoreBufferIds();

            stageExecution = new SqlStageExecution(new QueryId("query"),
                    new MockLocationFactory(),
                    joinPlan,
                    new NodeScheduler(nodeManager, new NodeSchedulerConfig(), nodeTaskMap),
                    new MockRemoteTaskFactory(executor),
                    TEST_SESSION,
                    1000,
                    8,
                    executor,
                    nodeTaskMap,
                    outputBuffers);

            Future<?> future = stageExecution.start();

            long start = System.nanoTime();
            while (true) {
                StageInfo stageInfo = stageExecution.getStageInfo();
                assertEquals(stageInfo.getState(), StageState.SCHEDULING);

                StageInfo tableScanInfo = stageInfo.getSubStages().get(0);
                StageState tableScanState = tableScanInfo.getState();
                switch (tableScanState) {
                    case PLANNED:
                    case SCHEDULING:
                    case SCHEDULED:
                        break;
                    case RUNNING:
                        // there should be two tasks (even though only one can ever be used)
                        assertEquals(stageInfo.getTasks().size(), 2);

                        assertEquals(tableScanInfo.getTasks().size(), 1);
                        assertEquals(tableScanInfo.getTasks().get(0).getOutputBuffers().getState(), BufferState.NO_MORE_BUFFERS);
                        return;
                    case FINISHED:
                    case CANCELED:
                    case FAILED:
                        fail("Unexpected state for table scan stage " + tableScanState);
                        break;
                }

                if (TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) > 1) {
                    fail("Expected test to complete within 1 second");
                }

                try {
                    future.get(50, TimeUnit.MILLISECONDS);
                }
                catch (TimeoutException e) {
                }
            }
        }
        finally {
            if (stageExecution != null) {
                stageExecution.cancel();
            }
            executor.shutdownNow();
        }
    }

    private StageExecutionPlan createJoinPlan(String planId)
    {
        // create table scan for build data with a single split, so it is only waiting on the no-more buffers call
        StageExecutionPlan build = createTableScanPlan("build", 1, splitFactory);

        // create an exchange to read the build data
        RemoteSourceNode buildSource = new RemoteSourceNode(new PlanNodeId(planId + "-build"),
                build.getFragment().getId(),
                ImmutableList.copyOf(build.getFragment().getSymbols().keySet()));

        // create table scan for probe data with three splits, so it will not send the no-more buffers call
        StageExecutionPlan probe = createTableScanPlan("probe", 10, splitFactory);

        // create an exchange to read the probe data
        RemoteSourceNode probeSource = new RemoteSourceNode(new PlanNodeId(planId + "-probe"),
                probe.getFragment().getId(),
                ImmutableList.copyOf(probe.getFragment().getSymbols().keySet()));

        // join build and probe
        JoinNode joinNode = new JoinNode(new PlanNodeId(planId), JoinNode.Type.INNER, probeSource, buildSource, ImmutableList.<EquiJoinClause>of(), Optional.empty(), Optional.empty());
        PlanFragment joinPlan = new PlanFragment(
                new PlanFragmentId(planId),
                joinNode,
                probe.getFragment().getSymbols(), // this is wrong, but it works
                joinNode.getOutputSymbols(),
                PlanDistribution.SOURCE,
                new PlanNodeId(planId),
                OutputPartitioning.NONE,
                ImmutableList.<Symbol>of(),
                Optional.empty());

        return new StageExecutionPlan(joinPlan,
                probe.getDataSource(),
                ImmutableList.of(probe, build)
        );
    }

    private StageExecutionPlan createTableScanPlan(String planId, int splitCount, Supplier<ConnectorSplit> splitFactory)
    {
        Symbol symbol = new Symbol("column");

        // table scan with splitCount splits
        PlanNodeId tableScanNodeId = new PlanNodeId(planId);
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId(planId),
                new TableScanNode(
                        tableScanNodeId,
                        new TableHandle("test", new TestingTableHandle()),
                        ImmutableList.of(symbol),
                        ImmutableMap.of(symbol, new ColumnHandle("test", new TestingColumnHandle("column"))),
                        null,
                        Optional.empty()),
                ImmutableMap.<Symbol, Type>of(symbol, VARCHAR),
                ImmutableList.of(symbol),
                PlanDistribution.SOURCE,
                tableScanNodeId,
                OutputPartitioning.NONE,
                ImmutableList.<Symbol>of(),
                Optional.empty());

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (int i = 0; i < splitCount; i++) {
            splits.add(splitFactory.get());
        }
        SplitSource splitSource = new ConnectorAwareSplitSource("test", new FixedSplitSource(null, splits.build()));

        return new StageExecutionPlan(testFragment,
                Optional.of(splitSource),
                ImmutableList.<StageExecutionPlan>of()
        );
    }

    private static class MockRemoteTaskFactory
            implements RemoteTaskFactory
    {
        private final Executor executor;

        private MockRemoteTaskFactory(Executor executor)
        {
            this.executor = executor;
        }

        @Override
        public RemoteTask createRemoteTask(
                Session session,
                TaskId taskId,
                Node node,
                PlanFragment fragment,
                Multimap<PlanNodeId, Split> initialSplits,
                OutputBuffers outputBuffers)
        {
            return new MockRemoteTask(taskId, node.getNodeIdentifier(), fragment, executor, initialSplits);
        }

        private static class MockRemoteTask
                implements RemoteTask
        {
            private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskInfo.STARTING_VERSION);

            private final URI location;
            private final TaskStateMachine taskStateMachine;
            private final TaskContext taskContext;
            private final SharedBuffer sharedBuffer;
            private final String nodeId;

            private final PlanFragment fragment;

            @GuardedBy("this")
            private final Set<PlanNodeId> noMoreSplits = new HashSet<>();

            @GuardedBy("this")
            private final Multimap<PlanNodeId, Split> splits = HashMultimap.create();

            public MockRemoteTask(TaskId taskId,
                    String nodeId,
                    PlanFragment fragment,
                    Executor executor,
                    Multimap<PlanNodeId, Split> initialSplits)
            {
                this.taskStateMachine = new TaskStateMachine(checkNotNull(taskId, "taskId is null"), checkNotNull(executor, "executor is null"));

                this.taskContext = new TaskContext(taskStateMachine, executor, TEST_SESSION, new DataSize(256, MEGABYTE), new DataSize(1, MEGABYTE), true, true);

                this.location = URI.create("fake://task/" + taskId);

                this.sharedBuffer = new SharedBuffer(taskId, executor, checkNotNull(new DataSize(1, Unit.BYTE), "maxBufferSize is null"));
                this.fragment = checkNotNull(fragment, "fragment is null");
                this.nodeId = nodeId;
                splits.putAll(initialSplits);
            }

            @Override
            public String getNodeId()
            {
                return nodeId;
            }

            @Override
            public TaskInfo getTaskInfo()
            {
                TaskState state = taskStateMachine.getState();
                List<ExecutionFailureInfo> failures = ImmutableList.of();
                if (state == TaskState.FAILED) {
                    failures = toFailures(taskStateMachine.getFailureCauses());
                }

                return new TaskInfo(
                        taskStateMachine.getTaskId(),
                        Optional.empty(),
                        nextTaskInfoVersion.getAndIncrement(),
                        state,
                        location,
                        DateTime.now(),
                        sharedBuffer.getInfo(),
                        ImmutableSet.<PlanNodeId>of(),
                        taskContext.getTaskStats(),
                        failures);
            }

            public void finished()
            {
                taskStateMachine.finished();
            }

            @Override
            public void start()
            {
            }

            @Override
            public void addSplits(PlanNodeId sourceId, Iterable<Split> splits)
            {
                checkNotNull(splits, "splits is null");
                for (Split split : splits) {
                    this.splits.put(sourceId, split);
                }
            }

            @Override
            public void noMoreSplits(PlanNodeId sourceId)
            {
                noMoreSplits.add(sourceId);
            }

            @Override
            public void setOutputBuffers(OutputBuffers outputBuffers)
            {
                sharedBuffer.setOutputBuffers(outputBuffers);
            }

            @Override
            public void addStateChangeListener(final StateChangeListener<TaskInfo> stateChangeListener)
            {
                taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>()
                {
                    @Override
                    public void stateChanged(TaskState newValue)
                    {
                        stateChangeListener.stateChanged(getTaskInfo());
                    }
                });
            }

            @Override
            public void cancel()
            {
                taskStateMachine.cancel();
            }

            @Override
            public void abort()
            {
                taskStateMachine.abort();
            }

            @Override
            public Duration waitForTaskToFinish(Duration maxWait)
                    throws InterruptedException
            {
                while (true) {
                    TaskState currentState = taskStateMachine.getState();
                    if (maxWait.toMillis() <= 1 || currentState.isDone()) {
                        return maxWait;
                    }
                    maxWait = taskStateMachine.waitForStateChange(currentState, maxWait);
                }
            }

            @Override
            public int getPartitionedSplitCount()
            {
                if (taskStateMachine.getState().isDone()) {
                    return 0;
                }
                return splits.size();
            }

            @Override
            public int getQueuedPartitionedSplitCount()
            {
                return 0;
            }
        }
    }
}
