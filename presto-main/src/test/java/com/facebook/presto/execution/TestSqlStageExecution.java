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
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplit;
import com.facebook.presto.execution.SharedBuffer.QueueState;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TestSqlTaskManager.MockLocationFactory;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.util.Threads;
import com.google.common.base.Optional;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;
import static com.facebook.presto.util.Failures.toFailures;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestSqlStageExecution
{
    public static final Session SESSION = new Session("user", "source", "catalog", "schema", "address", "agent");

    @Test
    public void testYieldCausesFullSchedule()
            throws Exception
    {
        ExecutorService executor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("test"));
        SqlStageExecution stageExecution = null;
        try {
            MetadataManager metadata = new MetadataManager();
            metadata.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, new DualMetadata());

            StageExecutionPlan joinPlan = createJoinPlan("A", metadata);

            InMemoryNodeManager nodeManager = new InMemoryNodeManager();
            nodeManager.addNode("foo", new Node("other", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN));

            OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS
                    .withBuffer("out", new UnpartitionedPagePartitionFunction())
                    .withNoMoreBufferIds();

            stageExecution = new SqlStageExecution(new QueryId("query"),
                    new MockLocationFactory(),
                    joinPlan,
                    new NodeScheduler(nodeManager, new NodeSchedulerConfig()), new MockRemoteTaskFactory(executor),
                    SESSION,
                    1,
                    8,
                    executor,
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
                        assertEquals(tableScanInfo.getTasks().get(0).getOutputBuffers().getState(), QueueState.NO_MORE_QUEUES);
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
                stageExecution.cancel(false);
            }
            executor.shutdownNow();
        }
    }

    private StageExecutionPlan createJoinPlan(String planId, MetadataManager metadata)
    {
        // create table scan for build data with a single split, so it is only waiting on the no-more buffers call
        StageExecutionPlan build = createTableScanPlan("build", metadata, 1);

        // create an exchange to read the build data
        ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId(planId + "-build"),
                build.getFragment().getId(),
                ImmutableList.copyOf(build.getFragment().getSymbols().keySet()));

        // create table scan for probe data with three splits, so it will not send the no-more buffers call
        StageExecutionPlan probe = createTableScanPlan("probe", metadata, 10);

        // join build and probe
        PlanFragment joinPlan = new PlanFragment(
                new PlanFragmentId(planId),
                new JoinNode(new PlanNodeId(planId), JoinNode.Type.INNER, probe.getFragment().getRoot(), exchangeNode, ImmutableList.<EquiJoinClause>of()),
                probe.getFragment().getSymbols(), // this is wrong, but it works
                PlanDistribution.SOURCE,
                new PlanNodeId(planId),
                OutputPartitioning.NONE);

        return new StageExecutionPlan(joinPlan,
                probe.getDataSource(),
                ImmutableList.<StageExecutionPlan>of(build),
                ImmutableMap.<PlanNodeId, OutputReceiver>of());
    }

    private StageExecutionPlan createTableScanPlan(String planId, MetadataManager metadata, int splitCount)
    {
        TableHandle tableHandle = metadata.getTableHandle(new QualifiedTableName("default", "default", DualMetadata.NAME)).get();
        ColumnHandle columnHandle = metadata.getColumnHandle(tableHandle, DualMetadata.COLUMN_NAME).get();
        Symbol symbol = new Symbol(DualMetadata.COLUMN_NAME);

        // table scan with 3 splits
        Split split = new DualSplit(HostAddress.fromString("127.0.0.1"));
        PlanNodeId tableScanNodeId = new PlanNodeId(planId);
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId(planId),
                new TableScanNode(
                        tableScanNodeId,
                        tableHandle,
                        ImmutableList.of(symbol),
                        ImmutableMap.of(symbol, columnHandle),
                        null,
                        Optional.<GeneratedPartitions>absent()),
                ImmutableMap.<Symbol, Type>of(symbol, Type.VARCHAR),
                PlanDistribution.SOURCE,
                tableScanNodeId,
                OutputPartitioning.NONE);
        DataSource dataSource = new DataSource(null, ImmutableList.copyOf(Collections.nCopies(splitCount, split)));

        return new StageExecutionPlan(testFragment,
                Optional.<DataSource>of(dataSource),
                ImmutableList.<StageExecutionPlan>of(),
                ImmutableMap.<PlanNodeId, OutputReceiver>of());
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
                Map<PlanNodeId, OutputReceiver> outputReceivers,
                OutputBuffers outputBuffers)
        {
            return new MockRemoteTask(taskId, fragment, executor);
        }

        private static class MockRemoteTask
                implements RemoteTask
        {
            private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskInfo.STARTING_VERSION);

            private final URI location;
            private final TaskStateMachine taskStateMachine;
            private final TaskContext taskContext;
            private final SharedBuffer sharedBuffer;

            private final PlanFragment fragment;

            @GuardedBy("this")
            private final Set<PlanNodeId> noMoreSplits = new HashSet<>();

            @GuardedBy("this")
            private final Multimap<PlanNodeId, Split> splits = HashMultimap.create();

            public MockRemoteTask(TaskId taskId,
                    PlanFragment fragment,
                    Executor executor)
            {
                this.taskStateMachine = new TaskStateMachine(checkNotNull(taskId, "taskId is null"), checkNotNull(executor, "executor is null"));

                Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
                this.taskContext = new TaskContext(taskStateMachine, executor, session, new DataSize(256, MEGABYTE), new DataSize(1, MEGABYTE), true);

                this.location = URI.create("fake://task/" + taskId);

                this.sharedBuffer = new SharedBuffer(checkNotNull(new DataSize(1, Unit.BYTE), "maxBufferSize is null"), INITIAL_EMPTY_OUTPUT_BUFFERS);
                this.fragment = checkNotNull(fragment, "fragment is null");
            }

            @Override
            public String getNodeId()
            {
                return "node";
            }

            @Override
            public TaskInfo getTaskInfo()
            {
                TaskState state = taskStateMachine.getState();
                List<FailureInfo> failures = ImmutableList.of();
                if (state == TaskState.FAILED) {
                    failures = toFailures(taskStateMachine.getFailureCauses());
                }

                return new TaskInfo(
                        taskStateMachine.getTaskId(),
                        nextTaskInfoVersion.getAndIncrement(),
                        state,
                        location,
                        DateTime.now(),
                        sharedBuffer.getInfo(),
                        ImmutableSet.<PlanNodeId>of(),
                        taskContext.getTaskStats(),
                        failures,
                        taskContext.getOutputItems());
            }

            @Override
            public void start()
            {
            }

            @Override
            public void addSplit(PlanNodeId sourceId, Split split)
            {
                checkNotNull(split, "split is null");
                splits.put(sourceId, split);
            }

            @Override
            public void noMoreSplits(PlanNodeId sourceId)
            {
                noMoreSplits.add(sourceId);
                if (noMoreSplits.containsAll(fragment.getSources())) {
                    taskStateMachine.finished();
                }
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
            public int getQueuedSplits()
            {
                if (taskStateMachine.getState().isDone()) {
                    return 0;
                }
                return splits.size();
            }
        }
    }
}
