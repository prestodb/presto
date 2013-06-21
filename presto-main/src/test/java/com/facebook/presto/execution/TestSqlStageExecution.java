package com.facebook.presto.execution;

import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplit;
import com.facebook.presto.execution.SharedBuffer.QueueState;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.execution.TestSqlTaskManager.MockLocationFactory;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.util.Threads;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
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
            metadata.addInternalSchemaMetadata(new DualMetadata());

            StageExecutionPlan joinPlan = createJoinPlan("A", metadata);

            InMemoryNodeManager nodeManager = new InMemoryNodeManager();
            nodeManager.addNode("foo", new Node("other", URI.create("http://127.0.0.1:11")));

            stageExecution = new SqlStageExecution(new QueryId("query"),
                    new MockLocationFactory(),
                    joinPlan,
                    new NodeScheduler(nodeManager), new MockRemoteTaskFactory(executor),
                    SESSION,
                    1,
                    executor);

            stageExecution.addOutputBuffer("out");
            stageExecution.noMoreOutputBuffers();
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
                stageExecution.cancel();
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
        PlanFragment joinPlan = new PlanFragment(new PlanFragmentId(planId),
                new PlanNodeId(planId),
                probe.getFragment().getSymbols(), // this is wrong, but it works
                new JoinNode(new PlanNodeId(planId), JoinNode.Type.INNER, probe.getFragment().getRoot(), exchangeNode, ImmutableList.<EquiJoinClause>of()));

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
        Split split = new DualSplit(ImmutableList.of(HostAddress.fromString("127.0.0.1")));
        PlanNodeId tableScanNodeId = new PlanNodeId(planId);
        PlanFragment testFragment = new PlanFragment(new PlanFragmentId(planId),
                tableScanNodeId,
                ImmutableMap.<Symbol, Type>of(symbol, Type.STRING),
                new TableScanNode(tableScanNodeId,
                        tableHandle, ImmutableList.of(symbol),
                        ImmutableMap.of(symbol, columnHandle),
                        BooleanLiteral.TRUE_LITERAL,
                        BooleanLiteral.TRUE_LITERAL));
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

        public RemoteTask createRemoteTask(Session session,
                TaskId taskId,
                Node node,
                PlanFragment fragment,
                Split initialSplit,
                Map<PlanNodeId, OutputReceiver> outputReceivers,
                Multimap<PlanNodeId, URI> initialExchangeLocations,
                Set<String> initialOutputIds)
        {

            return new MockRemoteTask(taskId, fragment, executor);
        }

        private static class MockRemoteTask
                implements RemoteTask
        {
            private final TaskOutput taskOutput;
            private final PlanFragment fragment;
            private int splits;

            public MockRemoteTask(TaskId taskId, PlanFragment fragment, Executor executor)
            {
                this.fragment = fragment;
                URI location = URI.create("fake://task/" + taskId);
                taskOutput = new TaskOutput(taskId, location, new DataSize(1, Unit.BYTE), executor, new SqlTaskManagerStats());
            }

            @Override
            public TaskInfo getTaskInfo()
            {
                return taskOutput.getTaskInfo(false);
            }

            @Override
            public void start()
            {
            }

            @Override
            public void addSplit(Split split)
            {
                checkNotNull(split, "split is null");
                splits++;
            }

            @Override
            public void noMoreSplits()
            {
                taskOutput.noMoreSplits(fragment.getPartitionedSource());
                if (taskOutput.getNoMoreSplits().containsAll(fragment.getSources())) {
                    taskOutput.finish();
                }
            }

            @Override
            public void addExchangeLocations(Multimap<PlanNodeId, URI> exchangeLocations, boolean noMore)
            {
                if (noMore) {
                    for (PlanNodeId planNodeId : exchangeLocations.keys()) {
                        taskOutput.noMoreSplits(planNodeId);
                    }
                    if (taskOutput.getNoMoreSplits().containsAll(fragment.getSources())) {
                        taskOutput.finish();
                    }
                }
            }

            @Override
            public void addOutputBuffers(Set<String> outputBuffers, boolean noMore)
            {
                for (String bufferId : outputBuffers) {
                    taskOutput.addResultQueue(bufferId);
                }
                if (noMore) {
                    taskOutput.noMoreResultQueues();
                }
            }

            @Override
            public void addStateChangeListener(final StateChangeListener<TaskInfo> stateChangeListener)
            {
                taskOutput.addStateChangeListener(new StateChangeListener<TaskState>()
                {
                    @Override
                    public void stateChanged(TaskState newValue)
                    {
                        stateChangeListener.stateChanged(taskOutput.getTaskInfo(false));
                    }
                });
            }

            @Override
            public void cancel()
            {
                taskOutput.cancel();
            }

            @Override
            public int getQueuedSplits()
            {
                if (taskOutput.getState().isDone()) {
                    return 0;
                }
                return splits;
            }
        }
    }
}
