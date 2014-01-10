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
import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplit;
import com.facebook.presto.execution.TestSqlTaskManager.MockLocationFactory;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.execution.SharedBuffer.QueueState;
import static com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestSqlStageExecution
{
    public static final Session SESSION = new Session("user", "source", "catalog", "schema", "address", "agent");
    private final MetadataManager metadata = new MetadataManager();
    private final LocationFactory locationFactory = new MockLocationFactory();
    private final NodeTaskMap nodeTaskMap = new NodeTaskMap();
    private final NodeSchedulerConfig nodeSchedulerConfig = new NodeSchedulerConfig().setMaxSplitsPerNode(20);
    private final String datasourceName = "test_datasource";

    ImmutableList<Node> nodes = new ImmutableList.Builder<Node>()
            .add(new PrestoNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN))
            .add(new PrestoNode("other2", URI.create("http://127.0.0.1:12"), NodeVersion.UNKNOWN))
            .add(new PrestoNode("other3", URI.create("http://127.0.0.1:13"), NodeVersion.UNKNOWN))
            .add(new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN))
            .build();

    private InMemoryNodeManager nodeManager;
    private NodeScheduler nodeScheduler;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        nodeManager = new InMemoryNodeManager();
        nodeManager.addNode(datasourceName, nodes);
        nodeScheduler = new NodeScheduler(nodeManager, nodeSchedulerConfig, nodeTaskMap);
        metadata.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, new DualMetadata());
    }

    @Test
    public void testSplitAssignment()
            throws Exception
    {
        SqlStageExecution sqlStageExecution;
        Future future;
        Map<Node, RemoteTask> tasks;

        // Start sql stage execution
        sqlStageExecution = createSqlStageExecution(nodeScheduler, 20, 20);
        future = sqlStageExecution.start();
        future.get(1, TimeUnit.SECONDS);
        tasks = sqlStageExecution.getTasks();
        assertEquals(tasks.size(), 4);
        for (Map.Entry<Node, RemoteTask> entry : tasks.entrySet()) {
            assertEquals(entry.getValue().getPartitionedSplitCount(), 5);
        }

        for (RemoteTask task : tasks.values()) {
            task.cancel();
        }

        // Add new node
        Node additionalNode = new PrestoNode("other5", URI.create("http://127.0.0.1:15"), NodeVersion.UNKNOWN);
        nodeManager.addNode(datasourceName, additionalNode);

        // Schedule next query
        sqlStageExecution = createSqlStageExecution(nodeScheduler, 20, 20);
        future = sqlStageExecution.start();
        future.get(1, TimeUnit.SECONDS);
        tasks = sqlStageExecution.getTasks();

        assertNotNull(tasks.get(additionalNode));
        assertEquals(tasks.size(), 5);

        for (Map.Entry<Node, RemoteTask> entry : tasks.entrySet()) {
            assertEquals(entry.getValue().getPartitionedSplitCount(), 4);
        }

        for (RemoteTask task : tasks.values()) {
            task.cancel();
        }

        // Test that the global split count is reduced after the task is completed.
        assertEquals(nodeTaskMap.getPartitionedSplitsOnNode(additionalNode), 0);
    }

    @Test
    public void testSplitAssignmentBatchSizeGreaterThanMaxPending()
            throws Exception
    {
        // Start sql stage execution
        SqlStageExecution sqlStageExecution = createSqlStageExecution(nodeScheduler, 150, 200);
        sqlStageExecution.start();

        Map<Node, RemoteTask> tasks = sqlStageExecution.getTasks();
        for (Map.Entry<Node, RemoteTask> entry : tasks.entrySet()) {
            assertEquals(entry.getValue().getPartitionedSplitCount(), 20);
        }
    }

    @Test
    public void testGlobalSplitAssignment()
            throws Exception
    {
        SqlStageExecution sqlStageExecution;
        Future future;

        // Start sql stage execution
        sqlStageExecution = createSqlStageExecution(nodeScheduler, 20, 20);
        future = sqlStageExecution.start();
        future.get(1, TimeUnit.SECONDS);
        Map<Node, RemoteTask> tasks1 = sqlStageExecution.getTasks();
        assertEquals(tasks1.size(), 4);
        for (Map.Entry<Node, RemoteTask> entry : tasks1.entrySet()) {
            assertEquals(entry.getValue().getPartitionedSplitCount(), 5);
        }

        // Add two new node
        Node additionalNode = new PrestoNode("other5", URI.create("http://127.0.0.1:15"), NodeVersion.UNKNOWN);
        nodeManager.addNode(datasourceName, additionalNode);
        nodeManager.addNode(datasourceName, new PrestoNode("other6", URI.create("http://127.0.0.1:16"), NodeVersion.UNKNOWN));

        // Schedule next query
        sqlStageExecution = createSqlStageExecution(nodeScheduler, 20, 10);
        future = sqlStageExecution.start();
        future.get(1, TimeUnit.SECONDS);

        // The previous tasks are not cancelled. So the previous 4 nodes still have 5 splits each.
        // The two newly added nodes have no splits. So expect these two nodes to be selected for running the new tasks
        // with 5 splits each.
        Map<Node, RemoteTask> tasks2 = sqlStageExecution.getTasks();
        assertEquals(tasks2.size(), 2);

        RemoteTask task = tasks2.get(additionalNode);
        assertNotNull(task);

        // All nodes now have 5 splits from the two SqlStageExecutions
        for (Map.Entry<Node, RemoteTask> entry : tasks2.entrySet()) {
            assertEquals(entry.getValue().getPartitionedSplitCount(), 5);
        }

        for (RemoteTask remoteTask : tasks1.values()) {
            remoteTask.cancel();
        }
        for (RemoteTask remoteTask : tasks2.values()) {
            remoteTask.cancel();
        }
    }

    private SqlStageExecution createSqlStageExecution(NodeScheduler nodeScheduler, int splitBatchSize, int splitCount)
    {
        ExecutorService remoteTaskExecutor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("remoteTaskExecutor"));
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        ExecutorService executor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("stageExecutor"));

        OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS
                .withBuffer("out", new UnpartitionedPagePartitionFunction())
                .withNoMoreBufferIds();

        StageExecutionPlan tableScanPlan = createTableScanPlan("test", datasourceName, metadata, splitCount);
        return new SqlStageExecution(
                new QueryId("query"),
                locationFactory,
                tableScanPlan,
                nodeScheduler,
                remoteTaskFactory,
                SESSION,
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
        ExecutorService executor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("test"));
        SqlStageExecution stageExecution = null;
        try {
            MetadataManager metadata = new MetadataManager();
            metadata.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, new DualMetadata());

            StageExecutionPlan joinPlan = createJoinPlan("A", metadata);

            InMemoryNodeManager nodeManager = new InMemoryNodeManager();
            nodeManager.addNode("foo", new PrestoNode("other", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN));

            OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS
                    .withBuffer("out", new UnpartitionedPagePartitionFunction())
                    .withNoMoreBufferIds();

            stageExecution = new SqlStageExecution(new QueryId("query"),
                    new MockLocationFactory(),
                    joinPlan,
                    new NodeScheduler(nodeManager, nodeSchedulerConfig, nodeTaskMap),
                    new MockRemoteTaskFactory(executor),
                    SESSION,
                    1000,  // splitBatchSize
                    8,     // initialHashPartitions
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
        StageExecutionPlan build = createTableScanPlan("build", datasourceName, metadata, 1);

        // create an exchange to read the build data
        ExchangeNode buildExchange = new ExchangeNode(new PlanNodeId(planId + "-build"),
                build.getFragment().getId(),
                ImmutableList.copyOf(build.getFragment().getSymbols().keySet()));

        // create table scan for probe data with three splits, so it will not send the no-more buffers call
        StageExecutionPlan probe = createTableScanPlan("probe", datasourceName, metadata, 3);

        // create an exchange to read the probe data
        ExchangeNode probeExchange = new ExchangeNode(new PlanNodeId(planId + "-probe"),
                probe.getFragment().getId(),
                ImmutableList.copyOf(probe.getFragment().getSymbols().keySet()));

        // join build and probe
        PlanFragment joinPlan = new PlanFragment(
                new PlanFragmentId(planId),
                new JoinNode(new PlanNodeId(planId), JoinNode.Type.INNER, probeExchange, buildExchange, ImmutableList.<EquiJoinClause>of()),
                probe.getFragment().getSymbols(), // this is wrong, but it works
                PlanDistribution.SOURCE,
                new PlanNodeId(planId),
                OutputPartitioning.NONE,
                ImmutableList.<Symbol>of());

        return new StageExecutionPlan(joinPlan,
                probe.getDataSource(),
                ImmutableList.of(probe, build)
        );
    }

    static StageExecutionPlan createTableScanPlan(String planId, String datasource, MetadataManager metadata, int initialSplitCount)
    {
        TableHandle tableHandle = metadata.getTableHandle(new QualifiedTableName("default", "default", DualMetadata.NAME)).get();
        ColumnHandle columnHandle = metadata.getColumnHandle(tableHandle, DualMetadata.COLUMN_NAME).get();
        Symbol symbol = new Symbol(DualMetadata.COLUMN_NAME);

        // table scan with splitCount splits
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
                ImmutableMap.of(symbol, Type.VARCHAR),
                PlanDistribution.SOURCE,
                tableScanNodeId,
                OutputPartitioning.NONE,
                ImmutableList.<Symbol>of());

        ImmutableList.Builder<Split> splits = ImmutableList.builder();
        for (int i = 0; i < initialSplitCount; i++) {
            splits.add(new DualSplit(HostAddress.fromString("127.0.0.1")));
        }
        SplitSource splitSource = new FixedSplitSource(datasource, splits.build());

        return new StageExecutionPlan(testFragment,
                Optional.of(splitSource),
                ImmutableList.<StageExecutionPlan>of()
        );
    }
}
