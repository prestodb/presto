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
import com.facebook.presto.execution.SharedBuffer.QueueState;
import com.facebook.presto.execution.TestSqlTaskManager.MockLocationFactory;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.StageExecutionPlan;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestSqlStageExecution
{
    public static final ConnectorSession SESSION = new ConnectorSession("user", "source", "catalog", "schema", UTC_KEY, Locale.ENGLISH, "address", "agent");
    private NodeTaskMap nodeTaskMap;
    private InMemoryNodeManager nodeManager;
    private NodeScheduler nodeScheduler;
    private LocationFactory locationFactory;

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
    }

    @Test(expectedExceptions = ExecutionException.class, expectedExceptionsMessageRegExp = ".*No nodes available to run query")
    public void testExcludeCoordinator()
            throws Exception
    {
        InMemoryNodeManager nodeManager = new InMemoryNodeManager();
        NodeScheduler nodeScheduler = new NodeScheduler(nodeManager, new NodeSchedulerConfig().setIncludeCoordinator(false), nodeTaskMap);

        // Start sql stage execution
        SqlStageExecution sqlStageExecution = createSqlStageExecution(nodeScheduler, 2, 20);
        Future future = sqlStageExecution.start();
        future.get(1, TimeUnit.SECONDS);
    }

    @Test
    public void testSplitAssignment()
            throws Exception
    {
        // Start sql stage execution (schedule 15 splits in batches of 2), there are 3 nodes, each node should get 5 splits
        SqlStageExecution sqlStageExecution1 = createSqlStageExecution(nodeScheduler, 2, 15);
        Future future1 = sqlStageExecution1.start();
        future1.get(1, TimeUnit.SECONDS);
        Map<Node, RemoteTask> tasks1 = sqlStageExecution1.getTasks();
        for (Map.Entry<Node, RemoteTask> entry : tasks1.entrySet()) {
            assertEquals(entry.getValue().getPartitionedSplitCount(), 5);
        }

        // Add new node
        Node additionalNode = new PrestoNode("other4", URI.create("http://127.0.0.1:14"), NodeVersion.UNKNOWN);
        nodeManager.addNode("foo", additionalNode);

        // Schedule next query with 5 splits. Since the new node does not have any splits, all 5 splits are assigned to the new node
        SqlStageExecution sqlStageExecution2 = createSqlStageExecution(nodeScheduler, 5, 5);
        Future future2 = sqlStageExecution2.start();
        future2.get(1, TimeUnit.SECONDS);
        Map<Node, RemoteTask> tasks2 = sqlStageExecution2.getTasks();

        RemoteTask task = tasks2.get(additionalNode);
        assertNotNull(task);
        assertEquals(task.getPartitionedSplitCount(), 5);
    }

    @Test
    public void testSplitAssignmentBatchSizeGreaterThanMaxPending()
            throws Exception
    {
        // Start sql stage execution with 100 splits. Only 20 will be scheduled on each node as that is the maxSplitsPerNode
        SqlStageExecution sqlStageExecution1 = createSqlStageExecution(nodeScheduler, 100, 100);
        Future future1 = sqlStageExecution1.start();

        // The stage scheduler will block and this will cause a timeout exception
        try {
            future1.get(1, TimeUnit.SECONDS);
        }
        catch (TimeoutException e) {
        }

        Map<Node, RemoteTask> tasks1 = sqlStageExecution1.getTasks();
        for (Map.Entry<Node, RemoteTask> entry : tasks1.entrySet()) {
            assertEquals(entry.getValue().getPartitionedSplitCount(), 20);
        }
    }

    private SqlStageExecution createSqlStageExecution(NodeScheduler nodeScheduler, int splitBatchSize, int splitCount)
    {
        ExecutorService remoteTaskExecutor = newCachedThreadPool(daemonThreadsNamed("remoteTaskExecutor"));
        MockRemoteTaskFactory remoteTaskFactory = new MockRemoteTaskFactory(remoteTaskExecutor);
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stageExecutor"));

        OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS
                .withBuffer("out", new UnpartitionedPagePartitionFunction())
                .withNoMoreBufferIds();

        StageExecutionPlan tableScanPlan = createTableScanPlan("test", splitCount);
        return new SqlStageExecution(new QueryId("query"),
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
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        SqlStageExecution stageExecution = null;
        try {
            StageExecutionPlan joinPlan = createJoinPlan("A");

            InMemoryNodeManager nodeManager = new InMemoryNodeManager();
            nodeManager.addNode("foo", new PrestoNode("other", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN));

            OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS
                    .withBuffer("out", new UnpartitionedPagePartitionFunction())
                    .withNoMoreBufferIds();

            stageExecution = new SqlStageExecution(new QueryId("query"),
                    new MockLocationFactory(),
                    joinPlan,
                    new NodeScheduler(nodeManager, new NodeSchedulerConfig(), nodeTaskMap),
                    new MockRemoteTaskFactory(executor),
                    SESSION,
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

    private StageExecutionPlan createJoinPlan(String planId)
    {
        // create table scan for build data with a single split, so it is only waiting on the no-more buffers call
        StageExecutionPlan build = createTableScanPlan("build", 1);

        // create an exchange to read the build data
        ExchangeNode buildExchange = new ExchangeNode(new PlanNodeId(planId + "-build"),
                build.getFragment().getId(),
                ImmutableList.copyOf(build.getFragment().getSymbols().keySet()));

        // create table scan for probe data with three splits, so it will not send the no-more buffers call
        StageExecutionPlan probe = createTableScanPlan("probe", 10);

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

    private StageExecutionPlan createTableScanPlan(String planId, int splitCount)
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
                        Optional.<GeneratedPartitions>absent()),
                ImmutableMap.<Symbol, Type>of(symbol, VARCHAR),
                PlanDistribution.SOURCE,
                tableScanNodeId,
                OutputPartitioning.NONE,
                ImmutableList.<Symbol>of());

        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();

        for (int i = 0; i < splitCount; i++) {
            splits.add(new TestingSplit());
        }
        SplitSource splitSource = new ConnectorAwareSplitSource("test", new FixedSplitSource(null, splits.build()));

        return new StageExecutionPlan(testFragment,
                Optional.of(splitSource),
                ImmutableList.<StageExecutionPlan>of()
        );
    }
}
