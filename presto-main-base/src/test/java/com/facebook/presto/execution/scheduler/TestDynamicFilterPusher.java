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

import com.facebook.airlift.units.Duration;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.MockRemoteTaskFactory;
import com.facebook.presto.execution.MockRemoteTaskFactory.MockRemoteTask;
import com.facebook.presto.execution.MockRemoteTaskFactory.MockRemoteTask.DynamicFilterPush;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.failureDetector.NoOpFailureDetector;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.StageExecutionDescriptor;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PUSH_TO_WORKER_COUNT;
import static com.facebook.presto.common.RuntimeMetricName.DYNAMIC_FILTER_PUSH_TO_WORKER_TASK_COUNT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.SqlStageExecution.createSqlStageExecution;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.spi.plan.ExchangeEncoding.COLUMNAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterPusher
{
    private static final PlanNodeId SCAN_NODE_ID = new PlanNodeId("scan_0");
    private static final String FILTER_ID = "df_1";
    private static final Duration DEFAULT_TIMEOUT = new Duration(2, TimeUnit.SECONDS);
    private static final long DEFAULT_MAX_SIZE_BYTES = 1_048_576L;

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

    @Test
    public void testPushToLateTaskAfterResolution()
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        DynamicFilterPusher pusher = new DynamicFilterPusher(runtimeStats, true);
        SqlStageExecution stage = createStage();

        JoinDynamicFilter joinFilter = createJoinFilter(runtimeStats, 1);
        pusher.startPushing(FILTER_ID, SCAN_NODE_ID, joinFilter, stage);

        // Resolve filter BEFORE any task is scheduled in the stage
        joinFilter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of(FILTER_ID, Domain.singleValue(BIGINT, 42L))));
        assertTrue(joinFilter.isComplete());

        // Late task is scheduled after filter resolution
        InternalNode node = new InternalNode("node1", URI.create("http://10.0.0.1:8080"), NodeVersion.UNKNOWN, false);
        RemoteTask task = stage.scheduleTask(node, 0).orElseThrow(() -> new IllegalStateException("Task not created"));
        MockRemoteTask mockTask = (MockRemoteTask) task;

        // Late task must receive the pushed dynamic filter replay
        List<DynamicFilterPush> pushes = mockTask.getPushedDynamicFilters();
        assertEquals(pushes.size(), 1);
        DynamicFilterPush push = pushes.get(0);
        assertEquals(push.getScanNodeId(), SCAN_NODE_ID);
        assertEquals(push.getFilterId(), FILTER_ID);
        assertTrue(push.getConstraint() instanceof DomainRuntimeFilter);
        assertEquals(((DomainRuntimeFilter) push.getConstraint()).getDomain(),
                TupleDomain.withColumnDomains(ImmutableMap.of(FILTER_ID, Domain.singleValue(BIGINT, 42L))));

        // Metrics check
        assertEquals(runtimeStats.getMetrics().get(DYNAMIC_FILTER_PUSH_TO_WORKER_COUNT).getSum(), 1);
        assertEquals(runtimeStats.getMetrics().get(DYNAMIC_FILTER_PUSH_TO_WORKER_TASK_COUNT).getSum(), 1);
    }

    @Test
    public void testPushToExistingTasksOnResolution()
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        DynamicFilterPusher pusher = new DynamicFilterPusher(runtimeStats, true);
        SqlStageExecution stage = createStage();

        InternalNode node1 = new InternalNode("node1", URI.create("http://10.0.0.1:8080"), NodeVersion.UNKNOWN, false);
        InternalNode node2 = new InternalNode("node2", URI.create("http://10.0.0.2:8080"), NodeVersion.UNKNOWN, false);
        MockRemoteTask task1 = (MockRemoteTask) stage.scheduleTask(node1, 0).orElseThrow(() -> new IllegalStateException("Task not created"));
        MockRemoteTask task2 = (MockRemoteTask) stage.scheduleTask(node2, 1).orElseThrow(() -> new IllegalStateException("Task not created"));

        JoinDynamicFilter joinFilter = createJoinFilter(runtimeStats, 2);
        pusher.startPushing(FILTER_ID, SCAN_NODE_ID, joinFilter, stage);

        // Pre-existing tasks have not received push before resolution
        assertEquals(task1.getPushedDynamicFilters().size(), 0);
        assertEquals(task2.getPushedDynamicFilters().size(), 0);

        // Partial partition received (1 of 2)
        joinFilter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of(FILTER_ID, Domain.singleValue(BIGINT, 10L))));
        assertEquals(task1.getPushedDynamicFilters().size(), 0);
        assertEquals(task2.getPushedDynamicFilters().size(), 0);

        // Second partition completes resolution
        joinFilter.addPartitionByFilterId(TupleDomain.withColumnDomains(
                ImmutableMap.of(FILTER_ID, Domain.singleValue(BIGINT, 20L))));
        assertTrue(joinFilter.isComplete());

        // Both pre-existing tasks received push exactly once
        assertEquals(task1.getPushedDynamicFilters().size(), 1);
        assertEquals(task2.getPushedDynamicFilters().size(), 1);

        // A late task scheduled after resolution should also receive replay exactly once
        InternalNode node3 = new InternalNode("node3", URI.create("http://10.0.0.3:8080"), NodeVersion.UNKNOWN, false);
        MockRemoteTask task3 = (MockRemoteTask) stage.scheduleTask(node3, 2).orElseThrow(() -> new IllegalStateException("Task not created"));
        assertEquals(task3.getPushedDynamicFilters().size(), 1);

        // Total 3 pushes across 3 tasks
        assertEquals(runtimeStats.getMetrics().get(DYNAMIC_FILTER_PUSH_TO_WORKER_COUNT).getSum(), 3);
        assertEquals(runtimeStats.getMetrics().get(DYNAMIC_FILTER_PUSH_TO_WORKER_TASK_COUNT).getSum(), 3);
    }

    @Test
    public void testIsAllFilterIsNotPushedOrReplayed()
    {
        RuntimeStats runtimeStats = new RuntimeStats();
        DynamicFilterPusher pusher = new DynamicFilterPusher(runtimeStats, true);
        SqlStageExecution stage = createStage();

        InternalNode node1 = new InternalNode("node1", URI.create("http://10.0.0.1:8080"), NodeVersion.UNKNOWN, false);
        MockRemoteTask task1 = (MockRemoteTask) stage.scheduleTask(node1, 0).orElseThrow(() -> new IllegalStateException("Task not created"));

        JoinDynamicFilter joinFilter = createJoinFilter(runtimeStats, 1);
        pusher.startPushing(FILTER_ID, SCAN_NODE_ID, joinFilter, stage);

        // Resolve with DomainRuntimeFilter containing TupleDomain.all() (isAll() == true)
        joinFilter.addPartitionByFilterId(new DomainRuntimeFilter(TupleDomain.all()));
        assertTrue(joinFilter.isComplete());

        // Existing task should not be pushed
        assertEquals(task1.getPushedDynamicFilters().size(), 0);

        // Late task should not receive replay either
        InternalNode node2 = new InternalNode("node2", URI.create("http://10.0.0.2:8080"), NodeVersion.UNKNOWN, false);
        MockRemoteTask task2 = (MockRemoteTask) stage.scheduleTask(node2, 1).orElseThrow(() -> new IllegalStateException("Task not created"));
        assertEquals(task2.getPushedDynamicFilters().size(), 0);

        assertNull(runtimeStats.getMetrics().get(DYNAMIC_FILTER_PUSH_TO_WORKER_COUNT));
    }

    private SqlStageExecution createStage()
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
                new TableWriteInfo(Optional.empty(), Optional.empty()));
        stage.setOutputBuffers(createInitialEmptyOutputBuffers(ARBITRARY));
        return stage;
    }

    private static JoinDynamicFilter createJoinFilter(RuntimeStats runtimeStats, int expectedPartitions)
    {
        JoinDynamicFilter filter = new JoinDynamicFilter(
                FILTER_ID,
                "col_a",
                DEFAULT_TIMEOUT,
                DEFAULT_MAX_SIZE_BYTES,
                new DynamicFilterServiceStats(),
                runtimeStats,
                true);
        filter.setExpectedPartitions(expectedPartitions);
        return filter;
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
                REPARTITION,
                COLUMNAR);

        return new PlanFragment(
                new PlanFragmentId(0),
                planNode,
                ImmutableSet.copyOf(planNode.getOutputVariables()),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(planNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), planNode.getOutputVariables()),
                Optional.empty(),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.of(StatsAndCosts.empty()),
                Optional.empty());
    }
}
