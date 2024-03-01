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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.MockRemoteTaskFactory;
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.SqlStageExecution;
import com.facebook.presto.execution.StageExecutionId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.failureDetector.NoOpFailureDetector;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.server.security.SecurityConfig;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tracing.TracingConfig;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.SqlStageExecution.createSqlStageExecution;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertTrue;

public class TestAdaptivePhasedExecutionPolicy
{
    private static final ConnectorTransactionHandle TRANSACTION_HANDLE = TestingTransactionHandle.create();
    private static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("tableScan");
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("test");

    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("testAdaptivePhasedExecutionPolicy-%s"));

    @AfterClass
    public void tearDownExecutor()
    {
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testCreateExecutionSchedule()
    {
        Session session = testSessionBuilder(new SessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig().setMaxStageCountForEagerScheduling(5),
                new NodeMemoryConfig(),
                new WarningCollectorConfig(),
                new NodeSchedulerConfig(),
                new NodeSpillConfig(),
                new TracingConfig(),
                new CompilerConfig(),
                new SecurityConfig()))).build();
        AdaptivePhasedExecutionPolicy policy = new AdaptivePhasedExecutionPolicy();
        Collection<StageExecutionAndScheduler> schedulers = getStageExecutionAndSchedulers(4);
        assertTrue(policy.createExecutionSchedule(session, schedulers) instanceof AllAtOnceExecutionSchedule);
        schedulers = getStageExecutionAndSchedulers(5);
        assertTrue(policy.createExecutionSchedule(session, schedulers) instanceof AllAtOnceExecutionSchedule);
        schedulers = getStageExecutionAndSchedulers(6);
        assertTrue(policy.createExecutionSchedule(session, schedulers) instanceof PhasedExecutionSchedule);
    }

    private Collection<StageExecutionAndScheduler> getStageExecutionAndSchedulers(int count)
    {
        PlanNode node = getTableScanNode();

        ImmutableList<StageExecutionAndScheduler> exchanges = IntStream.rangeClosed(1, count - 1)
                .mapToObj(stage -> getStageExecutionAndScheduler(stage, getRemoteSourcePlanNode(new PlanFragmentId(stage))))
                .collect(toImmutableList());
        return ImmutableList.<StageExecutionAndScheduler>builder()
            .add(getStageExecutionAndScheduler(0, node))
            .addAll(exchanges)
            .build();
    }

    private StageExecutionAndScheduler getStageExecutionAndScheduler(int stage, PlanNode fragementNode)
    {
        PlanFragmentId fragmentId = new PlanFragmentId(stage);
        StageId stageId = new StageId(new QueryId("query"), stage);
        SqlStageExecution stageExecution = createSqlStageExecution(
                new StageExecutionId(stageId, stage),
                createPlanFragment(fragmentId, fragementNode),
                new MockRemoteTaskFactory(directExecutor(), scheduledExecutor),
                TEST_SESSION,
                true,
                new NodeTaskMap(new FinalizerService()),
                newDirectExecutorService(),
                new NoOpFailureDetector(),
                new SplitSchedulerStats(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()));
        StageLinkage stageLinkage = new StageLinkage(fragmentId, (id, tasks, noMoreExchangeLocations) -> {}, ImmutableSet.of());
        StageScheduler stageScheduler = new FixedCountScheduler(stageExecution, ImmutableList.of());
        StageExecutionAndScheduler scheduler = new StageExecutionAndScheduler(stageExecution, stageLinkage, stageScheduler);
        return scheduler;
    }

    private static PlanFragment createPlanFragment(PlanFragmentId fragmentId, PlanNode remoteSourcePlanNode)
    {
        return new PlanFragment(
                fragmentId,
                remoteSourcePlanNode,
                ImmutableSet.copyOf(remoteSourcePlanNode.getOutputVariables()),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(remoteSourcePlanNode.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), remoteSourcePlanNode.getOutputVariables()),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());
    }

    private PlanNode getTableScanNode()
    {
        return new TableScanNode(
                Optional.empty(),
                TABLE_SCAN_NODE_ID,
                new TableHandle(CONNECTOR_ID, new TestingMetadata.TestingTableHandle(), TRANSACTION_HANDLE, Optional.empty()),
                ImmutableList.of(),
                ImmutableMap.of());
    }

    private static PlanNode getRemoteSourcePlanNode(PlanFragmentId fragmentId)
    {
        PlanNode planNode = new RemoteSourceNode(
                Optional.empty(),
                new PlanNodeId("exchange"),
                ImmutableList.of(new PlanFragmentId(fragmentId.getId() - 1)),
                ImmutableList.of(new VariableReferenceExpression(Optional.empty(), "column", VARCHAR)),
                false,
                Optional.empty(),
                REPARTITION);
        return planNode;
    }
}
