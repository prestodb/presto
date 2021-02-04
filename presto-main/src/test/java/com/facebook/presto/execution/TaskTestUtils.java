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

import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.Session;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.event.SplitMonitor;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.execution.scheduler.nodeSelection.NodeSelectionStats;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.ConnectorMetadataUpdaterManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.NoOpFragmentResultCacheManager;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.operator.TableCommitContext;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spiller.GenericSpillerFactory;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.presto.util.FinalizerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;

import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;

public final class TaskTestUtils
{
    private TaskTestUtils()
    {
    }

    private static final ConnectorTransactionHandle TRANSACTION_HANDLE = TestingTransactionHandle.create();

    public static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("tableScan");

    private static final ConnectorId CONNECTOR_ID = new ConnectorId("test");

    public static final ScheduledSplit SPLIT = new ScheduledSplit(0, TABLE_SCAN_NODE_ID, new Split(CONNECTOR_ID, TRANSACTION_HANDLE, TestingSplit.createLocalSplit()));

    public static final ImmutableList<TaskSource> EMPTY_SOURCES = ImmutableList.of();

    public static final VariableReferenceExpression VARIABLE = new VariableReferenceExpression("column", BIGINT);

    public static final PlanFragment PLAN_FRAGMENT = createPlanFragment();

    public static PlanFragment createPlanFragment()
    {
        return new PlanFragment(
                new PlanFragmentId(0),
                new TableScanNode(
                        TABLE_SCAN_NODE_ID,
                        new TableHandle(CONNECTOR_ID, new TestingTableHandle(), TRANSACTION_HANDLE, Optional.empty()),
                        ImmutableList.of(VARIABLE),
                        ImmutableMap.of(VARIABLE, new TestingColumnHandle("column", 0, BIGINT)),
                        TupleDomain.all(),
                        TupleDomain.all()),
                ImmutableSet.of(VARIABLE),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(TABLE_SCAN_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(VARIABLE))
                        .withBucketToPartition(Optional.of(new int[1])),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());
    }

    public static LocalExecutionPlanner createTestingPlanner()
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();

        PageSourceManager pageSourceManager = new PageSourceManager();
        pageSourceManager.addConnectorPageSourceProvider(CONNECTOR_ID, new TestingPageSourceProvider());

        // we don't start the finalizer so nothing will be collected, which is ok for a test
        FinalizerService finalizerService = new FinalizerService();

        NodeScheduler nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                new InMemoryNodeManager(),
                new NodeSelectionStats(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService));
        PartitioningProviderManager partitioningProviderManager = new PartitioningProviderManager();
        NodePartitioningManager nodePartitioningManager = new NodePartitioningManager(nodeScheduler, partitioningProviderManager);

        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(metadata, 0);
        return new LocalExecutionPlanner(
                metadata,
                Optional.empty(),
                pageSourceManager,
                new IndexManager(),
                partitioningProviderManager,
                nodePartitioningManager,
                new PageSinkManager(),
                new ConnectorMetadataUpdaterManager(),
                new ExpressionCompiler(metadata, pageFunctionCompiler),
                pageFunctionCompiler,
                new JoinFilterFunctionCompiler(metadata),
                new IndexJoinLookupStats(),
                new TaskManagerConfig(),
                new GenericSpillerFactory((types, spillContext, memoryContext) -> {
                    throw new UnsupportedOperationException();
                }),
                (types, spillContext, memoryContext) -> {
                    throw new UnsupportedOperationException();
                },
                (types, partitionFunction, spillContext, memoryContext) -> {
                    throw new UnsupportedOperationException();
                },
                new BlockEncodingManager(),
                new PagesIndex.TestingFactory(false),
                new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig()),
                new LookupJoinOperators(),
                new OrderingCompiler(),
                jsonCodec(TableCommitContext.class),
                new RowExpressionDeterminismEvaluator(metadata),
                new NoOpFragmentResultCacheManager(),
                new ObjectMapper());
    }

    public static TaskInfo updateTask(SqlTask sqlTask, List<TaskSource> taskSources, OutputBuffers outputBuffers)
    {
        return sqlTask.updateTask(TEST_SESSION, Optional.of(PLAN_FRAGMENT), taskSources, outputBuffers, Optional.of(new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty())));
    }

    public static SplitMonitor createTestSplitMonitor()
    {
        return new SplitMonitor(
                new EventListenerManager(),
                new ObjectMapperProvider().get());
    }

    public static QueryStateMachine createQueryStateMachine(
            String sqlString,
            Session session,
            boolean transactionControl,
            TransactionManager tm,
            Executor executor,
            MetadataManager metadata)
    {
        return QueryStateMachine.begin(
                sqlString,
                session,
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                Optional.empty(),
                transactionControl,
                tm,
                new AllowAllAccessControl(),
                executor,
                metadata,
                WarningCollector.NOOP);
    }
}
