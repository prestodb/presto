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
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.cost.CoefficientBasedStatsCalculator;
import com.facebook.presto.cost.CostCalculatorUsingExchanges;
import com.facebook.presto.cost.SelectingStatsCalculator;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.event.query.QueryMonitorConfig;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.TestSqlTaskManager.MockExchangeClientSupplier;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.LookupJoinOperators;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.server.ServerMainModule;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.TestingTypeManager;
import com.facebook.presto.spiller.GenericSpillerFactory;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.airlift.json.JsonCodec.jsonCodec;

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

    public static final Symbol SYMBOL = new Symbol("column");

    public static final PlanFragment PLAN_FRAGMENT = new PlanFragment(
            new PlanFragmentId("fragment"),
            new TableScanNode(
                    TABLE_SCAN_NODE_ID,
                    new TableHandle(CONNECTOR_ID, new TestingTableHandle()),
                    ImmutableList.of(SYMBOL),
                    ImmutableMap.of(SYMBOL, new TestingColumnHandle("column", 0, BIGINT)),
                    Optional.empty(),
                    TupleDomain.all(),
                    null),
            ImmutableMap.of(SYMBOL, VARCHAR),
            SOURCE_DISTRIBUTION,
            ImmutableList.of(TABLE_SCAN_NODE_ID),
            new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(SYMBOL))
                    .withBucketToPartition(Optional.of(new int[1])),
            UNGROUPED_EXECUTION);

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
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService));
        NodePartitioningManager nodePartitioningManager = new NodePartitioningManager(nodeScheduler);

        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(metadata, 0);
        return new LocalExecutionPlanner(
                metadata,
                new SqlParser(),
                new SelectingStatsCalculator(
                        new CoefficientBasedStatsCalculator(metadata),
                        ServerMainModule.createNewStatsCalculator(metadata)),
                new CostCalculatorUsingExchanges(() -> 1),
                Optional.empty(),
                pageSourceManager,
                new IndexManager(),
                nodePartitioningManager,
                new PageSinkManager(),
                new MockExchangeClientSupplier(),
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
                new BlockEncodingManager(new TestingTypeManager()),
                new PagesIndex.TestingFactory(false),
                new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig()),
                new LookupJoinOperators(),
                new OrderingCompiler());
    }

    public static TaskInfo updateTask(SqlTask sqlTask, List<TaskSource> taskSources, OutputBuffers outputBuffers)
    {
        return sqlTask.updateTask(TEST_SESSION, Optional.of(PLAN_FRAGMENT), taskSources, outputBuffers, OptionalInt.empty());
    }

    public static QueryMonitor createTestQueryMonitor()
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        return new QueryMonitor(
                new ObjectMapperProvider().get(),
                jsonCodec(StageInfo.class),
                new EventListenerManager(),
                new NodeInfo("test"),
                new NodeVersion("testVersion"),
                new SessionPropertyManager(),
                metadata,
                new QueryMonitorConfig());
    }
}
