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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.OutputBuffers;
import io.prestosql.ScheduledSplit;
import io.prestosql.TaskSource;
import io.prestosql.block.BlockEncodingManager;
import io.prestosql.connector.ConnectorId;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.event.SplitMonitor;
import io.prestosql.eventlistener.EventListenerManager;
import io.prestosql.execution.TestSqlTaskManager.MockExchangeClientSupplier;
import io.prestosql.execution.scheduler.LegacyNetworkTopology;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.NodeSchedulerConfig;
import io.prestosql.index.IndexManager;
import io.prestosql.metadata.InMemoryNodeManager;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.metadata.Split;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.LookupJoinOperators;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.StageExecutionStrategy;
import io.prestosql.operator.index.IndexJoinLookupStats;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spiller.GenericSpillerFactory;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSourceManager;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.gen.ExpressionCompiler;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.gen.PageFunctionCompiler;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.testing.TestingMetadata.TestingColumnHandle;
import io.prestosql.testing.TestingMetadata.TestingTableHandle;
import io.prestosql.testing.TestingSplit;
import io.prestosql.testing.TestingTransactionHandle;
import io.prestosql.util.FinalizerService;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;

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
                    ImmutableMap.of(SYMBOL, new TestingColumnHandle("column", 0, BIGINT))),
            ImmutableMap.of(SYMBOL, VARCHAR),
            SOURCE_DISTRIBUTION,
            ImmutableList.of(TABLE_SCAN_NODE_ID),
            new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of(SYMBOL))
                    .withBucketToPartition(Optional.of(new int[1])),
            StageExecutionStrategy.ungroupedExecution(),
            StatsAndCosts.empty());

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

    public static SplitMonitor createTestSplitMonitor()
    {
        return new SplitMonitor(
                new EventListenerManager(),
                new ObjectMapperProvider().get());
    }
}
