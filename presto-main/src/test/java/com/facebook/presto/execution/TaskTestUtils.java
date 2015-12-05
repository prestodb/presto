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
import com.facebook.presto.execution.TestSqlTaskManager.MockExchangeClientSupplier;
import com.facebook.presto.execution.scheduler.LegacyNetworkTopology;
import com.facebook.presto.execution.scheduler.NodeScheduler;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.PartitionFunctionBinding;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.util.FinalizerService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;

public final class TaskTestUtils
{
    private TaskTestUtils()
    {
    }

    public static final ConnectorTransactionHandle TRANSACTION_HANDLE = TestingTransactionHandle.create("test");

    public static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("tableScan");

    public static final ScheduledSplit SPLIT = new ScheduledSplit(0, TABLE_SCAN_NODE_ID, new Split("test", TRANSACTION_HANDLE, TestingSplit.createLocalSplit()));

    public static final ImmutableList<TaskSource> EMPTY_SOURCES = ImmutableList.of();

    public static final Symbol SYMBOL = new Symbol("column");

    public static final PlanFragment PLAN_FRAGMENT = new PlanFragment(
            new PlanFragmentId("fragment"),
            new TableScanNode(
                    TABLE_SCAN_NODE_ID,
                    new TableHandle("test", new TestingTableHandle()),
                    ImmutableList.of(SYMBOL),
                    ImmutableMap.of(SYMBOL, new TestingColumnHandle("column")),
                    Optional.empty(),
                    TupleDomain.all(),
                    null),
            ImmutableMap.<Symbol, Type>of(SYMBOL, VARCHAR),
            SOURCE_DISTRIBUTION,
            ImmutableList.of(TABLE_SCAN_NODE_ID),
            new PartitionFunctionBinding(SINGLE_DISTRIBUTION, ImmutableList.of(SYMBOL), ImmutableList.of())
                    .withBucketToPartition(Optional.of(new int[1])));

    public static LocalExecutionPlanner createTestingPlanner()
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();

        PageSourceManager pageSourceManager = new PageSourceManager();
        pageSourceManager.addConnectorPageSourceProvider("test", new TestingPageSourceProvider());

        // we don't start the finalizer so nothing will be collected, which is ok for a test
        FinalizerService finalizerService = new FinalizerService();

        NodeScheduler nodeScheduler = new NodeScheduler(
                new LegacyNetworkTopology(),
                new InMemoryNodeManager(),
                new NodeSchedulerConfig().setIncludeCoordinator(true),
                new NodeTaskMap(finalizerService));
        NodePartitioningManager nodePartitioningManager = new NodePartitioningManager(nodeScheduler);

        return new LocalExecutionPlanner(
                metadata,
                new SqlParser(),
                Optional.empty(),
                pageSourceManager,
                new IndexManager(),
                nodePartitioningManager,
                new PageSinkManager(),
                new MockExchangeClientSupplier(),
                new ExpressionCompiler(metadata),
                new IndexJoinLookupStats(),
                new CompilerConfig(),
                new TaskManagerConfig());
    }

    public static TaskInfo updateTask(SqlTask sqlTask, List<TaskSource> taskSources, OutputBuffers outputBuffers)
    {
        return sqlTask.updateTask(TEST_SESSION, Optional.of(PLAN_FRAGMENT), taskSources, outputBuffers);
    }
}
