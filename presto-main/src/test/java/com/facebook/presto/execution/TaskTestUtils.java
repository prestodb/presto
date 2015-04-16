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
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.index.IndexJoinLookupStats;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TestingColumnHandle;
import com.facebook.presto.sql.planner.TestingTableHandle;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public final class TaskTestUtils
{
    private TaskTestUtils()
    {
    }

    public static final ScheduledSplit SPLIT = new ScheduledSplit(0, new Split("test", TestingSplit.createLocalSplit()));

    public static final PlanNodeId TABLE_SCAN_NODE_ID = new PlanNodeId("tableScan");

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
            ImmutableList.of(SYMBOL),
            PlanDistribution.SOURCE,
            TABLE_SCAN_NODE_ID,
            OutputPartitioning.NONE,
            ImmutableList.<Symbol>of(),
            Optional.empty());

    public static LocalExecutionPlanner createTestingPlanner()
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();

        PageSourceManager pageSourceManager = new PageSourceManager();
        pageSourceManager.addConnectorPageSourceProvider("test", new TestingPageSourceProvider());
        return new LocalExecutionPlanner(
                metadata,
                new SqlParser(),
                pageSourceManager,
                new IndexManager(),
                new PageSinkManager(),
                new MockExchangeClientSupplier(),
                new ExpressionCompiler(metadata),
                new IndexJoinLookupStats(),
                new CompilerConfig(),
                new TaskManagerConfig());
    }

    public static TaskInfo updateTask(SqlTask sqlTask, List<TaskSource> taskSources, OutputBuffers outputBuffers)
    {
        return sqlTask.updateTask(TEST_SESSION, PLAN_FRAGMENT, taskSources, outputBuffers);
    }
}
