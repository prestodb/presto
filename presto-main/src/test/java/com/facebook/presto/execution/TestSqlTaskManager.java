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

import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.RecordSinkManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
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
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.event.client.NullEventClient;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestSqlTaskManager
{
    private static final ScheduledSplit SPLIT = new ScheduledSplit(0, new Split("test", new TestingSplit()));

    private SqlTaskManager sqlTaskManager;
    private PlanFragment testFragment;
    private TaskExecutor taskExecutor;
    private LocalExecutionPlanner planner;
    private TaskId taskId;
    private ConnectorSession session;
    private PlanNodeId tableScanNodeId;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        Symbol symbol = new Symbol("column");

        MetadataManager metadata = new MetadataManager(new FeaturesConfig(), new TypeRegistry());

        DataStreamManager dataStreamProvider = new DataStreamManager();
        dataStreamProvider.addConnectorDataStreamProvider("test", new TestingDataStreamProvider());
        planner = new LocalExecutionPlanner(
                new NodeInfo("test"),
                metadata,
                new SqlParser(),
                dataStreamProvider,
                new IndexManager(),
                new RecordSinkManager(),
                new MockExchangeClientSupplier(),
                new ExpressionCompiler(metadata),
                new CompilerConfig());

        taskExecutor = new TaskExecutor(8);
        taskExecutor.start();

        sqlTaskManager = new SqlTaskManager(
                planner,
                new MockLocationFactory(),
                taskExecutor,
                new QueryMonitor(new ObjectMapperProvider().get(), new NullEventClient(), new NodeInfo("test")),
                new TaskManagerConfig());

        tableScanNodeId = new PlanNodeId("tableScan");
        testFragment = new PlanFragment(
                new PlanFragmentId("fragment"),
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

        taskId = new TaskId("query", "stage", "task");
        session = new ConnectorSession("user", "test", "default", "default", UTC_KEY, Locale.ENGLISH, "test", "test");
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        sqlTaskManager.stop();
        taskExecutor.stop();
    }

    @Test
    public void testEmptyQuery()
            throws Exception
    {
        TaskInfo taskInfo = sqlTaskManager.updateTask(session,
                taskId,
                testFragment,
                ImmutableList.<TaskSource>of(),
                INITIAL_EMPTY_OUTPUT_BUFFERS);
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTaskManager.updateTask(session,
                taskId,
                testFragment,
                ImmutableList.of(new TaskSource(tableScanNodeId, ImmutableSet.<ScheduledSplit>of(), true)),
                INITIAL_EMPTY_OUTPUT_BUFFERS.withNoMoreBufferIds());
        assertEquals(taskInfo.getState(), TaskState.FINISHED);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
    }

    @Test
    public void testSimpleQuery()
            throws Exception
    {
        TaskInfo taskInfo = sqlTaskManager.updateTask(session,
                taskId,
                testFragment,
                ImmutableList.of(new TaskSource(tableScanNodeId, ImmutableSet.of(SPLIT), true)),
                INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer("out", new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds());
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        BufferResult results = sqlTaskManager.getTaskResults(taskId, "out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(1, TimeUnit.SECONDS));
        assertEquals(results.isBufferClosed(), false);
        assertEquals(results.getPages().size(), 1);
        assertEquals(results.getPages().get(0).getPositionCount(), 1);

        results = sqlTaskManager.getTaskResults(taskId, "out", results.getToken() + results.getPages().size(), new DataSize(1, Unit.MEGABYTE), new Duration(1, TimeUnit.SECONDS));
        // todo this should be true
        assertEquals(results.isBufferClosed(), false);
        assertEquals(results.getPages().size(), 0);

        sqlTaskManager.waitForStateChange(taskInfo.getTaskId(), taskInfo.getState(), new Duration(1, TimeUnit.SECONDS));
        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
    }

    @Test
    public void testCancel()
            throws Exception
    {
        TaskInfo taskInfo = sqlTaskManager.updateTask(session,
                taskId,
                testFragment,
                ImmutableList.<TaskSource>of(),
                INITIAL_EMPTY_OUTPUT_BUFFERS);
        assertEquals(taskInfo.getState(), TaskState.RUNNING);
        assertNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.RUNNING);
        assertNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTaskManager.cancelTask(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.CANCELED);
        assertNotNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.CANCELED);
        assertNotNull(taskInfo.getStats().getEndTime());
    }

    @Test
    public void testAbort()
            throws Exception
    {
        TaskInfo taskInfo = sqlTaskManager.updateTask(session,
                taskId,
                testFragment,
                ImmutableList.of(new TaskSource(tableScanNodeId, ImmutableSet.of(SPLIT), true)),
                INITIAL_EMPTY_OUTPUT_BUFFERS.withBuffer("out", new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds());
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        sqlTaskManager.abortTaskResults(taskInfo.getTaskId(), "out");

        sqlTaskManager.waitForStateChange(taskInfo.getTaskId(), taskInfo.getState(), new Duration(1, TimeUnit.SECONDS));
        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.FINISHED);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
    }

    @Test
    public void testRemoveOldTasks()
            throws Exception
    {
        sqlTaskManager = new SqlTaskManager(
                planner,
                new MockLocationFactory(),
                taskExecutor,
                new QueryMonitor(new ObjectMapperProvider().get(), new NullEventClient(), new NodeInfo("test")),
                new TaskManagerConfig().setInfoMaxAge(new Duration(5, TimeUnit.MILLISECONDS)));

        TaskInfo taskInfo = sqlTaskManager.updateTask(session,
                taskId,
                testFragment,
                ImmutableList.<TaskSource>of(),
                INITIAL_EMPTY_OUTPUT_BUFFERS);
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTaskManager.cancelTask(taskId);
        assertEquals(taskInfo.getState(), TaskState.CANCELED);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.CANCELED);

        Thread.sleep(100);
        sqlTaskManager.removeOldTasks();
        try {
            sqlTaskManager.getTaskInfo(taskInfo.getTaskId());
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
    }

    public static class MockExchangeClientSupplier
            implements Supplier<ExchangeClient>
    {
        @Override
        public ExchangeClient get()
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class MockLocationFactory
            implements LocationFactory
    {
        @Override
        public URI createQueryLocation(QueryId queryId)
        {
            return URI.create("fake://query/" + queryId);
        }

        @Override
        public URI createStageLocation(StageId stageId)
        {
            return URI.create("fake://stage/" + stageId);
        }

        @Override
        public URI createLocalTaskLocation(TaskId taskId)
        {
            return URI.create("fake://task/" + taskId);
        }

        @Override
        public URI createTaskLocation(Node node, TaskId taskId)
        {
            return URI.create("fake://task/" + node.getNodeIdentifier() + "/" + taskId);
        }
    }
}
