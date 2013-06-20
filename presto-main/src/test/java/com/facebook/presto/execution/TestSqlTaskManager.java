package com.facebook.presto.execution;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.TaskSource;
import com.facebook.presto.connector.dual.DualDataStreamProvider;
import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplitManager;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MockLocalStorageManager;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Provider;
import io.airlift.event.client.NullEventClient;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class TestSqlTaskManager
{
    private SqlTaskManager sqlTaskManager;
    private PlanFragment testFragment;
    private LocalExecutionPlanner planner;
    private TaskId taskId;
    private Session session;
    private Symbol symbol;
    private ColumnHandle columnHandle;
    private TableHandle tableHandle;
    private PlanNodeId tableScanNodeId;
    private Split split;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        DualMetadata dualMetadata = new DualMetadata();
        tableHandle = dualMetadata.getTableHandle(new SchemaTableName("default", DualMetadata.NAME));
        assertNotNull(tableHandle, "tableHandle is null");;
        columnHandle = dualMetadata.getColumnHandle(tableHandle, DualMetadata.COLUMN_NAME);
        assertNotNull(columnHandle, "columnHandle is null");
        symbol = new Symbol(DualMetadata.COLUMN_NAME);

        MetadataManager metadata = new MetadataManager();
        metadata.addInternalSchemaMetadata(dualMetadata);

        DualSplitManager dualSplitManager = new DualSplitManager(new InMemoryNodeManager());
        split = Iterables.getOnlyElement(dualSplitManager.getPartitionSplits(dualSplitManager.getPartitions(tableHandle, ImmutableMap.<ColumnHandle, Object>of())));


        planner = new LocalExecutionPlanner(
                new NodeInfo("test"),
                metadata,
                new QueryManagerConfig(),
                new DataStreamManager(new DualDataStreamProvider()),
                new MockLocalStorageManager(new File("target/temp")),
                new MockExchangeClientProvider(),
                new ExpressionCompiler(metadata));

        sqlTaskManager = new SqlTaskManager(
                planner,
                new MockLocationFactory(),
                new QueryMonitor(new ObjectMapperProvider().get(), new NullEventClient(), new NodeInfo("test")),
                new QueryManagerConfig());

        tableScanNodeId = new PlanNodeId("tableScan");
        testFragment = new PlanFragment(new PlanFragmentId("fragment"),
                tableScanNodeId,
                ImmutableMap.<Symbol, Type>of(symbol, Type.STRING),
                new TableScanNode(tableScanNodeId, tableHandle, ImmutableList.of(symbol), ImmutableMap.of(symbol, columnHandle), TRUE_LITERAL, TRUE_LITERAL));

        taskId = new TaskId("query", "stage", "task");
        session = new Session("user", "test", "default", "default", "test", "test");
    }

    @Test
    public void testEmptyQuery()
            throws Exception
    {
        TaskInfo taskInfo = sqlTaskManager.updateTask(session,
                taskId,
                testFragment,
                ImmutableList.<TaskSource>of(),
                new OutputBuffers(ImmutableSet.<String>of(), false));
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTaskManager.updateTask(session,
                taskId,
                testFragment,
                ImmutableList.<TaskSource>of(new TaskSource(tableScanNodeId, ImmutableSet.<ScheduledSplit>of(), true)),
                new OutputBuffers(ImmutableSet.<String>of(), true));
        assertEquals(taskInfo.getState(), TaskState.FINISHED);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
    }

    @Test
    public void testSimpleQuery()
            throws Exception
    {
        TaskInfo taskInfo = sqlTaskManager.updateTask(session,
                taskId,
                testFragment,
                ImmutableList.<TaskSource>of(new TaskSource(tableScanNodeId, ImmutableSet.of(new ScheduledSplit(0, split)), true)),
                new OutputBuffers(ImmutableSet.of("out"), true));
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        BufferResult results = sqlTaskManager.getTaskResults(taskId, "out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(1, TimeUnit.SECONDS));
        assertEquals(results.isBufferClosed(), false);
        assertEquals(results.getElements().size(), 1);
        assertEquals(results.getElements().get(0).getPositionCount(), 1);

        results = sqlTaskManager.getTaskResults(taskId, "out", results.getStartingSequenceId() + results.getElements().size(), new DataSize(1, Unit.MEGABYTE), new Duration(1, TimeUnit.SECONDS));
        // todo this should be true
        assertEquals(results.isBufferClosed(), false);
        assertEquals(results.getElements().size(), 0);

        sqlTaskManager.waitForStateChange(taskInfo.getTaskId(), taskInfo.getState(), new Duration(1, TimeUnit.SECONDS));
        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
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
                new OutputBuffers(ImmutableSet.<String>of(), false));
        assertEquals(taskInfo.getState(), TaskState.RUNNING);
        assertNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        assertEquals(taskInfo.getState(), TaskState.RUNNING);
        assertNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTaskManager.cancelTask(taskInfo.getTaskId());
        assertEquals(taskInfo.getState(), TaskState.CANCELED);
        assertNotNull(taskInfo.getStats().getEndTime());

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
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
                ImmutableList.<TaskSource>of(new TaskSource(tableScanNodeId, ImmutableSet.of(new ScheduledSplit(0, split)), true)),
                new OutputBuffers(ImmutableSet.of("out"), true));
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        sqlTaskManager.abortTaskResults(taskInfo.getTaskId(), "out");

        sqlTaskManager.waitForStateChange(taskInfo.getTaskId(), taskInfo.getState(), new Duration(1, TimeUnit.SECONDS));
        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        assertEquals(taskInfo.getState(), TaskState.FINISHED);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        assertEquals(taskInfo.getState(), TaskState.FINISHED);
    }

    @Test
    public void testRemoveOldTasks()
            throws Exception
    {
        sqlTaskManager = new SqlTaskManager(
                planner,
                new MockLocationFactory(),
                new QueryMonitor(new ObjectMapperProvider().get(), new NullEventClient(), new NodeInfo("test")),
                new QueryManagerConfig().setInfoMaxAge(new Duration(5, TimeUnit.MILLISECONDS)));

        TaskInfo taskInfo = sqlTaskManager.updateTask(session,
                taskId,
                testFragment,
                ImmutableList.<TaskSource>of(),
                new OutputBuffers(ImmutableSet.<String>of(), false));
        assertEquals(taskInfo.getState(), TaskState.RUNNING);

        taskInfo = sqlTaskManager.cancelTask(taskId);
        assertEquals(taskInfo.getState(), TaskState.CANCELED);

        taskInfo = sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
        assertEquals(taskInfo.getState(), TaskState.CANCELED);

        Thread.sleep(100);
        sqlTaskManager.removeOldTasks();
        try {
            sqlTaskManager.getTaskInfo(taskInfo.getTaskId(), false);
            fail("Expected NoSuchElementException");
        }
        catch (NoSuchElementException expected) {
        }
    }

    private static class MockExchangeClientProvider
            implements Provider<ExchangeClient>
    {
        @Override
        public ExchangeClient get()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class MockLocationFactory
            implements LocationFactory
    {
        @Override
        public URI createQueryLocation(QueryId queryId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI createStageLocation(StageId stageId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public URI createLocalTaskLocation(TaskId taskId)
        {
            return URI.create("fake://task/" + taskId);
        }

        @Override
        public URI createTaskLocation(Node node, TaskId taskId)
        {
            throw new UnsupportedOperationException();
        }
    }
}
