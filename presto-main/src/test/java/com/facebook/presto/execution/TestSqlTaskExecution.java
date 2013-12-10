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
import com.facebook.presto.UnpartitionedPagePartitionFunction;
import com.facebook.presto.connector.dual.DualDataStreamProvider;
import com.facebook.presto.connector.dual.DualMetadata;
import com.facebook.presto.connector.dual.DualSplitManager;
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.TestSqlTaskManager.MockExchangeClientSupplier;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MockLocalStorageManager;
import com.facebook.presto.operator.RecordSinkManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PartitionResult;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.Split;
import com.facebook.presto.spi.SplitSource;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.DataStreamManager;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.planner.LocalExecutionPlanner;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.event.client.NullEventClient;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.node.NodeInfo;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URI;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Threads.threadsNamed;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestSqlTaskExecution
{
    private TaskExecutor taskExecutor;
    private Split split;
    private ExecutorService taskNotificationExecutor;
    private SqlTaskExecution taskExecution;
    private OutputBuffers outputBuffers;
    private PlanNodeId tableScanNodeId;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        DualMetadata dualMetadata = new DualMetadata();
        TableHandle tableHandle = dualMetadata.getTableHandle(new SchemaTableName("default", DualMetadata.NAME));
        assertNotNull(tableHandle, "tableHandle is null");

        ColumnHandle columnHandle = dualMetadata.getColumnHandle(tableHandle, DualMetadata.COLUMN_NAME);
        assertNotNull(columnHandle, "columnHandle is null");
        Symbol symbol = new Symbol(DualMetadata.COLUMN_NAME);

        MetadataManager metadata = new MetadataManager();
        metadata.addInternalSchemaMetadata(MetadataManager.INTERNAL_CONNECTOR_ID, dualMetadata);

        DualSplitManager dualSplitManager = new DualSplitManager(new InMemoryNodeManager());
        PartitionResult partitionResult = dualSplitManager.getPartitions(tableHandle, TupleDomain.all());

        SplitSource splitSource = dualSplitManager.getPartitionSplits(tableHandle, partitionResult.getPartitions());
        split = Iterables.getOnlyElement(splitSource.getNextBatch(1));
        assertTrue(splitSource.isFinished());

        LocalExecutionPlanner planner = new LocalExecutionPlanner(
                new NodeInfo("test"),
                metadata,
                new DataStreamManager(new DualDataStreamProvider()),
                new IndexManager(),
                new MockLocalStorageManager(new File("target/temp")),
                new RecordSinkManager(),
                new MockExchangeClientSupplier(),
                new ExpressionCompiler(metadata));

        taskExecutor = new TaskExecutor(8);
        taskExecutor.start();

        tableScanNodeId = new PlanNodeId("tableScan");
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId("fragment"),
                new TableScanNode(
                        tableScanNodeId,
                        tableHandle,
                        ImmutableList.of(symbol),
                        ImmutableMap.of(symbol, columnHandle),
                        null,
                        Optional.<GeneratedPartitions>absent()),
                ImmutableMap.<Symbol, Type>of(symbol, VARCHAR),
                PlanDistribution.SOURCE,
                tableScanNodeId,
                OutputPartitioning.NONE,
                ImmutableList.<Symbol>of());

        TaskId taskId = new TaskId("query", "stage", "task");
        Session session = new Session("user", "test", "default", "default", UTC_KEY, Locale.ENGLISH, "test", "test");

        taskNotificationExecutor = Executors.newCachedThreadPool(threadsNamed("task-notification-%d"));

        outputBuffers = OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;

        taskExecution = SqlTaskExecution.createSqlTaskExecution(
                session,
                taskId,
                URI.create("fake://task/" + taskId),
                testFragment,
                ImmutableList.<TaskSource>of(),
                outputBuffers,
                planner,
                new DataSize(32, Unit.MEGABYTE),
                taskExecutor,
                taskNotificationExecutor,
                new DataSize(256, Unit.MEGABYTE),
                new DataSize(8, Unit.MEGABYTE),
                new QueryMonitor(new ObjectMapperProvider().get(), new NullEventClient(), new NodeInfo("test")),
                false);
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        taskExecutor.stop();
        taskNotificationExecutor.shutdownNow();
    }

    @Test
    public void testBufferCloseOnFinish()
            throws Exception
    {
        outputBuffers = outputBuffers.withBuffer("out", new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds();
        taskExecution.addResultQueue(outputBuffers);

        BufferResult bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(0, TimeUnit.MILLISECONDS));
        assertFalse(bufferResult.isBufferClosed());

        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(0, TimeUnit.MILLISECONDS));
        assertFalse(bufferResult.isBufferClosed());

        taskExecution.addSources(ImmutableList.of(new TaskSource(tableScanNodeId, ImmutableSet.<ScheduledSplit>of(), true)));
        assertEquals(taskExecution.getTaskInfo(false).getState(), TaskState.FINISHED);

        // buffer will be closed by cancel event (wait for 500 MS for event to fire)
        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(500, TimeUnit.MILLISECONDS));
        assertTrue(bufferResult.isBufferClosed());

        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(500, TimeUnit.MILLISECONDS));
        assertTrue(bufferResult.isBufferClosed());
    }

    @Test
    public void testBufferCloseOnCancel()
            throws Exception
    {
        outputBuffers = outputBuffers.withBuffer("out", new UnpartitionedPagePartitionFunction());
        taskExecution.addResultQueue(outputBuffers);

        BufferResult bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(0, TimeUnit.MILLISECONDS));
        assertFalse(bufferResult.isBufferClosed());

        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(0, TimeUnit.MILLISECONDS));
        assertFalse(bufferResult.isBufferClosed());

        taskExecution.cancel();
        assertEquals(taskExecution.getTaskInfo(false).getState(), TaskState.CANCELED);

        // buffer will be closed by cancel event.  event is async so wait for 500 MS for event to fire
        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(500, TimeUnit.MILLISECONDS));
        assertTrue(bufferResult.isBufferClosed());

        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(500, TimeUnit.MILLISECONDS));
        assertTrue(bufferResult.isBufferClosed());
    }

    @Test
    public void testBufferNotCloseOnFail()
            throws Exception
    {
        outputBuffers = outputBuffers.withBuffer("out", new UnpartitionedPagePartitionFunction()).withNoMoreBufferIds();
        taskExecution.addResultQueue(outputBuffers);

        BufferResult bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(0, TimeUnit.MILLISECONDS));
        assertFalse(bufferResult.isBufferClosed());

        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(0, TimeUnit.MILLISECONDS));
        assertFalse(bufferResult.isBufferClosed());

        taskExecution.fail(new Exception("test"));
        assertEquals(taskExecution.getTaskInfo(false).getState(), TaskState.FAILED);

        // buffer will not be closed by fail event.  event is async so wait for 500 MS for event to fire
        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(500, TimeUnit.MILLISECONDS));
        assertFalse(bufferResult.isBufferClosed());

        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(500, TimeUnit.MILLISECONDS));
        assertFalse(bufferResult.isBufferClosed());
    }
}
