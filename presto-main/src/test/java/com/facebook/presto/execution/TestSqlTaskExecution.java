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
import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.TestSqlTaskManager.MockExchangeClientSupplier;
import com.facebook.presto.index.IndexManager;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.RecordSinkManager;
import com.facebook.presto.spi.ConnectorSession;
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
import com.facebook.presto.sql.planner.plan.TableScanNode.GeneratedPartitions;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Optional;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.concurrent.Threads.threadsNamed;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestSqlTaskExecution
{
    private TaskExecutor taskExecutor;
    private ExecutorService taskNotificationExecutor;
    private SqlTaskExecution taskExecution;
    private OutputBuffers outputBuffers;
    private PlanNodeId tableScanNodeId;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        Symbol symbol = new Symbol("column");

        MetadataManager metadata = new MetadataManager(new FeaturesConfig(), new TypeRegistry());

        LocalExecutionPlanner planner = new LocalExecutionPlanner(
                new NodeInfo("test"),
                metadata,
                new SqlParser(),
                new DataStreamManager(),
                new IndexManager(),
                new RecordSinkManager(),
                new MockExchangeClientSupplier(),
                new ExpressionCompiler(metadata),
                new CompilerConfig());

        taskExecutor = new TaskExecutor(8);
        taskExecutor.start();

        tableScanNodeId = new PlanNodeId("tableScan");
        PlanFragment testFragment = new PlanFragment(
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

        TaskId taskId = new TaskId("query", "stage", "task");
        ConnectorSession session = new ConnectorSession("user", "test", "default", "default", UTC_KEY, Locale.ENGLISH, "test", "test");

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
        assertEquals(taskExecution.getTaskInfo().getState(), TaskState.FINISHED);

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
        assertEquals(taskExecution.getTaskInfo().getState(), TaskState.CANCELED);

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
        assertEquals(taskExecution.getTaskInfo().getState(), TaskState.FAILED);

        // buffer will not be closed by fail event.  event is async so wait for 500 MS for event to fire
        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(500, TimeUnit.MILLISECONDS));
        assertFalse(bufferResult.isBufferClosed());

        bufferResult = taskExecution.getResults("out", 0, new DataSize(1, Unit.MEGABYTE), new Duration(500, TimeUnit.MILLISECONDS));
        assertFalse(bufferResult.isBufferClosed());
    }
}
