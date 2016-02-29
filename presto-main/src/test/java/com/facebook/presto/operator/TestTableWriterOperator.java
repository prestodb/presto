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
package com.facebook.presto.operator;

import com.facebook.presto.metadata.OutputTableHandle;
import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestTableWriterOperator
{
    private static final String CONNECTOR_ID = "testConnectorId";
    private ExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testBlockedPageSink()
            throws Exception
    {
        BlockingPageSink blockingPageSink = new BlockingPageSink();
        Operator operator = createTableWriterOperator(blockingPageSink);

        // initial state validation
        assertEquals(operator.isBlocked().isDone(), true);
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), true);

        // blockingPageSink that will return blocked future
        operator.addInput(rowPagesBuilder(BIGINT).row(42).build().get(0));

        assertEquals(operator.isBlocked().isDone(), false);
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);

        // complete previously blocked future
        blockingPageSink.complete();

        assertEquals(operator.isBlocked().isDone(), true);
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), true);

        // add second page
        operator.addInput(rowPagesBuilder(BIGINT).row(44).build().get(0));

        assertEquals(operator.isBlocked().isDone(), false);
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // finish operator, state hasn't changed
        operator.finish();

        assertEquals(operator.isBlocked().isDone(), false);
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // complete previously blocked future
        blockingPageSink.complete();
        // and getOutput which actually finishes the operator
        assertPageEquals(
                TableWriterOperator.TYPES,
                operator.getOutput(),
                rowPagesBuilder(TableWriterOperator.TYPES).row(2, null).build().get(0));

        assertEquals(operator.isBlocked().isDone(), true);
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void addInputFailsOnBlockedOperator()
            throws Exception
    {
        Operator operator = createTableWriterOperator(new BlockingPageSink());

        operator.addInput(rowPagesBuilder(BIGINT).row(42).build().get(0));

        assertEquals(operator.isBlocked().isDone(), false);
        assertEquals(operator.needsInput(), false);

        operator.addInput(rowPagesBuilder(BIGINT).row(42).build().get(0));
    }

    private Operator createTableWriterOperator(BlockingPageSink blockingPageSink)
    {
        PageSinkManager pageSinkProvider = new PageSinkManager();
        pageSinkProvider.addConnectorPageSinkProvider(CONNECTOR_ID, new ConstantPageSinkProvider(blockingPageSink));

        TableWriterOperator.TableWriterOperatorFactory factory = new TableWriterOperator.TableWriterOperatorFactory(
                0,
                new PlanNodeId("test"),
                pageSinkProvider,
                new TableWriterNode.CreateHandle(new OutputTableHandle(
                        CONNECTOR_ID,
                        new ConnectorTransactionHandle() {},
                        new ConnectorOutputTableHandle() {})),
                ImmutableList.of(0),
                Optional.empty(),
                TEST_SESSION);

        return factory.createOperator(createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext());
    }

    private static class ConstantPageSinkProvider
            implements ConnectorPageSinkProvider
    {
        private final ConnectorPageSink pageSink;

        private ConstantPageSinkProvider(ConnectorPageSink pageSink)
        {
            this.pageSink = pageSink;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
        {
            return pageSink;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
        {
            return pageSink;
        }
    }

    private class BlockingPageSink
            implements ConnectorPageSink
    {
        private CompletableFuture<?> future = new CompletableFuture<>();

        @Override
        public CompletableFuture<?> appendPage(Page page, Block sampleWeightBlock)
        {
            future = new CompletableFuture<>();
            return future;
        }

        @Override
        public Collection<Slice> finish()
        {
            return ImmutableList.of();
        }

        @Override
        public void abort()
        {
        }

        public void complete()
        {
            future.complete(null);
        }
    }
}
