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
import com.facebook.presto.spi.ConnectorPageSinkProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TableWriterOperatorTest
{
    private static final String CONNECTOR_ID = "testConnectorId";
    private final PageSinkManager pageSinkProvider = new PageSinkManager();
    private final BlockingPageSink blockingPageSink = new BlockingPageSink();
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();

        pageSinkProvider.addConnectorPageSinkProvider(CONNECTOR_ID, new BlockingConnectorPageSinkProvider(blockingPageSink));
    }

    @Test
    public void testBlockedPageSink()
            throws Exception
    {
        TableWriterOperator.TableWriterOperatorFactory factory = new TableWriterOperator.TableWriterOperatorFactory(
                0,
                pageSinkProvider,
                new TableWriterNode.CreateHandle(new OutputTableHandle(CONNECTOR_ID, new ConnectorOutputTableHandle() {})),
                ImmutableList.of(0),
                Optional.empty(),
                TEST_SESSION);

        Operator operator = factory.createOperator(driverContext);

        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), true);

        operator.addInput(rowPagesBuilder(BIGINT).row(1).build().get(0));

        assertEquals(operator.isBlocked().isDone(), false);
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);
    }

    private static class BlockingConnectorPageSinkProvider
            implements ConnectorPageSinkProvider
    {
        private final ConnectorPageSink pageSink;

        private BlockingConnectorPageSinkProvider(ConnectorPageSink pageSink)
        {
            this.pageSink = pageSink;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
        {
            return pageSink;
        }

        @Override
        public ConnectorPageSink createPageSink(ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
        {
            return pageSink;
        }
    }

    private class BlockingPageSink
            implements ConnectorPageSink
    {
        @Override
        public CompletableFuture<?> appendPage(Page page, Block sampleWeightBlock)
        {
            return new CompletableFuture<>();
        }

        @Override
        public Collection<Slice> commit()
        {
            return ImmutableList.of();
        }

        @Override
        public void rollback()
        {
        }
    }
}
