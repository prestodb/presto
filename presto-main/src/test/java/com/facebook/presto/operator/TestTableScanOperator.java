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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestTableScanOperator
{
    private static final List<Type> TYPES = ImmutableList.of(VARCHAR);
    private static final Page PAGE = createSequencePage(TYPES, 10, 100);

    private ExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    @AfterClass
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
    }

    private DriverContext getDriverContext()
    {
        return createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @Test
    public void testTableScan()
            throws Exception
    {
        DriverContext driverContext = getDriverContext();

        OperatorContext context = driverContext.addOperatorContext(0, TableScanOperator.class.getSimpleName());

        MockPageSourceProvider pageSourceProvider = new MockPageSourceProvider();

        Operator operator = new TableScanOperator(context, new PlanNodeId("0"), pageSourceProvider, TYPES, ImmutableList.<ColumnHandle>of());

        assertEquals(operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes(), 0);

        if (operator instanceof TableScanOperator) {
            ((TableScanOperator) operator).addSplit(new Split("foo", new MockSplit()));
        }

        assertFalse(operator.isFinished());
        assertFalse(operator.needsInput());
        assertNull(operator.getOutput());
        long systemMemoryAfterFirstPage = operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes();
        assertTrue(systemMemoryAfterFirstPage > 0);

        ConnectorPageSource pageSource = pageSourceProvider.getConnectorPageSource();

        if (pageSource instanceof MockConnectorPageSource) {
            ((MockConnectorPageSource) pageSource).startOutput();
        }
        Page page = operator.getOutput();
        assertNotNull(page);
        assertEquals(page.getPositionCount(), 10);
        long systemMemoryAfterSecondPage = operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes();
        assertTrue(systemMemoryAfterSecondPage >= systemMemoryAfterFirstPage);

        if (pageSource instanceof MockConnectorPageSource) {
            ((MockConnectorPageSource) pageSource).endOutput();
        }
        assertNull(operator.getOutput());
        assertTrue(operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes() >= systemMemoryAfterSecondPage);

        operator.close();

        assertTrue(operator.isFinished());
        assertFalse(operator.needsInput());
        assertEquals(operator.getOperatorContext().getOperatorStats().getSystemMemoryReservation().toBytes(), 0);
    }

    private class MockPageSourceProvider
            implements PageSourceProvider
    {
        private ConnectorPageSource connectorPageSource;

        public ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns)
        {
            connectorPageSource = new MockConnectorPageSource();
            return connectorPageSource;
        }

        public ConnectorPageSource getConnectorPageSource()
        {
            return connectorPageSource;
        }
    }

    private class MockConnectorPageSource
            implements ConnectorPageSource
    {
        private boolean startOutput;

        public void startOutput()
        {
            this.startOutput = true;
        }

        public void endOutput()
        {
            this.startOutput = false;
        }

        @Override
        public long getSystemMemoryUsage()
        {
            return 120;
        }

        @Override
        public Page getNextPage()
        {
            if (startOutput) {
                return PAGE;
            }
            return null;
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public long getTotalBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public void close()
        {
        }
    }

    private static class MockSplit
            implements ConnectorSplit
    {
        @Override
        public boolean isRemotelyAccessible()
        {
            return false;
        }

        @Override
        public List<HostAddress> getAddresses()
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return null;
        }
    }
}
