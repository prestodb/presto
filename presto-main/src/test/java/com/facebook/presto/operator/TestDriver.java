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

import com.facebook.presto.ScheduledSplit;
import com.facebook.presto.Session;
import com.facebook.presto.TaskSource;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.PageConsumerOperator;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.getRootCause;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestDriver
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testNormalFinish()
    {
        List<Type> types = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        ValuesOperator source = new ValuesOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "values"), types, rowPagesBuilder(types)
                .addSequencePage(10, 20, 30, 40)
                .build());

        Operator sink = createSinkOperator(source);
        Driver driver = new Driver(driverContext, source, sink);

        assertSame(driver.getDriverContext(), driverContext);

        assertFalse(driver.isFinished());
        ListenableFuture<?> blocked = driver.processFor(new Duration(1, TimeUnit.SECONDS));
        assertTrue(blocked.isDone());
        assertTrue(driver.isFinished());

        assertTrue(sink.isFinished());
        assertTrue(source.isFinished());
    }

    @Test
    public void testAbruptFinish()
    {
        List<Type> types = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        ValuesOperator source = new ValuesOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "values"), types, rowPagesBuilder(types)
                .addSequencePage(10, 20, 30, 40)
                .build());

        PageConsumerOperator sink = createSinkOperator(source);
        Driver driver = new Driver(driverContext, source, sink);

        assertSame(driver.getDriverContext(), driverContext);

        assertFalse(driver.isFinished());
        driver.close();
        assertTrue(driver.isFinished());

        // finish is only called in normal operations
        assertFalse(source.isFinished());
        assertFalse(sink.isFinished());

        // close is always called (values operator doesn't have a closed state)
        assertTrue(sink.isClosed());
    }

    @Test
    public void testAddSourceFinish()
    {
        PlanNodeId sourceId = new PlanNodeId("source");
        final List<Type> types = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        TableScanOperator source = new TableScanOperator(driverContext.addOperatorContext(99, new PlanNodeId("test"), "values"),
                sourceId,
                new PageSourceProvider()
                {
                    @Override
                    public ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns)
                    {
                        return new FixedPageSource(rowPagesBuilder(types)
                                .addSequencePage(10, 20, 30, 40)
                                .build());
                    }
                },
                types,
                ImmutableList.<ColumnHandle>of());

        PageConsumerOperator sink = createSinkOperator(source);
        Driver driver = new Driver(driverContext, source, sink);

        assertSame(driver.getDriverContext(), driverContext);

        assertFalse(driver.isFinished());
        assertFalse(driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        assertFalse(driver.isFinished());

        driver.updateSource(new TaskSource(sourceId, ImmutableSet.of(new ScheduledSplit(0, sourceId, newMockSplit())), true));

        assertFalse(driver.isFinished());
        assertTrue(driver.processFor(new Duration(1, TimeUnit.SECONDS)).isDone());
        assertTrue(driver.isFinished());

        assertTrue(sink.isFinished());
        assertTrue(source.isFinished());
    }

    @Test
    public void testBrokenOperatorCloseWhileProcessing()
            throws Exception
    {
        BrokenOperator brokenOperator = new BrokenOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "source"), false);
        final Driver driver = new Driver(driverContext, brokenOperator, createSinkOperator(brokenOperator));

        assertSame(driver.getDriverContext(), driverContext);

        // block thread in operator processing
        Future<Boolean> driverProcessFor = executor.submit(new Callable<Boolean>()
        {
            @Override
            public Boolean call()
                    throws Exception
            {
                return driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone();
            }
        });
        brokenOperator.waitForLocked();

        driver.close();
        assertTrue(driver.isFinished());

        try {
            driverProcessFor.get(1, TimeUnit.SECONDS);
            fail("Expected InterruptedException");
        }
        catch (ExecutionException e) {
            checkArgument(getRootCause(e) instanceof InterruptedException, "Expected root cause exception to be an instance of InterruptedException");
        }
    }

    @Test
    public void testBrokenOperatorProcessWhileClosing()
            throws Exception
    {
        BrokenOperator brokenOperator = new BrokenOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "source"), true);
        final Driver driver = new Driver(driverContext, brokenOperator, createSinkOperator(brokenOperator));

        assertSame(driver.getDriverContext(), driverContext);

        // block thread in operator close
        Future<Boolean> driverClose = executor.submit(new Callable<Boolean>()
        {
            @Override
            public Boolean call()
                    throws Exception
            {
                driver.close();
                return true;
            }
        });
        brokenOperator.waitForLocked();

        assertTrue(driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        assertTrue(driver.isFinished());

        brokenOperator.unlock();

        assertTrue(driverClose.get());
    }

    @Test
    public void testBrokenOperatorAddSource()
            throws Exception
    {
        PlanNodeId sourceId = new PlanNodeId("source");
        final List<Type> types = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        // create a table scan operator that does not block, which will cause the driver loop to busy wait
        TableScanOperator source = new NotBlockedTableScanOperator(driverContext.addOperatorContext(99, new PlanNodeId("test"), "values"),
                sourceId,
                new PageSourceProvider()
                {
                    @Override
                    public ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns)
                    {
                        return new FixedPageSource(rowPagesBuilder(types)
                                .addSequencePage(10, 20, 30, 40)
                                .build());
                    }
                },
                types,
                ImmutableList.<ColumnHandle>of());

        BrokenOperator brokenOperator = new BrokenOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "source"));
        final Driver driver = new Driver(driverContext, source, brokenOperator);

        // block thread in operator processing
        Future<Boolean> driverProcessFor = executor.submit(new Callable<Boolean>()
        {
            @Override
            public Boolean call()
                    throws Exception
            {
                return driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone();
            }
        });
        brokenOperator.waitForLocked();

        assertSame(driver.getDriverContext(), driverContext);

        assertFalse(driver.isFinished());
        // processFor always returns NOT_BLOCKED, because DriveLockResult was not acquired
        assertTrue(driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        assertFalse(driver.isFinished());

        driver.updateSource(new TaskSource(sourceId, ImmutableSet.of(new ScheduledSplit(0, sourceId, newMockSplit())), true));

        assertFalse(driver.isFinished());
        // processFor always returns NOT_BLOCKED, because DriveLockResult was not acquired
        assertTrue(driver.processFor(new Duration(1, TimeUnit.SECONDS)).isDone());
        assertFalse(driver.isFinished());

        driver.close();
        assertTrue(driver.isFinished());

        try {
            driverProcessFor.get(1, TimeUnit.SECONDS);
            fail("Expected InterruptedException");
        }
        catch (ExecutionException e) {
            checkArgument(getRootCause(e) instanceof InterruptedException, "Expected root cause exception to be an instance of InterruptedException");
        }
    }

    private static Split newMockSplit()
    {
        return new Split(new ConnectorId("test"), TestingTransactionHandle.create(), new MockSplit());
    }

    private PageConsumerOperator createSinkOperator(Operator source)
    {
        // materialize the output to catch some type errors
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(driverContext.getSession(), source.getTypes());
        return new PageConsumerOperator(driverContext.addOperatorContext(1, new PlanNodeId("test"), "sink"), resultBuilder::page, Function.identity());
    }

    private static class BrokenOperator
            implements Operator, Closeable
    {
        private final OperatorContext operatorContext;
        private final ReentrantLock lock = new ReentrantLock();
        private final CountDownLatch lockedLatch = new CountDownLatch(1);
        private final CountDownLatch unlockLatch = new CountDownLatch(1);
        private final boolean lockForClose;

        private BrokenOperator(OperatorContext operatorContext)
        {
            this(operatorContext, false);
        }

        private BrokenOperator(OperatorContext operatorContext, boolean lockForClose)
        {
            this.operatorContext = operatorContext;
            this.lockForClose = lockForClose;
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        public void unlock()
        {
            unlockLatch.countDown();
        }

        private void waitForLocked()
        {
            try {
                assertTrue(lockedLatch.await(10, TimeUnit.SECONDS));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
        }

        private void waitForUnlock()
        {
            try {
                assertTrue(lock.tryLock(1, TimeUnit.SECONDS));
                try {
                    lockedLatch.countDown();
                    assertTrue(unlockLatch.await(5, TimeUnit.SECONDS));
                }
                finally {
                    lock.unlock();
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public void finish()
        {
            waitForUnlock();
        }

        @Override
        public boolean isFinished()
        {
            waitForUnlock();
            return true;
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            waitForUnlock();
            return NOT_BLOCKED;
        }

        @Override
        public boolean needsInput()
        {
            waitForUnlock();
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            waitForUnlock();
        }

        @Override
        public Page getOutput()
        {
            waitForUnlock();
            return null;
        }

        @Override
        public void close()
                throws IOException
        {
            if (lockForClose) {
                waitForUnlock();
            }
        }
    }

    private static class NotBlockedTableScanOperator
            extends TableScanOperator
    {
        public NotBlockedTableScanOperator(
                OperatorContext operatorContext,
                PlanNodeId planNodeId,
                PageSourceProvider pageSourceProvider,
                List<Type> types,
                Iterable<ColumnHandle> columns)
        {
            super(operatorContext, planNodeId, pageSourceProvider, types, columns);
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            return NOT_BLOCKED;
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
